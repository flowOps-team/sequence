/**
 * Balance Service
 *
 * Handles balance queries and calculations.
 *
 * Key Design Principles (preserved from original):
 * 1. Balances are derived from transaction history
 * 2. No separate balance field is updated
 * 3. Multi-currency support per account
 * 4. Balance = most recent transaction's balance field
 */

const Transaction = require('../models/Transaction');

/**
 * List all balances for an account
 *
 * Returns balance for each currency the account has transactions in.
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @returns {Array} Array of { currency, balance } objects
 */
async function listBalances(publicKey, account) {
  const pk = Transaction.createPK(publicKey, account);

  // Use aggregation to get the most recent transaction per currency
  const balances = await Transaction.aggregate([
    // Match transactions for this account
    { $match: { pk } },

    // Sort by timestamp descending to get latest first
    { $sort: { timestamp: -1 } },

    // Group by currency, taking the first (most recent) balance
    {
      $group: {
        _id: '$currency',
        balance: { $first: '$balance' },
        lastUpdated: { $first: '$date' }
      }
    },

    // Project to desired format
    {
      $project: {
        _id: 0,
        currency: '$_id',
        balance: 1,
        lastUpdated: 1
      }
    },

    // Sort by currency for consistent output
    { $sort: { currency: 1 } }
  ]);

  return balances;
}

/**
 * Get balance for a specific currency
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @param {string} currency - Currency code
 * @returns {number} Balance (0 if no transactions)
 */
async function getBalance(publicKey, account, currency) {
  const pk = Transaction.createPK(publicKey, account);

  const lastTx = await Transaction.findOne({
    pk,
    currency: currency.toUpperCase()
  })
    .sort({ timestamp: -1 })
    .select('balance')
    .lean();

  return lastTx ? lastTx.balance : 0;
}

/**
 * Calculate balance from transaction history
 *
 * Alternative method that sums credits and debits
 * instead of relying on the balance field.
 *
 * Balance = sum(credits) - sum(debits)
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @param {string} currency - Currency code (optional)
 * @returns {Object} { balance, totalCredit, totalDebit }
 */
async function calculateBalance(publicKey, account, currency = null) {
  const pk = Transaction.createPK(publicKey, account);
  const matchConditions = { pk };

  if (currency) {
    matchConditions.currency = currency.toUpperCase();
  }

  const result = await Transaction.aggregate([
    { $match: matchConditions },
    {
      $group: {
        _id: null,
        totalDebit: {
          $sum: {
            $cond: [{ $eq: ['$type', 'debit'] }, '$amount', 0]
          }
        },
        totalCredit: {
          $sum: {
            $cond: [{ $in: ['$type', ['credit', 'genesis']] }, '$amount', 0]
          }
        }
      }
    }
  ]);

  if (result.length === 0) {
    return {
      balance: 0,
      totalCredit: 0,
      totalDebit: 0
    };
  }

  const { totalCredit, totalDebit } = result[0];
  return {
    balance: totalCredit - totalDebit,
    totalCredit,
    totalDebit
  };
}

/**
 * Check if account has sufficient balance
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @param {string} currency - Currency code
 * @param {number} amount - Required amount
 * @returns {boolean} True if sufficient balance
 */
async function hasSufficientBalance(publicKey, account, currency, amount) {
  const balance = await getBalance(publicKey, account, currency);
  return balance >= amount;
}

/**
 * Get account summary with all currencies
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @returns {Object} Account summary
 */
async function getAccountSummary(publicKey, account) {
  const pk = Transaction.createPK(publicKey, account);

  const summary = await Transaction.aggregate([
    { $match: { pk } },
    {
      $group: {
        _id: '$currency',
        transactionCount: { $sum: 1 },
        totalDebit: {
          $sum: {
            $cond: [{ $eq: ['$type', 'debit'] }, '$amount', 0]
          }
        },
        totalCredit: {
          $sum: {
            $cond: [{ $in: ['$type', ['credit', 'genesis']] }, '$amount', 0]
          }
        },
        lastTransaction: { $max: '$date' },
        firstTransaction: { $min: '$date' }
      }
    },
    {
      $project: {
        _id: 0,
        currency: '$_id',
        transactionCount: 1,
        totalDebit: 1,
        totalCredit: 1,
        balance: { $subtract: ['$totalCredit', '$totalDebit'] },
        lastTransaction: 1,
        firstTransaction: 1
      }
    },
    { $sort: { currency: 1 } }
  ]);

  return {
    account,
    currencies: summary
  };
}

module.exports = {
  listBalances,
  getBalance,
  calculateBalance,
  hasSufficientBalance,
  getAccountSummary
};
