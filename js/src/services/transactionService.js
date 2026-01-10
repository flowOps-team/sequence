/**
 * Transaction Service
 *
 * Implements the core double-entry ledger system.
 *
 * Key Design Principles (preserved from original):
 * 1. Each transaction creates TWO records (debit + credit)
 * 2. Transactions are immutable (append-only)
 * 3. Balances are derived from transaction history
 * 4. Genesis transactions seed new accounts
 * 5. Multi-currency support per account
 */

const mongoose = require('mongoose');
const Transaction = require('../models/Transaction');
const { generateTransactionId, normalizeCurrency } = require('../utils');

/**
 * Create a new transaction
 *
 * Algorithm (from original Clojure implementation):
 * 1. Check if sender needs genesis (is public key)
 * 2. Get sender's current balance
 * 3. Validate sufficient funds
 * 4. Create debit record for sender
 * 5. Create credit record for receiver
 * 6. Execute atomic write
 *
 * @param {Object} txData - Transaction data
 * @param {string} txData.from - Sender account
 * @param {string} txData.to - Receiver account
 * @param {number} txData.amount - Amount to transfer
 * @param {string} txData.currency - Currency code
 * @param {string[]} txData.metadata - Optional metadata
 * @param {string} publicKey - Tenant public key
 * @returns {Object} Created transaction details
 */
async function createTransaction(txData, publicKey) {
  const { from, to, amount, metadata = [] } = txData;
  const currency = normalizeCurrency(txData.currency);
  const now = new Date();
  const timestamp = now.getTime().toString();
  const isoDate = now.toISOString();

  // Start a MongoDB session for atomic transaction
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    // Step 1: Check for genesis transaction (sender is public key)
    const isGenesis = from === publicKey;

    // Step 2: Get sender's current balance
    let senderBalance = await getAccountBalance(publicKey, from, currency);

    // Step 3: Handle genesis - seed the account if sender is public key
    if (isGenesis) {
      // Genesis creates initial balance
      // Original logic: genesis amount = amount * 2 (to allow transfer)
      const genesisAmount = amount * 2;
      const genesisTx = await createGenesisRecord(
        publicKey,
        from,
        genesisAmount,
        currency,
        timestamp,
        isoDate,
        session
      );
      senderBalance = genesisAmount;
    }

    // Step 4: Check sufficient funds
    if (senderBalance < amount) {
      throw Object.assign(
        new Error('Insufficient funds'),
        { statusCode: 400 }
      );
    }

    // Step 5: Calculate new balances
    const newSenderBalance = senderBalance - amount;
    const receiverBalance = await getAccountBalance(publicKey, to, currency);
    const newReceiverBalance = receiverBalance + amount;

    // Step 6: Generate transaction ID
    const txId = generateTransactionId({
      from,
      to,
      amount,
      currency,
      timestamp,
      publicKey
    });

    // Step 7: Create debit record (sender's perspective)
    const debitRecord = {
      _id: `${txId}-debit`,
      pk: Transaction.createPK(publicKey, from),
      from,
      to,
      amount,
      currency,
      balance: newSenderBalance,
      type: 'debit',
      publicKey,
      account: from,
      party: 'from',
      date: isoDate,
      timestamp,
      metadata
    };

    // Step 8: Create credit record (receiver's perspective)
    const creditRecord = {
      _id: `${txId}-credit`,
      pk: Transaction.createPK(publicKey, to),
      from,
      to,
      amount,
      currency,
      balance: newReceiverBalance,
      type: 'credit',
      publicKey,
      account: to,
      party: 'to',
      date: isoDate,
      timestamp,
      metadata
    };

    // Step 9: Atomic write both records
    await Transaction.create([debitRecord, creditRecord], { session });

    // Commit the transaction
    await session.commitTransaction();
    session.endSession();

    // Return the transaction summary
    return {
      id: txId,
      from,
      to,
      amount,
      balance: newSenderBalance,
      currency,
      date: isoDate,
      metadata
    };
  } catch (error) {
    // Abort on error
    await session.abortTransaction();
    session.endSession();
    throw error;
  }
}

/**
 * Create a genesis record for account seeding
 *
 * Genesis records initialize an account with a starting balance.
 * They are special transactions where from === to === publicKey.
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account to seed
 * @param {number} amount - Initial amount
 * @param {string} currency - Currency code
 * @param {string} timestamp - Epoch timestamp
 * @param {string} isoDate - ISO date string
 * @param {Object} session - MongoDB session
 * @returns {Object} Genesis transaction record
 */
async function createGenesisRecord(publicKey, account, amount, currency, timestamp, isoDate, session) {
  const genesisId = generateTransactionId({
    from: publicKey,
    to: account,
    amount,
    currency,
    timestamp: `genesis-${timestamp}`,
    publicKey
  });

  const genesisRecord = {
    _id: `${genesisId}-genesis`,
    pk: Transaction.createPK(publicKey, account),
    from: publicKey,
    to: account,
    amount,
    currency,
    balance: amount,
    type: 'genesis',
    publicKey,
    account,
    party: 'to',
    date: isoDate,
    timestamp,
    metadata: ['genesis']
  };

  await Transaction.create([genesisRecord], { session });
  return genesisRecord;
}

/**
 * Get the current balance for an account in a specific currency
 *
 * Balance is derived from the most recent transaction record.
 * If no transactions exist, balance is 0.
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @param {string} currency - Currency code
 * @returns {number} Current balance
 */
async function getAccountBalance(publicKey, account, currency) {
  const pk = Transaction.createPK(publicKey, account);

  // Find the most recent transaction for this account and currency
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
 * Get a specific transaction by ID
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} transactionId - Transaction ID
 * @returns {Object|null} Transaction or null if not found
 */
async function getTransaction(publicKey, transactionId) {
  // Try to find the debit record first
  const tx = await Transaction.findOne({
    _id: { $regex: `^${transactionId}` },
    publicKey,
    type: 'debit'
  }).lean();

  if (!tx) {
    return null;
  }

  return {
    id: transactionId,
    from: tx.from,
    to: tx.to,
    amount: tx.amount,
    currency: tx.currency,
    balance: tx.balance,
    date: tx.date,
    metadata: tx.metadata
  };
}

/**
 * List transactions for an account
 *
 * Supports:
 * - Date range filtering
 * - Pagination with cursor
 * - Limit on results
 *
 * @param {string} publicKey - Tenant public key
 * @param {Object} query - Query parameters
 * @returns {Object} { transactions, total_debit, total_credit, balance }
 */
async function listTransactions(publicKey, query) {
  const { account, limit = 1000, startDate, endDate, startingAfter } = query;
  const pk = Transaction.createPK(publicKey, account);

  // Build query conditions
  const conditions = { pk };

  // Date range filter
  if (startDate || endDate) {
    conditions.timestamp = {};
    if (startDate) {
      conditions.timestamp.$gte = new Date(startDate).getTime().toString();
    }
    if (endDate) {
      conditions.timestamp.$lte = new Date(endDate).getTime().toString();
    }
  }

  // Pagination cursor
  if (startingAfter) {
    const cursorTx = await Transaction.findOne({
      _id: { $regex: `^${startingAfter}` },
      pk
    }).select('timestamp').lean();

    if (cursorTx) {
      conditions.timestamp = conditions.timestamp || {};
      conditions.timestamp.$lt = cursorTx.timestamp;
    }
  }

  // Execute query
  const transactions = await Transaction.find(conditions)
    .sort({ timestamp: -1 })
    .limit(limit)
    .lean();

  // Calculate totals
  let totalDebit = 0;
  let totalCredit = 0;

  transactions.forEach(tx => {
    if (tx.type === 'debit') {
      totalDebit += tx.amount;
    } else if (tx.type === 'credit') {
      totalCredit += tx.amount;
    }
  });

  // Get current balance
  const currentBalance = await getAccountBalance(publicKey, account,
    transactions.length > 0 ? transactions[0].currency : 'USD');

  return {
    transactions: transactions.map(tx => ({
      id: tx._id.replace(/-(?:debit|credit|genesis)$/, ''),
      from: tx.from,
      to: tx.to,
      amount: tx.amount,
      currency: tx.currency,
      balance: tx.balance,
      type: tx.type,
      date: tx.date,
      metadata: tx.metadata
    })),
    total_debit: totalDebit,
    total_credit: totalCredit,
    balance: currentBalance
  };
}

module.exports = {
  createTransaction,
  getTransaction,
  listTransactions,
  getAccountBalance,
  createGenesisRecord
};
