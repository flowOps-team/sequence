/**
 * Analytics Service
 *
 * Implements transaction analytics and aggregation.
 *
 * Features (preserved from original):
 * 1. Transaction statistics (count, volume, average)
 * 2. Trend analysis by period (daily, weekly, monthly)
 * 3. Cash flow aggregation (inflow/outflow)
 * 4. Multi-account aggregation
 */

const Transaction = require('../models/Transaction');

/**
 * Get transaction statistics for accounts
 *
 * @param {string} publicKey - Tenant public key
 * @param {string[]} accounts - Array of account IDs
 * @param {Object} options - Query options
 * @returns {Object} { totalTransactions, transactionVolume, averageTransactionSize }
 */
async function getStats(publicKey, accounts, options = {}) {
  const { startDate, endDate, currency } = options;

  // Build partition keys for all accounts
  const pks = accounts.map(account => Transaction.createPK(publicKey, account));

  // Build match conditions
  const matchConditions = {
    pk: { $in: pks },
    type: { $in: ['debit', 'credit'] } // Exclude genesis for stats
  };

  if (startDate || endDate) {
    matchConditions.timestamp = {};
    if (startDate) {
      matchConditions.timestamp.$gte = new Date(startDate).getTime().toString();
    }
    if (endDate) {
      matchConditions.timestamp.$lte = new Date(endDate).getTime().toString();
    }
  }

  if (currency) {
    matchConditions.currency = currency.toUpperCase();
  }

  const result = await Transaction.aggregate([
    { $match: matchConditions },
    {
      $group: {
        _id: null,
        totalTransactions: { $sum: 1 },
        transactionVolume: { $sum: '$amount' }
      }
    }
  ]);

  if (result.length === 0) {
    return {
      totalTransactions: 0,
      transactionVolume: 0,
      averageTransactionSize: 0
    };
  }

  const { totalTransactions, transactionVolume } = result[0];
  return {
    totalTransactions,
    transactionVolume,
    averageTransactionSize: totalTransactions > 0
      ? Math.round((transactionVolume / totalTransactions) * 100) / 100
      : 0
  };
}

/**
 * Get transaction trends grouped by period
 *
 * @param {string} publicKey - Tenant public key
 * @param {string[]} accounts - Array of account IDs
 * @param {string} period - 'daily', 'weekly', or 'monthly'
 * @param {Object} options - Query options
 * @returns {Array} Array of { period, volume, count }
 */
async function getTrends(publicKey, accounts, period = 'monthly', options = {}) {
  const { startDate, endDate, currency } = options;
  const pks = accounts.map(account => Transaction.createPK(publicKey, account));

  const matchConditions = {
    pk: { $in: pks },
    type: { $in: ['debit', 'credit'] }
  };

  if (startDate || endDate) {
    matchConditions.timestamp = {};
    if (startDate) {
      matchConditions.timestamp.$gte = new Date(startDate).getTime().toString();
    }
    if (endDate) {
      matchConditions.timestamp.$lte = new Date(endDate).getTime().toString();
    }
  }

  if (currency) {
    matchConditions.currency = currency.toUpperCase();
  }

  // Determine grouping expression based on period
  const groupExpression = getGroupExpression(period);

  const result = await Transaction.aggregate([
    { $match: matchConditions },
    {
      $addFields: {
        parsedDate: { $dateFromString: { dateString: '$date' } }
      }
    },
    {
      $group: {
        _id: groupExpression,
        volume: { $sum: '$amount' },
        count: { $sum: 1 }
      }
    },
    { $sort: { '_id': 1 } },
    {
      $project: {
        _id: 0,
        period: '$_id',
        volume: 1,
        count: 1
      }
    }
  ]);

  return result;
}

/**
 * Get cash flow aggregation (inflow/outflow) by period
 *
 * @param {string} publicKey - Tenant public key
 * @param {string[]} accounts - Array of account IDs
 * @param {string} period - 'daily', 'weekly', or 'monthly'
 * @param {Object} options - Query options
 * @returns {Array} Array of { period, inflow, outflow, net }
 */
async function getAggregation(publicKey, accounts, period = 'monthly', options = {}) {
  const { startDate, endDate, currency } = options;
  const pks = accounts.map(account => Transaction.createPK(publicKey, account));

  const matchConditions = {
    pk: { $in: pks },
    type: { $in: ['debit', 'credit'] }
  };

  if (startDate || endDate) {
    matchConditions.timestamp = {};
    if (startDate) {
      matchConditions.timestamp.$gte = new Date(startDate).getTime().toString();
    }
    if (endDate) {
      matchConditions.timestamp.$lte = new Date(endDate).getTime().toString();
    }
  }

  if (currency) {
    matchConditions.currency = currency.toUpperCase();
  }

  const groupExpression = getGroupExpression(period);

  const result = await Transaction.aggregate([
    { $match: matchConditions },
    {
      $addFields: {
        parsedDate: { $dateFromString: { dateString: '$date' } }
      }
    },
    {
      $group: {
        _id: groupExpression,
        inflow: {
          $sum: {
            $cond: [{ $eq: ['$type', 'credit'] }, '$amount', 0]
          }
        },
        outflow: {
          $sum: {
            $cond: [{ $eq: ['$type', 'debit'] }, '$amount', 0]
          }
        },
        transactionCount: { $sum: 1 }
      }
    },
    { $sort: { '_id': 1 } },
    {
      $project: {
        _id: 0,
        period: '$_id',
        inflow: 1,
        outflow: 1,
        net: { $subtract: ['$inflow', '$outflow'] },
        transactionCount: 1
      }
    }
  ]);

  return result;
}

/**
 * Get totals for an account (debits and credits)
 *
 * @param {string} publicKey - Tenant public key
 * @param {string} account - Account ID
 * @param {Object} options - Query options
 * @returns {Object} { totalDebit, totalCredit, balance }
 */
async function getAccountTotals(publicKey, account, options = {}) {
  const { startDate, endDate, currency } = options;
  const pk = Transaction.createPK(publicKey, account);

  const matchConditions = { pk };

  if (startDate || endDate) {
    matchConditions.timestamp = {};
    if (startDate) {
      matchConditions.timestamp.$gte = new Date(startDate).getTime().toString();
    }
    if (endDate) {
      matchConditions.timestamp.$lte = new Date(endDate).getTime().toString();
    }
  }

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
      totalDebit: 0,
      totalCredit: 0,
      balance: 0
    };
  }

  const { totalDebit, totalCredit } = result[0];
  return {
    totalDebit,
    totalCredit,
    balance: totalCredit - totalDebit
  };
}

/**
 * Get MongoDB group expression for period grouping
 *
 * @param {string} period - 'daily', 'weekly', or 'monthly'
 * @returns {Object} MongoDB group expression
 */
function getGroupExpression(period) {
  switch (period) {
    case 'daily':
      return {
        year: { $year: '$parsedDate' },
        month: { $month: '$parsedDate' },
        day: { $dayOfMonth: '$parsedDate' }
      };
    case 'weekly':
      return {
        year: { $year: '$parsedDate' },
        week: { $isoWeek: '$parsedDate' }
      };
    case 'monthly':
    default:
      return {
        year: { $year: '$parsedDate' },
        month: { $month: '$parsedDate' }
      };
  }
}

module.exports = {
  getStats,
  getTrends,
  getAggregation,
  getAccountTotals
};
