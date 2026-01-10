/**
 * Transaction Model
 *
 * Implements immutable double-entry ledger design.
 * Each financial transaction creates TWO records:
 * - Debit record (sender's perspective - balance decreases)
 * - Credit record (receiver's perspective - balance increases)
 *
 * This design ensures:
 * - Complete audit trail
 * - Balance derivation from history
 * - Immutability (no updates allowed)
 */

const mongoose = require('mongoose');

const TransactionSchema = new mongoose.Schema({
  // Unique transaction identifier (MD5 hash of transaction data)
  _id: {
    type: String,
    required: true
  },

  // Composite key for account lookup: {publicKey}#{accountId}
  // This mirrors the DynamoDB partition key design
  pk: {
    type: String,
    required: true,
    index: true
  },

  // Transaction parties
  from: {
    type: String,
    required: true
  },
  to: {
    type: String,
    required: true
  },

  // Financial data
  amount: {
    type: Number,
    required: true,
    min: 0
  },
  currency: {
    type: String,
    required: true,
    uppercase: true
  },

  // Balance after this transaction (for this account/currency)
  balance: {
    type: Number,
    required: true
  },

  // Transaction type: 'debit' (outflow) or 'credit' (inflow) or 'genesis'
  type: {
    type: String,
    required: true,
    enum: ['debit', 'credit', 'genesis']
  },

  // Multi-tenancy: identifies the tenant/customer
  publicKey: {
    type: String,
    required: true,
    index: true
  },

  // The account this record belongs to (from sender's or receiver's perspective)
  account: {
    type: String,
    required: true
  },

  // Which party this record represents: 'from' or 'to'
  party: {
    type: String,
    required: true,
    enum: ['from', 'to']
  },

  // ISO 8601 date string
  date: {
    type: String,
    required: true
  },

  // Milliseconds since epoch (for sorting and range queries)
  timestamp: {
    type: String,
    required: true
  },

  // Optional metadata array
  metadata: {
    type: [String],
    default: []
  }
}, {
  // Disable version key since we're using immutable records
  versionKey: false,

  // Disable _id auto-generation (we use our own hash-based IDs)
  _id: false,

  // Enable timestamps for record-keeping (but not for modification)
  timestamps: {
    createdAt: 'createdAt',
    updatedAt: false
  },

  // Collection name
  collection: 'transactions'
});

// Compound indexes for efficient queries (mirrors DynamoDB GSI design)

// Index for querying transactions by account and date range
TransactionSchema.index({ pk: 1, timestamp: -1 });

// Index for querying by public key (cross-account queries)
TransactionSchema.index({ publicKey: 1, account: 1 });

// Index for currency-specific balance queries
TransactionSchema.index({ pk: 1, currency: 1, timestamp: -1 });

// Index for aggregation queries
TransactionSchema.index({ publicKey: 1, timestamp: -1 });

// Prevent updates to preserve immutability
TransactionSchema.pre('findOneAndUpdate', function() {
  throw new Error('Transactions are immutable and cannot be updated');
});

TransactionSchema.pre('updateOne', function() {
  throw new Error('Transactions are immutable and cannot be updated');
});

TransactionSchema.pre('updateMany', function() {
  throw new Error('Transactions are immutable and cannot be updated');
});

// Static method to create composite key
TransactionSchema.statics.createPK = function(publicKey, accountId) {
  return `${publicKey}#${accountId}`;
};

// Static method to parse composite key
TransactionSchema.statics.parsePK = function(pk) {
  const parts = pk.split('#');
  return {
    publicKey: parts[0],
    accountId: parts.slice(1).join('#')
  };
};

const Transaction = mongoose.model('Transaction', TransactionSchema);

module.exports = Transaction;
