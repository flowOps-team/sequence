/**
 * MongoDB Initialization Script
 *
 * Creates indexes for the transactions collection
 * Mirrors the DynamoDB GSI design from the original Clojure implementation
 */

db = db.getSiblingDB('sequence_ledger');

// Create the transactions collection
db.createCollection('transactions');

// Create indexes for efficient queries
// Index for account transactions by timestamp (equivalent to LSI1)
db.transactions.createIndex(
  { pk: 1, timestamp: -1 },
  { name: 'pk_timestamp_idx' }
);

// Index for public key queries (equivalent to GSI1)
db.transactions.createIndex(
  { publicKey: 1, account: 1 },
  { name: 'publicKey_account_idx' }
);

// Index for currency-specific balance queries
db.transactions.createIndex(
  { pk: 1, currency: 1, timestamp: -1 },
  { name: 'pk_currency_timestamp_idx' }
);

// Index for aggregation queries across a tenant
db.transactions.createIndex(
  { publicKey: 1, timestamp: -1 },
  { name: 'publicKey_timestamp_idx' }
);

print('MongoDB initialized with indexes for sequence_ledger database');
