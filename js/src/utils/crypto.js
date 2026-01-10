/**
 * Crypto Utilities
 *
 * Provides hashing functions for:
 * - Transaction ID generation (MD5)
 * - API key validation (SHA256)
 */

const crypto = require('crypto');

/**
 * Generate MD5 hash from a string
 * Used for generating unique transaction IDs
 * @param {string} data - Data to hash
 * @returns {string} MD5 hash in hexadecimal
 */
function md5(data) {
  return crypto.createHash('md5').update(data).digest('hex');
}

/**
 * Generate SHA256 hash from a string
 * Used for API key validation
 * @param {string} data - Data to hash
 * @returns {string} SHA256 hash in hexadecimal
 */
function sha256(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

/**
 * Serialize a map/object to a deterministic string for hashing
 * Keys are sorted alphabetically to ensure consistent hash generation
 * @param {Object} obj - Object to serialize
 * @returns {string} Deterministic string representation
 */
function serializeForHash(obj) {
  const sortedKeys = Object.keys(obj).sort();
  const pairs = sortedKeys.map(key => {
    const value = obj[key];
    if (value === null || value === undefined) {
      return `${key}:`;
    }
    if (typeof value === 'object') {
      return `${key}:${serializeForHash(value)}`;
    }
    return `${key}:${value}`;
  });
  return pairs.join('|');
}

/**
 * Generate a unique transaction ID from transaction data
 * Uses MD5 hash of serialized transaction data
 * @param {Object} txData - Transaction data
 * @returns {string} Unique transaction ID
 */
function generateTransactionId(txData) {
  const hashInput = serializeForHash({
    from: txData.from,
    to: txData.to,
    amount: txData.amount,
    currency: txData.currency,
    timestamp: txData.timestamp,
    publicKey: txData.publicKey
  });
  return md5(hashInput);
}

/**
 * Decode Base64 encoded API key from Authorization header
 * Expected format: "Basic <base64-encoded-key>:"
 * @param {string} authHeader - Authorization header value
 * @returns {string|null} Decoded API key or null if invalid
 */
function decodeApiKey(authHeader) {
  if (!authHeader || !authHeader.startsWith('Basic ')) {
    return null;
  }

  try {
    const base64Part = authHeader.substring(6); // Remove "Basic "
    const decoded = Buffer.from(base64Part, 'base64').toString('utf8');

    // Format is "key:" (with trailing colon)
    if (decoded.endsWith(':')) {
      return decoded.slice(0, -1);
    }

    // Also accept just the key without colon
    return decoded;
  } catch (error) {
    return null;
  }
}

module.exports = {
  md5,
  sha256,
  serializeForHash,
  generateTransactionId,
  decodeApiKey
};
