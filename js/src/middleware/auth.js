/**
 * Authentication Middleware
 *
 * Implements API key-based authentication:
 * 1. Extract Authorization header (Basic auth format)
 * 2. Decode Base64 to get the API key
 * 3. Compute SHA256 hash of the key
 * 4. Match against configured secret-key-hashes
 * 5. Attach customer/tenant info to request
 */

const config = require('../config');
const { decodeApiKey, sha256 } = require('../utils/crypto');

/**
 * Authentication middleware
 * Validates API key and attaches customer info to request
 */
function authenticate(req, res, next) {
  const authHeader = req.headers.authorization;

  // Check for Authorization header
  if (!authHeader) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Missing Authorization header'
    });
  }

  // Decode the API key
  const apiKey = decodeApiKey(authHeader);
  if (!apiKey) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Invalid Authorization header format'
    });
  }

  // Compute hash and find matching key
  const keyHash = sha256(apiKey);
  const customer = findCustomerByKeyHash(keyHash);

  if (!customer) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Invalid API key'
    });
  }

  // Attach customer info to request
  req.customer = {
    name: customer.name,
    email: customer.email,
    publicKey: customer.publicKey
  };

  next();
}

/**
 * Find customer by secret key hash
 * @param {string} keyHash - SHA256 hash of the API key
 * @returns {Object|null} Customer object or null if not found
 */
function findCustomerByKeyHash(keyHash) {
  return config.keys.find(key => key.secretKeyHash === keyHash) || null;
}

/**
 * Optional authentication middleware
 * Attaches customer info if valid key provided, but doesn't require it
 */
function optionalAuthenticate(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return next();
  }

  const apiKey = decodeApiKey(authHeader);
  if (!apiKey) {
    return next();
  }

  const keyHash = sha256(apiKey);
  const customer = findCustomerByKeyHash(keyHash);

  if (customer) {
    req.customer = {
      name: customer.name,
      email: customer.email,
      publicKey: customer.publicKey
    };
  }

  next();
}

module.exports = {
  authenticate,
  optionalAuthenticate
};
