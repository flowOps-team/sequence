/**
 * Configuration module
 * Loads environment variables and provides configuration settings
 */

require('dotenv').config();

const config = {
  // Server configuration
  port: process.env.PORT || 8910,
  env: process.env.NODE_ENV || 'development',

  // MongoDB configuration
  mongodb: {
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/sequence_ledger',
    options: {
      // Connection options
    }
  },

  // API Keys configuration
  // Format: [{ name, email, publicKey, secretKeyHash }]
  keys: parseKeys(process.env.KEYS),

  // Analytics (optional Segment integration)
  analytics: {
    enabled: process.env.ANALYTICS_ENABLED === 'true',
    writeKey: process.env.SEGMENT_WRITE_KEY || ''
  },

  // Pagination defaults
  pagination: {
    defaultLimit: 1000,
    maxLimit: 1000
  }
};

/**
 * Parse KEYS environment variable
 * Expected format: JSON array of key objects
 * @param {string} keysEnv - JSON string of keys
 * @returns {Array} Parsed keys array
 */
function parseKeys(keysEnv) {
  if (!keysEnv) {
    // Default development key (SHA256 hash of "123")
    return [{
      name: 'Development',
      email: 'dev@example.com',
      publicKey: 'dev',
      secretKeyHash: 'a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3'
    }];
  }

  try {
    return JSON.parse(keysEnv);
  } catch (error) {
    console.error('Error parsing KEYS environment variable:', error.message);
    return [];
  }
}

module.exports = config;
