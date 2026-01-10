/**
 * Validation Utilities
 *
 * Provides validation functions for:
 * - Transaction data
 * - Query parameters
 * - Account identifiers
 */

/**
 * Validate transaction request body
 * @param {Object} body - Request body
 * @returns {Object} { valid: boolean, errors: string[] }
 */
function validateTransaction(body) {
  const errors = [];

  // Required fields
  if (!body.from || typeof body.from !== 'string') {
    errors.push('from: required string field');
  }

  if (!body.to || typeof body.to !== 'string') {
    errors.push('to: required string field');
  }

  if (body.amount === undefined || body.amount === null) {
    errors.push('amount: required field');
  } else if (typeof body.amount !== 'number' || !Number.isInteger(body.amount)) {
    errors.push('amount: must be an integer');
  } else if (body.amount < 0) {
    errors.push('amount: must be non-negative');
  }

  if (!body.currency || typeof body.currency !== 'string') {
    errors.push('currency: required string field');
  } else if (body.currency.length < 1 || body.currency.length > 10) {
    errors.push('currency: must be 1-10 characters');
  }

  // Optional metadata validation
  if (body.metadata !== undefined) {
    if (!Array.isArray(body.metadata)) {
      errors.push('metadata: must be an array of strings');
    } else {
      const invalidItems = body.metadata.filter(item => typeof item !== 'string');
      if (invalidItems.length > 0) {
        errors.push('metadata: all items must be strings');
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Validate and parse query parameters for listing transactions
 * @param {Object} query - Query parameters
 * @returns {Object} { valid: boolean, errors: string[], parsed: Object }
 */
function validateListQuery(query) {
  const errors = [];
  const parsed = {};

  // Account (required)
  if (!query.account) {
    errors.push('account: required query parameter');
  } else {
    parsed.account = query.account;
  }

  // Limit (optional, default 1000, max 1000)
  if (query.limit !== undefined) {
    const limit = parseInt(query.limit, 10);
    if (isNaN(limit) || limit < 1 || limit > 1000) {
      errors.push('limit: must be between 1 and 1000');
    } else {
      parsed.limit = limit;
    }
  } else {
    parsed.limit = 1000;
  }

  // Date range (optional)
  if (query.start_date) {
    const startDate = parseDate(query.start_date);
    if (!startDate) {
      errors.push('start_date: invalid date format (use YYYY-MM-DD or ISO 8601)');
    } else {
      parsed.startDate = startDate;
    }
  }

  if (query.end_date) {
    const endDate = parseDate(query.end_date);
    if (!endDate) {
      errors.push('end_date: invalid date format (use YYYY-MM-DD or ISO 8601)');
    } else {
      parsed.endDate = endDate;
    }
  }

  // Pagination cursor
  if (query.starting_after) {
    parsed.startingAfter = query.starting_after;
  }

  return {
    valid: errors.length === 0,
    errors,
    parsed
  };
}

/**
 * Validate query parameters for balance endpoint
 * @param {Object} query - Query parameters
 * @returns {Object} { valid: boolean, errors: string[], parsed: Object }
 */
function validateBalanceQuery(query) {
  const errors = [];
  const parsed = {};

  if (!query.account) {
    errors.push('account: required query parameter');
  } else {
    parsed.account = query.account;
  }

  return {
    valid: errors.length === 0,
    errors,
    parsed
  };
}

/**
 * Validate query parameters for stats endpoint
 * @param {Object} query - Query parameters
 * @returns {Object} { valid: boolean, errors: string[], parsed: Object }
 */
function validateStatsQuery(query) {
  const errors = [];
  const parsed = {};

  // Account(s) - can be comma-separated
  if (!query.account) {
    errors.push('account: required query parameter');
  } else {
    parsed.accounts = query.account.split(',').map(a => a.trim()).filter(a => a);
    if (parsed.accounts.length === 0) {
      errors.push('account: at least one account required');
    }
  }

  // Period (optional)
  if (query.period) {
    const validPeriods = ['daily', 'weekly', 'monthly'];
    if (!validPeriods.includes(query.period)) {
      errors.push('period: must be one of daily, weekly, monthly');
    } else {
      parsed.period = query.period;
    }
  }

  // Date range (optional)
  if (query.start_date) {
    const startDate = parseDate(query.start_date);
    if (!startDate) {
      errors.push('start_date: invalid date format');
    } else {
      parsed.startDate = startDate;
    }
  }

  if (query.end_date) {
    const endDate = parseDate(query.end_date);
    if (!endDate) {
      errors.push('end_date: invalid date format');
    } else {
      parsed.endDate = endDate;
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    parsed
  };
}

/**
 * Parse date string to Date object
 * Accepts YYYY-MM-DD or ISO 8601 format
 * @param {string} dateStr - Date string
 * @returns {Date|null} Parsed date or null if invalid
 */
function parseDate(dateStr) {
  if (!dateStr) return null;

  // Try parsing as ISO 8601
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    return date;
  }

  return null;
}

/**
 * Normalize currency code to uppercase
 * @param {string} currency - Currency code
 * @returns {string} Uppercase currency code
 */
function normalizeCurrency(currency) {
  return currency ? currency.toUpperCase().trim() : '';
}

module.exports = {
  validateTransaction,
  validateListQuery,
  validateBalanceQuery,
  validateStatsQuery,
  parseDate,
  normalizeCurrency
};
