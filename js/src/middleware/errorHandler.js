/**
 * Error Handling Middleware
 *
 * Provides consistent error response format across the API
 */

/**
 * Global error handler middleware
 * Catches all errors and returns consistent JSON response
 */
function errorHandler(err, req, res, next) {
  console.error('Error:', err.message);
  console.error('Stack:', err.stack);

  // Default error response
  const statusCode = err.statusCode || 500;
  const response = {
    error: err.name || 'Error',
    message: err.message || 'Internal server error'
  };

  // Add validation errors if present
  if (err.errors) {
    response.errors = err.errors;
  }

  // Add details for development
  if (process.env.NODE_ENV === 'development' && err.stack) {
    response.stack = err.stack;
  }

  res.status(statusCode).json(response);
}

/**
 * Not found handler
 * Returns 404 for unmatched routes
 */
function notFoundHandler(req, res) {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.method} ${req.path} not found`
  });
}

/**
 * Create a custom error with status code
 * @param {string} message - Error message
 * @param {number} statusCode - HTTP status code
 * @param {Array} errors - Optional array of validation errors
 * @returns {Error} Error with statusCode property
 */
function createError(message, statusCode = 500, errors = null) {
  const error = new Error(message);
  error.statusCode = statusCode;
  if (errors) {
    error.errors = errors;
  }
  return error;
}

module.exports = {
  errorHandler,
  notFoundHandler,
  createError
};
