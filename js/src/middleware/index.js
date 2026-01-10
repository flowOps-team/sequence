/**
 * Middleware index
 * Exports all middleware modules
 */

const auth = require('./auth');
const errorHandler = require('./errorHandler');

module.exports = {
  ...auth,
  ...errorHandler
};
