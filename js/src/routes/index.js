/**
 * Routes index
 * Configures and exports all API routes
 */

const express = require('express');
const transactionRoutes = require('./transactions');
const balanceRoutes = require('./balances');
const analyticsRoutes = require('./analytics');
const { authenticate } = require('../middleware');

/**
 * Configure all routes
 * @param {express.Application} app - Express application
 */
function configureRoutes(app) {
  const router = express.Router();

  // Health check (no auth required)
  router.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // Protected routes (require authentication)
  router.use('/transactions', authenticate, transactionRoutes);
  router.use('/balances', authenticate, balanceRoutes);
  router.use('/stats', authenticate, analyticsRoutes);
  router.use('/trends', authenticate, analyticsRoutes);
  router.use('/aggregation', authenticate, analyticsRoutes);

  // Mount router at /v1
  app.use('/v1', router);
}

module.exports = { configureRoutes };
