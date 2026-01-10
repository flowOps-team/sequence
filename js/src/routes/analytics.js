/**
 * Analytics Routes
 *
 * Implements the analytics API endpoints:
 * - GET /v1/stats - Get transaction statistics
 * - GET /v1/trends - Get transaction trends by period
 * - GET /v1/aggregation - Get cash flow aggregation
 */

const express = require('express');
const router = express.Router();
const { analyticsService } = require('../services');
const { validateStatsQuery } = require('../utils');
const { createError } = require('../middleware');

/**
 * GET /v1/stats
 * Get transaction statistics for accounts
 *
 * Query parameters:
 * - account (required): Comma-separated account IDs
 * - period (optional): 'daily', 'weekly', or 'monthly'
 * - start_date (optional): Filter start date
 * - end_date (optional): Filter end date
 * - currency (optional): Filter by currency
 *
 * Response (200):
 * {
 *   "totalTransactions": 42,
 *   "transactionVolume": 125000,
 *   "averageTransactionSize": 2976.19
 * }
 */
router.get('/stats', async (req, res, next) => {
  try {
    // Validate query parameters
    const validation = validateStatsQuery(req.query);
    if (!validation.valid) {
      throw createError('Invalid query parameters', 400, validation.errors);
    }

    const { accounts, startDate, endDate } = validation.parsed;

    // Get statistics
    const stats = await analyticsService.getStats(
      req.customer.publicKey,
      accounts,
      {
        startDate,
        endDate,
        currency: req.query.currency
      }
    );

    res.json(stats);
  } catch (error) {
    next(error);
  }
});

/**
 * GET /v1/trends
 * Get transaction trends grouped by period
 *
 * Query parameters:
 * - account (required): Comma-separated account IDs
 * - period (optional): 'daily', 'weekly', or 'monthly' (default: monthly)
 * - start_date (optional): Filter start date
 * - end_date (optional): Filter end date
 * - currency (optional): Filter by currency
 *
 * Response (200):
 * [
 *   {
 *     "period": { "year": 2024, "month": 1 },
 *     "volume": 50000,
 *     "count": 20
 *   },
 *   ...
 * ]
 */
router.get('/trends', async (req, res, next) => {
  try {
    // Validate query parameters
    const validation = validateStatsQuery(req.query);
    if (!validation.valid) {
      throw createError('Invalid query parameters', 400, validation.errors);
    }

    const { accounts, period, startDate, endDate } = validation.parsed;

    // Get trends
    const trends = await analyticsService.getTrends(
      req.customer.publicKey,
      accounts,
      period || 'monthly',
      {
        startDate,
        endDate,
        currency: req.query.currency
      }
    );

    res.json(trends);
  } catch (error) {
    next(error);
  }
});

/**
 * GET /v1/aggregation
 * Get cash flow aggregation (inflow/outflow) by period
 *
 * Query parameters:
 * - account (required): Comma-separated account IDs
 * - period (optional): 'daily', 'weekly', or 'monthly' (default: monthly)
 * - start_date (optional): Filter start date
 * - end_date (optional): Filter end date
 * - currency (optional): Filter by currency
 *
 * Response (200):
 * [
 *   {
 *     "period": { "year": 2024, "month": 1 },
 *     "inflow": 100000,
 *     "outflow": 50000,
 *     "net": 50000,
 *     "transactionCount": 25
 *   },
 *   ...
 * ]
 */
router.get('/aggregation', async (req, res, next) => {
  try {
    // Validate query parameters
    const validation = validateStatsQuery(req.query);
    if (!validation.valid) {
      throw createError('Invalid query parameters', 400, validation.errors);
    }

    const { accounts, period, startDate, endDate } = validation.parsed;

    // Get aggregation
    const aggregation = await analyticsService.getAggregation(
      req.customer.publicKey,
      accounts,
      period || 'monthly',
      {
        startDate,
        endDate,
        currency: req.query.currency
      }
    );

    res.json(aggregation);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
