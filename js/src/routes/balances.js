/**
 * Balance Routes
 *
 * Implements the balance API endpoints:
 * - GET /v1/balances - List balances for an account
 */

const express = require('express');
const router = express.Router();
const { balanceService } = require('../services');
const { validateBalanceQuery } = require('../utils');
const { createError } = require('../middleware');

/**
 * GET /v1/balances
 * List balances for an account
 *
 * Query parameters:
 * - account (required): Account ID
 *
 * Response (200):
 * [
 *   {
 *     "currency": "USD",
 *     "balance": 5000
 *   },
 *   {
 *     "currency": "EUR",
 *     "balance": 3000
 *   }
 * ]
 */
router.get('/', async (req, res, next) => {
  try {
    // Validate query parameters
    const validation = validateBalanceQuery(req.query);
    if (!validation.valid) {
      throw createError('Invalid query parameters', 400, validation.errors);
    }

    // Get balances
    const balances = await balanceService.listBalances(
      req.customer.publicKey,
      validation.parsed.account
    );

    res.json(balances);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
