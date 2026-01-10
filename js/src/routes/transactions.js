/**
 * Transaction Routes
 *
 * Implements the transaction API endpoints:
 * - POST /v1/transactions - Create a new transaction
 * - GET /v1/transactions - List transactions for an account
 * - GET /v1/transactions/:id - Get a specific transaction
 */

const express = require('express');
const router = express.Router();
const { transactionService } = require('../services');
const { validateTransaction, validateListQuery, normalizeCurrency } = require('../utils');
const { createError } = require('../middleware');

/**
 * POST /v1/transactions
 * Create a new transaction
 *
 * Request body:
 * {
 *   "from": "Alice",
 *   "to": "Bob",
 *   "amount": 1000,
 *   "currency": "USD",
 *   "metadata": ["optional", "tags"]
 * }
 *
 * Response (201):
 * {
 *   "id": "abc123",
 *   "from": "Alice",
 *   "to": "Bob",
 *   "amount": 1000,
 *   "balance": 5000,
 *   "currency": "USD",
 *   "date": "2024-01-10T15:30:45Z"
 * }
 */
router.post('/', async (req, res, next) => {
  try {
    // Parse and validate request body
    const body = req.body;

    // Normalize currency to uppercase
    if (body.currency) {
      body.currency = normalizeCurrency(body.currency);
    }

    // Validate transaction data
    const validation = validateTransaction(body);
    if (!validation.valid) {
      throw createError('Invalid transaction data', 400, validation.errors);
    }

    // Create the transaction
    const result = await transactionService.createTransaction(
      body,
      req.customer.publicKey
    );

    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

/**
 * GET /v1/transactions
 * List transactions for an account
 *
 * Query parameters:
 * - account (required): Account ID
 * - limit (optional): Max results (1-1000, default 1000)
 * - start_date (optional): Filter start date
 * - end_date (optional): Filter end date
 * - starting_after (optional): Pagination cursor
 *
 * Response (200):
 * {
 *   "transactions": [...],
 *   "total_debit": 5000,
 *   "total_credit": 10000,
 *   "balance": 5000
 * }
 */
router.get('/', async (req, res, next) => {
  try {
    // Validate query parameters
    const validation = validateListQuery(req.query);
    if (!validation.valid) {
      throw createError('Invalid query parameters', 400, validation.errors);
    }

    // List transactions
    const result = await transactionService.listTransactions(
      req.customer.publicKey,
      validation.parsed
    );

    res.json(result);
  } catch (error) {
    next(error);
  }
});

/**
 * GET /v1/transactions/:id
 * Get a specific transaction
 *
 * Response (200):
 * {
 *   "id": "abc123",
 *   "from": "Alice",
 *   "to": "Bob",
 *   "amount": 1000,
 *   "currency": "USD",
 *   "balance": 5000,
 *   "date": "2024-01-10T15:30:45Z"
 * }
 */
router.get('/:id', async (req, res, next) => {
  try {
    const transaction = await transactionService.getTransaction(
      req.customer.publicKey,
      req.params.id
    );

    if (!transaction) {
      throw createError('Transaction not found', 404);
    }

    res.json(transaction);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
