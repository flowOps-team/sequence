/**
 * Sequence Ledger API Server
 *
 * An immutable double-entry ledger system built with:
 * - Node.js
 * - Express
 * - MongoDB
 *
 * Migrated from Clojure/Pedestal/DynamoDB while preserving
 * the original ledger design, algorithms, and architecture.
 */

const express = require('express');
const mongoose = require('mongoose');
const config = require('./config');
const { configureRoutes } = require('./routes');
const { errorHandler, notFoundHandler } = require('./middleware');

// Create Express application
const app = express();

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// CORS headers (if needed)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// Request logging
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`${req.method} ${req.path} ${res.statusCode} ${duration}ms`);
  });
  next();
});

// Configure routes
configureRoutes(app);

// Error handling
app.use(notFoundHandler);
app.use(errorHandler);

/**
 * Connect to MongoDB and start the server
 */
async function start() {
  try {
    // Connect to MongoDB
    console.log('Connecting to MongoDB...');
    await mongoose.connect(config.mongodb.uri, config.mongodb.options);
    console.log('Connected to MongoDB');

    // Start the server
    app.listen(config.port, () => {
      console.log(`Sequence Ledger API running on port ${config.port}`);
      console.log(`Environment: ${config.env}`);
      console.log(`API endpoint: http://localhost:${config.port}/v1`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully...');
  await mongoose.connection.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully...');
  await mongoose.connection.close();
  process.exit(0);
});

// Start the server
start();

module.exports = app;
