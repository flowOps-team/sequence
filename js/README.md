# Sequence Ledger API (Node.js/MongoDB)

An immutable double-entry ledger system built with Node.js, Express, and MongoDB.

This is a migration of the original Clojure/Pedestal/DynamoDB implementation, preserving the same ledger design, algorithms, and architecture.

## Features

- **Double-Entry Ledger**: Each transaction creates two records (debit + credit)
- **Immutable Transactions**: Append-only design with no updates
- **Derived Balances**: Balances calculated from transaction history
- **Multi-Currency Support**: Each account can hold multiple currencies
- **Multi-Tenancy**: Isolated data per API key (public key)
- **Genesis Transactions**: Account seeding mechanism

## Quick Start

### Using Docker Compose

```bash
# Start MongoDB and the API
docker-compose up -d

# API will be available at http://localhost:8910/v1
```

### Manual Setup

```bash
# Install dependencies
npm install

# Start MongoDB (if not using Docker)
mongod --dbpath /path/to/data

# Copy environment file
cp .env.example .env

# Start the server
npm start

# For development with auto-reload
npm run dev
```

## API Endpoints

All endpoints require authentication via HTTP Basic Auth with your API key.

### Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/transactions` | Create a new transaction |
| GET | `/v1/transactions` | List transactions for an account |
| GET | `/v1/transactions/:id` | Get a specific transaction |

### Balances

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/balances` | List balances for an account |

### Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/stats` | Get transaction statistics |
| GET | `/v1/trends` | Get transaction trends by period |
| GET | `/v1/aggregation` | Get cash flow aggregation |

## Authentication

Use HTTP Basic Authentication with your API key:

```bash
# Encode your API key in Base64 (key:)
echo -n "your-api-key:" | base64
# Output: eW91ci1hcGkta2V5Og==

# Use in requests
curl -H "Authorization: Basic eW91ci1hcGkta2V5Og==" \
  http://localhost:8910/v1/balances?account=Alice
```

Development API key: `123` (Base64: `MTIz`)

## Example Usage

### Create a Transaction

```bash
curl -X POST http://localhost:8910/v1/transactions \
  -H "Authorization: Basic MTIzOg==" \
  -H "Content-Type: application/json" \
  -d '{
    "from": "dev",
    "to": "Alice",
    "amount": 1000,
    "currency": "USD"
  }'
```

Response:
```json
{
  "id": "a3f5b8c9d2e1...",
  "from": "dev",
  "to": "Alice",
  "amount": 1000,
  "balance": 1000,
  "currency": "USD",
  "date": "2024-01-10T15:30:45.000Z"
}
```

### List Transactions

```bash
curl http://localhost:8910/v1/transactions?account=Alice \
  -H "Authorization: Basic MTIzOg=="
```

### Get Balance

```bash
curl http://localhost:8910/v1/balances?account=Alice \
  -H "Authorization: Basic MTIzOg=="
```

## Ledger Design

### Double-Entry System

Every transaction creates two immutable records:

1. **Debit Record**: Sender's perspective (balance decreases)
2. **Credit Record**: Receiver's perspective (balance increases)

### Genesis Transactions

When the sender (`from`) equals the public key, a genesis transaction is created to seed the account with initial funds. This allows creating money supply in the system.

### Balance Calculation

Balances are derived from the most recent transaction record for each account/currency pair. No separate balance field is updated, ensuring complete audit trail.

## Project Structure

```
js/
├── src/
│   ├── config/         # Configuration
│   ├── middleware/     # Express middleware (auth, error handling)
│   ├── models/         # MongoDB schemas
│   ├── routes/         # API routes
│   ├── services/       # Business logic
│   ├── utils/          # Utility functions
│   └── server.js       # Application entry point
├── docker-compose.yml  # Docker configuration
├── Dockerfile          # Container build
└── package.json        # Dependencies
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | 8910 |
| `NODE_ENV` | Environment | development |
| `MONGODB_URI` | MongoDB connection string | mongodb://localhost:27017/sequence_ledger |
| `KEYS` | API keys JSON array | Development key |

## License

MIT
