# CoreIndex

A lightweight Bitcoin indexer and Electrum server powered by Python and Polars.

## Overview
CoreIndex fetches blocks from Bitcoin Core via RPC and builds a high-performance index of headers, UTXOs, and address history using partitioned Parquet files with Zstd compression. It serves the Electrum protocol (JSON-RPC 2.0 over TCP), making it compatible with wallets like Sparrow.

## Features
- **Fast Lookups**: Partitioned address index for rapid history retrieval.
- **BIP-158 Compact Block Filters**: Serves `blockchain.block.get_filter` for modern light clients.
- **BIP-352 Silent Payments Ready**: Indexes shared-secret "tweaks" for transactions with Taproot outputs, enabling future Silent Payment scanning support.
- **Storage Efficient**: Automatic migration and Zstd compression of Parquet files.
- **Asyncio**: High-concurrency TCP server for multiple wallet connections.
- **Multi-Node Support**: Load balance or failover between multiple Bitcoin Core nodes.

## Quick Start

### 1. Configuration
Create a `.env` file:
```bash
# Supports multiple nodes (comma-separated) for redundancy and speed
BITCOIN_RPC_URL=http://user:pass@localhost:8332,http://user:pass@backup:8332
ELECTRUM_HOST=0.0.0.0
ELECTRUM_PORT=50001
DATA_DIR=./data
# Start height for transaction indexing (pruning). Headers are always fetched.
INDEX_START_HEIGHT=0
```

### 2. Run the Server
```bash
uv run src/main.py
```

## Docker

### 1. Build the Image
```bash
docker build -t coreindex .
```

### 2. Run with Persistent Data
Mount your local `./data` folder to persist the blockchain index:
```bash
docker run -d --restart unless-stopped\
  -v $(pwd)/data:/app/data \
  -p 50001:50001 \
  --env-file .env \
  --name coreindex \
  coreindex
```
*Note: Ensure your `.env` file contains a valid `BITCOIN_RPC_URL` reachable from the container.*

## Maintenance (Compact Index)
```

## Advanced Features

### Silent Payments (BIP-352)
CoreIndex is optimized for Silent Payments. It automatically identifies eligible Taproot transactions and pre-calculates the shared secret tweaks. This allows compatible light clients to scan for payments without downloading full blocks.

### Compact Block Filters (BIP-158)
The server implements the server-side logic for GCS (Golomb-Coded Sets). Clients can request filters per block height to locally verify transaction inclusion while preserving privacy.

## Development

### Run Tests
```bash
PYTHONPATH=. uv run pytest tests/
```
