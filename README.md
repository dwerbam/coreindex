# CoreIndex

A lightweight Bitcoin indexer and Electrum server powered by Python and Polars.

## Overview
CoreIndex fetches blocks from Bitcoin Core via RPC and builds a high-performance index of headers, UTXOs, and address history using partitioned Parquet files with Zstd compression. It serves the Electrum protocol (JSON-RPC 2.0 over TCP), making it compatible with wallets like Sparrow.

## Quick Start

### 1. Configuration
Create a `.env` file:
```bash
BITCOIN_RPC_URL=http://user:pass@localhost:8332
ELECTRUM_HOST=0.0.0.0
ELECTRUM_PORT=50001
DATA_DIR=./data
```

### 2. Run the Server
```bash
uv run src/main.py
```

### 3. Maintenance (Compact Index)
Merge small index files into larger, optimized ones to save disk space:
```bash
uv run src/main.py --compact
```

## Development

### Run Tests
```bash
PYTHONPATH=. uv run pytest tests/
```

### Key Features
- **Fast Lookups**: Partitioned address index for rapid history retrieval.
- **Storage Efficient**: Automatic migration and Zstd compression of Parquet files.
- **Asyncio**: High-concurrency TCP server for multiple wallet connections.