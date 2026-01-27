# CoreIndex

A lightweight Bitcoin indexer and Electrum server powered by Python and Polars.

## Overview
CoreIndex fetches blocks from Bitcoin Core via RPC and builds a high-performance index of headers, UTXOs, and address history using partitioned Parquet files with Zstd compression. It serves the Electrum protocol (JSON-RPC 2.0 over TCP), making it compatible with wallets like Sparrow.

For a detailed technical explanation of the indexing strategy and architecture, see [How it Works](how_does_it_work.md).

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
BITCOIN_RPC_URL=https://bitcoin-rpc.publicnode.com,http://user:pass@localhost:8332
ELECTRUM_HOST=0.0.0.0
ELECTRUM_PORT=50001
DATA_DIR=./data
# Start height for transaction indexing (pruning). Headers are always fetched.
INDEX_START_HEIGHT=0
# Optional: mainnet (default) or testnet
NETWORK=mainnet
```

### 2. Run the Server
```bash
uv run src/main.py
```

### 3. Silent Payments Benchmark
CoreIndex includes a benchmark tool to simulate a mobile wallet scanning for Silent Payments. It connects to your running server and scans for tweaks at high speeds (70,000+ tweaks/s).
```bash
uv run sp_benchmark.py
```
*Note: Use `--testnet` flag if running against a Testnet server.*

## Docker

### 1. Run from GitHub Container Registry
You can pull and run the pre-built image directly from GitHub.
Mount your local `./data` folder to persist the blockchain index:

```bash
docker run -d --restart unless-stopped \
  -v $(pwd)/data:/app/data \
  -p 50001:50001 \
  --env-file .env \
  --name coreindex \
  ghcr.io/dwerbam/coreindex:latest
```
*Note: Ensure your `.env` file contains a valid `BITCOIN_RPC_URL` reachable from the container.*

### 2. Build Locally (Optional)
If you prefer to build the image yourself:
```bash
docker build -t coreindex .
```

## Network Support

CoreIndex supports both Mainnet and Testnet. When you change the `NETWORK` environment variable, the server automatically adjusts its default port and indexing start height.

| Network | Default Port | Default Start Height | Description |
|---------|--------------|----------------------|-------------|
| `mainnet` | `50001` | `0` | The primary Bitcoin network. |
| `testnet` | `51001` | `0` | Public test network (Testnet3). |

### Using Testnet
To run on testnet, simply set the `NETWORK` variable in your `.env`:
```bash
NETWORK=testnet
BITCOIN_RPC_URL=http://user:pass@localhost:18332
```

**What happens automatically:**
- **Port**: Changes to `51001` (unless `ELECTRUM_PORT` is explicitly set).
- **Start Height**: Default is `0` (unless `INDEX_START_HEIGHT` is explicitly set).
- **Safety Check**: The indexer verifies the Testnet3 genesis block on startup to prevent data corruption.

**Docker Example for Testnet:**
```bash
docker run -d --restart unless-stopped \
  -v $(pwd)/data:/app/data \
  -p 51001:51001 \
  --env NETWORK=testnet \
  --env BITCOIN_RPC_URL=http://... \
  --name coreindex-testnet \
  ghcr.io/dwerbam/coreindex:latest
```

## Maintenance (Compact Index)
If the address index grows too fragmented (many small Parquet files), you can run a compaction task to merge them:
```bash
uv run python -m src.main --compact
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
