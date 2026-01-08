import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Bitcoin Core RPC
RPC_URL = os.getenv("BITCOIN_RPC_URL", "http://user:pass@localhost:8332")
RPC_URLS = os.getenv("BITCOIN_RPC_URLS", RPC_URL).split(",")
RPC_URLS = [url.strip() for url in RPC_URLS if url.strip()]

# Esplora REST API (Comma separated list)
ESPLORA_URLS = os.getenv("ESPLORA_URLS", "").split(",")
ESPLORA_URLS = [url.strip() for url in ESPLORA_URLS if url.strip()]

# Electrum Server
HOST = os.getenv("ELECTRUM_HOST", "0.0.0.0")
PORT = int(os.getenv("ELECTRUM_PORT", "50001"))

# Data Storage
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Application Constants
batch_size = 10  # Blocks to process in parallel/batch
