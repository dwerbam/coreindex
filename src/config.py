import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Bitcoin Core RPC
RPC_URLS = [
    url.strip()
    for url in os.getenv("BITCOIN_RPC_URL", "https://bitcoin-rpc.publicnode.com").split(",")
    if url.strip()
]

# Electrum Server
HOST = os.getenv("ELECTRUM_HOST", "0.0.0.0")
PORT = int(os.getenv("ELECTRUM_PORT", "50001"))

# Data Storage
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Application Constants
FLUSH_INTERVAL = 1000  # Blocks to process before flushing to disk
