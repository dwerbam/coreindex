import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Bitcoin Core RPC
RPC_URL = os.getenv("BITCOIN_RPC_URL", "http://user:pass@localhost:8332")

# Electrum Server
HOST = os.getenv("ELECTRUM_HOST", "0.0.0.0")
PORT = int(os.getenv("ELECTRUM_PORT", "50001"))

# Data Storage
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Application Constants
batch_size = 10  # Blocks to process in parallel/batch
