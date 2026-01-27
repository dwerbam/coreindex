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
NETWORK = os.getenv("NETWORK", "mainnet").lower()

# Network Defaults
DEFAULT_PORTS = {"mainnet": 50001, "testnet": 51001}
DEFAULT_START_BLOCKS = {"mainnet": 0, "testnet": 0}

# Taproot Activation Heights (BIP-341)
TAPROOT_ACTIVATION = {
    "mainnet": 709632,
    "testnet": 2091968,
    "regtest": 0,
    "signet": 0
}

PORT = int(os.getenv("ELECTRUM_PORT", DEFAULT_PORTS.get(NETWORK, 50001)))
INDEX_START_HEIGHT = int(os.getenv("INDEX_START_HEIGHT", DEFAULT_START_BLOCKS.get(NETWORK, 0)))

# Data Storage
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Application Constants
FLUSH_INTERVAL = 500  # Blocks to process before flushing to disk
