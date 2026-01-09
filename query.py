import sys
import polars as pl
from pathlib import Path

DATA_DIR = Path("data")
ADDR_INDEX_PATH = DATA_DIR / "addr_index"
UTXO_PATH = DATA_DIR / "utxo.parquet"

def query_scripthash(sh: str):
    print(f"--- Querying Scripthash: {sh} ---")
    
    # 1. Search History (in Partitioned Index)
    prefix = sh[:2]
    partition_path = ADDR_INDEX_PATH / f"sh_prefix={prefix}"
    
    if not partition_path.exists():
        print(f"History: No data found for prefix {prefix}")
    else:
        try:
            # Lazy scan all parquet files in the specific prefix partition
            history = (
                pl.scan_parquet(partition_path / "*.parquet")
                .filter(pl.col("scripthash") == sh)
                .sort("height", "tx_hash")
                .collect()
            )
            
            if history.is_empty():
                print("History: No transactions found.")
            else:
                print(f"History: Found {len(history)} transactions")
                print(history)
        except Exception as e:
            print(f"Error reading history: {e}")

    # 2. Search UTXOs
    print("\n--- Balance / UTXOs ---")
    if not UTXO_PATH.exists():
        print("UTXO: database file not found.")
    else:
        try:
            utxos = (
                pl.scan_parquet(UTXO_PATH)
                .filter(pl.col("scripthash") == sh)
                .collect()
            )
            
            if utxos.is_empty():
                print("Balance: 0 sats (No UTXOs)")
            else:
                total_balance = utxos["value"].sum()
                print(f"Total Balance: {total_balance} sats")
                print("UTXOs:")
                print(utxos)
        except Exception as e:
            print(f"Error reading UTXOs: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query.py <scripthash>")
        sys.exit(1)
        
    query_scripthash(sys.argv[1])
