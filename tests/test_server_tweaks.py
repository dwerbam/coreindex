import pytest
import polars as pl
from unittest.mock import MagicMock
from src.indexer import Indexer
import src.indexer

@pytest.mark.asyncio
async def test_get_tweaks(tmp_path):
    # Patch DATA_DIR
    orig_data_dir = src.indexer.DATA_DIR
    src.indexer.DATA_DIR = tmp_path
    
    try:
        rpc = MagicMock()
        indexer = Indexer(rpc)
        # Fix internal paths since they are set in __init__ using the passed DATA_DIR (which is now tmp_path via patch?)
        # Actually Indexer uses imported DATA_DIR in __init__ for some things, but let's just manually set it to be sure
        indexer.tweaks_dir = tmp_path / "tweaks"
        indexer.tweaks_dir.mkdir(parents=True, exist_ok=True)
        
        # Create dummy tweak data
        # Using schema consistent with Indexer.init_db
        data = [
            (100, b"block100", b"tx1", b"tweak1"),
            (100, b"block100", b"tx2", b"tweak2"),
            (101, b"block101", b"tx3", b"tweak3"),
            (105, b"block105", b"tx4", b"tweak4"),
        ]
        
        # Cast to match schema explicitly
        df = pl.DataFrame(data, schema=["height", "block_hash", "tx_hash", "tweak"], orient="row")
        
        df.write_parquet(indexer.tweaks_dir / "tweaks_100_105.parquet")
        
        # Test 1: Fetch exact height
        tweaks = indexer.get_tweaks(100, 1)
        assert len(tweaks) == 2
        assert tweaks[0]['height'] == 100
        # Sort order is height, tx_hash. tx1 < tx2
        assert tweaks[0]['tx_hash'] == b"tx1".hex()
        assert tweaks[0]['tweak'] == b"tweak1".hex()
        assert tweaks[1]['tx_hash'] == b"tx2".hex()
        
        # Test 2: Fetch range
        tweaks = indexer.get_tweaks(100, 2) # 100 and 101
        assert len(tweaks) == 3
        assert tweaks[2]['height'] == 101
        assert tweaks[2]['tx_hash'] == b"tx3".hex()
        
        # Test 3: Fetch range with gap
        tweaks = indexer.get_tweaks(100, 6) # 100 to 105
        assert len(tweaks) == 4
        assert tweaks[-1]['height'] == 105
        assert tweaks[-1]['tx_hash'] == b"tx4".hex()
        
        # Test 4: Fetch empty range
        tweaks = indexer.get_tweaks(200, 1)
        assert len(tweaks) == 0

    finally:
        src.indexer.DATA_DIR = orig_data_dir
