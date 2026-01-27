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
        # Use heights above Taproot activation (709632)
        data = [
            (800000, b"block800k", b"tx1", b"tweak1"),
            (800000, b"block800k", b"tx2", b"tweak2"),
            (800001, b"block800k+1", b"tx3", b"tweak3"),
            (800005, b"block800k+5", b"tx4", b"tweak4"),
        ]
        
        # Cast to match schema explicitly
        df = pl.DataFrame(data, schema=["height", "block_hash", "tx_hash", "tweak"], orient="row")
        
        df.write_parquet(indexer.tweaks_dir / "tweaks_800000_800005.parquet")
        
        # Test 1: Fetch exact height
        tweaks = indexer.get_tweaks(800000, 1)
        assert len(tweaks) == 2
        assert tweaks[0]['height'] == 800000
        # Sort order is height, tx_hash. tx1 < tx2
        assert tweaks[0]['tx_hash'] == b"tx1".hex()
        assert tweaks[0]['tweak'] == b"tweak1".hex()
        assert tweaks[1]['tx_hash'] == b"tx2".hex()
        
        # Test 2: Fetch range
        tweaks = indexer.get_tweaks(800000, 2) # 800000 and 800001
        assert len(tweaks) == 3
        assert tweaks[2]['height'] == 800001
        assert tweaks[2]['tx_hash'] == b"tx3".hex()
        
        # Test 3: Fetch range with gap
        tweaks = indexer.get_tweaks(800000, 6) # 800000 to 800005
        assert len(tweaks) == 4
        assert tweaks[-1]['height'] == 800005
        assert tweaks[-1]['tx_hash'] == b"tx4".hex()
        
        # Test 4: Fetch empty range
        tweaks = indexer.get_tweaks(900000, 1)
        assert len(tweaks) == 0

    finally:
        src.indexer.DATA_DIR = orig_data_dir
