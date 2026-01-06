import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from src.indexer import Indexer

@pytest.mark.asyncio
async def test_fetch_worker_retries_on_error(tmp_path):
    # Setup
    mock_rpc = AsyncMock()
    mock_rpc.get_block_count.return_value = 105
    
    # Define side effect for get_block_hash to fail on first call
    # We use a mutable container to track state across calls
    state = {"fail_count": 1}
    
    async def get_hash_side_effect(height):
        if state["fail_count"] > 0:
            state["fail_count"] -= 1
            raise Exception("Transient RPC Error")
        return f"hash_{height}"

    mock_rpc.get_block_hash.side_effect = get_hash_side_effect
    
    async def get_block_side_effect(block_hash, verbosity=2, **kwargs):
        return {
            "hash": block_hash, 
            "height": 100, # Dummy height
            "time": 1234567890, 
            "tx": [], 
            "previousblockhash": "prev"
        }
    mock_rpc.get_block.side_effect = get_block_side_effect

    with patch("src.indexer.DATA_DIR", tmp_path):
        indexer = Indexer(mock_rpc)
        # Mock DB methods to avoid file I/O
        indexer.process_block = MagicMock()
        indexer.save_db = MagicMock(return_value=set())
        
        # Mock height property
        with patch("src.indexer.Indexer.height", new_callable=PropertyMock) as mock_height:
            mock_height.return_value = 100

            # Patch sleep to run faster, and START_BLOCK
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
                 patch("src.indexer.FLUSH_INTERVAL", 1), \
                 patch("src.indexer.Indexer.START_BLOCK", 0):
                # Run sync
                # We convert the async generator to a list to run it to completion
                [_ async for _ in indexer.sync()]

            # Assertions
            # 1. Verify get_block_hash was called multiple times (retries)
            assert mock_rpc.get_block_hash.call_count > 5
            
            # 2. Verify we actually processed blocks despite the initial error
            assert indexer.process_block.call_count == 5 # 101 to 105
            
            # 3. Verify sleep was called (backoff)
            assert mock_sleep.call_count > 0