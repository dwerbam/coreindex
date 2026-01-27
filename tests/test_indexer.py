import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.indexer import Indexer, get_scripthash

# Mock RPC
class MockRPC:
    async def get_block_count(self):
        return 100

    async def get_block_hash(self, height):
        # Valid 64-char hex
        return "00000000000000000000000000000000000000000000000000000000deadbeef"

    async def get_block(self, block_hash, verbosity=2, **kwargs):
        # Return a dummy block with 1 tx
        return {
            "hash": block_hash,
            "tx": [
                {
                    "txid": "0000000000000000000000000000000000000000000000000000000000000001", # Valid 64-char hex
                    "vin": [{"coinbase": "00"}],
                    "vout": [
                        {
                            "value": 50.0,
                            "scriptPubKey": {"hex": "76a914deadbeefdeadbeefdeadbeefdeadbeefdeadbeef88ac"}
                        }
                    ]
                }
            ]
        }

@pytest.mark.asyncio
async def test_scripthash():
    # p2pkh for deadbeef...
    # script: 76a914deadbeefdeadbeefdeadbeefdeadbeefdeadbeef88ac
    # sha256: ...
    # This is just a smoke test for the function
    sh = get_scripthash("76a914deadbeefdeadbeefdeadbeefdeadbeefdeadbeef88ac")
    assert sh is not None
    assert len(sh) == 32
    assert isinstance(sh, bytes)

@pytest.mark.asyncio
async def test_indexer_sync(tmp_path):
    # Patch DATA_DIR
    import src.indexer
    orig_data_dir = src.indexer.DATA_DIR
    src.indexer.DATA_DIR = tmp_path
    
    try:
        rpc = MockRPC()
        indexer = Indexer(rpc)
        # Fix paths in instance since they are set in __init__ using the global DATA_DIR
        # But wait, we patched the module attribute before init, so it should be fine?
        # Indexer.__init__ uses `from src.config import DATA_DIR` style import?
        # No, src.indexer.py has `from src.config import DATA_DIR`.
        # So patching src.indexer.DATA_DIR works if we do it before Indexer() is called.
        
        # Mocking storage to avoid writing files during test
        indexer.save_db = MagicMock() 
        indexer.check_network = AsyncMock()
        
        # Run sync for 1 block (current height starts at -1 or 0 if empty)
        # We need to monkeypatch get_block_count or rely on the mock
        # The mock returns 100, so it will try to sync 100 blocks. 
        # That's too many for a unit test.
        rpc.get_block_count = AsyncMock(return_value=1)
        
        # Patch FLUSH_INTERVAL and START_BLOCK
        with patch("src.indexer.FLUSH_INTERVAL", 1), \
             patch("src.indexer.Indexer.START_BLOCK", 0):
            # Consume the generator
            async for _ in indexer.sync():
                pass
        
        # Verify headers
        # Note: headers/history are not in-memory objects in the new implementation, 
        # they are flushed to disk. But save_db is mocked, so nothing hits disk.
        # So we can't verify them easily unless we inspect the batch buffers.
        
        assert len(indexer.batch_headers) > 0
        assert len(indexer.batch_new_utxos) > 0
        
    finally:
        src.indexer.DATA_DIR = orig_data_dir
