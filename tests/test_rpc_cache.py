import pytest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
from src.rpc import BitcoinRPC
from pathlib import Path

@pytest.mark.asyncio
async def test_rpc_cache(tmp_path):
    # Setup
    with patch("src.rpc.DATA_DIR", tmp_path), \
         patch("httpx.AsyncClient") as mock_client_cls:
        
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client
        
        # Define response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": {"hash": "cached_block"}, "error": None}
        mock_client.post.return_value = mock_response
        
        rpc = BitcoinRPC()
        # Ensure cache is initialized
        assert (tmp_path / "rpc_cache.db").exists()
        
        # 1. First Call - Miss
        res1 = await rpc.get_block("cached_block_hash", use_cache=True)
        assert res1["hash"] == "cached_block"
        assert mock_client.post.call_count == 1
        
        # 2. Second Call - Hit
        # Reset mock to ensure it's NOT called
        mock_client.post.reset_mock()
        
        res2 = await rpc.get_block("cached_block_hash", use_cache=True)
        assert res2["hash"] == "cached_block"
        assert mock_client.post.call_count == 0

@pytest.mark.asyncio
async def test_rpc_cache_ttl(tmp_path):
    # Setup
    with patch("src.rpc.DATA_DIR", tmp_path), \
         patch("httpx.AsyncClient") as mock_client_cls:
             
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": {"hash": "ttl_block"}, "error": None}
        mock_client.post.return_value = mock_response

        rpc = BitcoinRPC()
        
        # 1. Call and Cache
        await rpc.get_block("ttl_block_hash", use_cache=True)
        
        # 2. Simulate expiration
        # We need to manually expire the entry in DB
        import sqlite3
        import time
        with sqlite3.connect(tmp_path / "rpc_cache.db") as conn:
            # Set expires_at to past
            conn.execute("UPDATE cache SET expires_at = ?", (time.time() - 1,))
            conn.commit()
            
        # 3. Call again - Should be Miss (re-fetch)
        mock_client.post.reset_mock()
        mock_client.post.return_value = mock_response # Ensure return value available
        
        res = await rpc.get_block("ttl_block_hash", use_cache=True)
        assert res["hash"] == "ttl_block"
        assert mock_client.post.call_count == 1
