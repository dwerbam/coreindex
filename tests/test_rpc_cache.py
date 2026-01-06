import pytest
import asyncio
import json
import gzip
import time
from unittest.mock import MagicMock, AsyncMock, patch
from src.rpc import BitcoinRPC
from pathlib import Path

@pytest.mark.asyncio
async def test_rpc_cache(tmp_path):
    # Setup
    with patch("src.rpc.DATA_DIR", tmp_path), \
         patch("src.rpc.RPC_URLS", ["http://mock"]), \
         patch("httpx.AsyncClient") as mock_client_cls:
        
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client
        
        # Define response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": {"hash": "cached_block"}, "error": None}
        mock_client.post.return_value = mock_response
        
        rpc = BitcoinRPC()
        # Ensure cache dir is initialized
        assert (tmp_path / "rpc_cache").exists()
        
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
         patch("src.rpc.RPC_URLS", ["http://mock"]), \
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
        # Find the cached file
        cache_dir = tmp_path / "rpc_cache"
        found = False
        for p in cache_dir.rglob("*.json.gz"):
            found = True
            # Read, modify expires_at, Write
            with gzip.open(p, "rt", encoding="utf-8") as f:
                data = json.load(f)
            
            data["expires_at"] = time.time() - 1
            
            with gzip.open(p, "wt", encoding="utf-8") as f:
                json.dump(data, f)
            break
        
        assert found, "Cache file not found"
            
        # 3. Call again - Should be Miss (re-fetch)
        mock_client.post.reset_mock()
        mock_client.post.return_value = mock_response # Ensure return value available
        
        res = await rpc.get_block("ttl_block_hash", use_cache=True)
        assert res["hash"] == "ttl_block"
        assert mock_client.post.call_count == 1