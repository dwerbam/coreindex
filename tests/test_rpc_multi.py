import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from src.rpc import BitcoinRPC

@pytest.mark.asyncio
async def test_rpc_race_condition():
    # Setup
    rpc = BitcoinRPC()
    rpc.urls = ["http://slow", "http://fast"]
    
    # Mock _call_single
    async def side_effect(url, payload):
        if url == "http://slow":
            await asyncio.sleep(0.5)
            return "slow_result"
        elif url == "http://fast":
            await asyncio.sleep(0.01)
            return "fast_result"
        return "unknown"

    with patch.object(rpc, '_call_single', side_effect=side_effect):
        result = await rpc.call("getblockcount")
        assert result == "fast_result"

@pytest.mark.asyncio
async def test_rpc_failover():
    # Setup
    rpc = BitcoinRPC()
    rpc.urls = ["http://fail", "http://success"]
    
    # Mock _call_single
    async def side_effect(url, payload):
        if url == "http://fail":
            raise Exception("Failed")
        elif url == "http://success":
            return "success_result"
        return "unknown"

    with patch.object(rpc, '_call_single', side_effect=side_effect):
        result = await rpc.call("getblockcount")
        assert result == "success_result"

@pytest.mark.asyncio
async def test_rpc_all_fail():
    # Setup
    rpc = BitcoinRPC()
    rpc.urls = ["http://fail1", "http://fail2"]
    
    # Patch asyncio.sleep to speed up retries
    with patch("asyncio.sleep", new_callable=AsyncMock), \
         patch.object(rpc, '_call_single', side_effect=Exception("All Failed")):
        
        with pytest.raises(Exception, match="All Failed"):
             await rpc.call("getblockcount")
