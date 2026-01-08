import httpx
import base64
import json
import itertools
import asyncio
from src.config import RPC_URLS

class BitcoinRPC:
    def __init__(self):
        self.urls = RPC_URLS
        self.url_cycle = itertools.cycle(self.urls)
        self.id_counter = 0
        limits = httpx.Limits(max_keepalive_connections=50, max_connections=200)
        self.client = httpx.AsyncClient(timeout=30.0, limits=limits)

    async def call(self, method: str, params: list = None):
        if params is None:
            params = []
        
        self.id_counter += 1
        payload = {
            "jsonrpc": "1.0",
            "id": f"coreindex-{self.id_counter}",
            "method": method,
            "params": params
        }

        retries = 3
        delay = 1
        
        for attempt in range(retries):
            # Round-robin selection
            url = next(self.url_cycle)
            
            try:
                # Handle Basic Auth if present in URL
                response = await self.client.post(url, json=payload)
                response.raise_for_status()
                result = response.json()
                
                if result["error"]:
                    raise Exception(f"RPC Error: {result['error']}")
                
                return result["result"]
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 503 and attempt < retries - 1:
                    # print(f"RPC 503 (Busy) at {url}, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                print(f"RPC Call Failed at {url}: {method} - {e}")
                raise
            except Exception as e:
                # If one node fails, we might want to try another, but simple retry logic handles transient issues.
                # Ideally we should failover to next URL on connection error.
                if attempt < retries - 1:
                     # print(f"RPC connection failed at {url}, retrying...")
                     await asyncio.sleep(delay)
                     continue
                print(f"RPC Call Failed at {url}: {method} - {e}")
                raise

    async def get_block_count(self):
        return await self.call("getblockcount")

    async def get_block_hash(self, height: int):
        return await self.call("getblockhash", [height])

    async def get_best_block_hash(self):
        return await self.call("getbestblockhash")

    async def get_transaction(self, tx_hash: str, verbose: bool = False):
        return await self.call("getrawtransaction", [tx_hash, verbose])
        
    async def estimate_smart_fee(self, blocks: int):
        return await self.call("estimatesmartfee", [blocks])
    
    async def send_raw_transaction(self, hex_tx: str):
        return await self.call("sendrawtransaction", [hex_tx])

    async def close(self):
        await self.client.aclose()
