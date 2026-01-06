import httpx
import base64
import json
from src.config import RPC_URL

class BitcoinRPC:
    def __init__(self):
        self.url = RPC_URL
        self.id_counter = 0
        self.client = httpx.AsyncClient(timeout=30.0)

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

        try:
            # Handle Basic Auth if present in URL
            response = await self.client.post(self.url, json=payload)
            response.raise_for_status()
            result = response.json()
            
            if result["error"]:
                raise Exception(f"RPC Error: {result['error']}")
            
            return result["result"]
        except Exception as e:
            print(f"RPC Call Failed: {method} - {e}")
            raise

    async def get_block_count(self):
        return await self.call("getblockcount")

    async def get_block_hash(self, height: int):
        return await self.call("getblockhash", [height])

    async def get_block(self, block_hash: str, verbosity: int = 2):
        return await self.call("getblock", [block_hash, verbosity])

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
