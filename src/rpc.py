import base64
import json
import asyncio
import httpx

from src.config import RPC_URLS


class BitcoinRPC:
    def __init__(self):
        self.urls = RPC_URLS
        self.id_counter = 0
        self.active_calls = 0
        limits = httpx.Limits(max_keepalive_connections=15, max_connections=80)
        self.client = httpx.AsyncClient(timeout=60.0, limits=limits)

    def get_stats(self):
        return {
            "active_calls": self.active_calls,
            "total_calls": self.id_counter,
            "servers": len(self.urls)
        }

    async def _call_single(self, url: str, payload: dict):
        try:
            # Handle Basic Auth if present in URL
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            result = response.json()

            if result["error"]:
                raise Exception(f"RPC Error: {result['error']}")

            return result["result"]
        except Exception as e:
            raise e

    async def call(self, method: str, params: list = None):
        if params is None:
            params = []

        self.id_counter += 1
        self.active_calls += 1

        payload = {
            "jsonrpc": "1.0",
            "id": f"coreindex-{self.id_counter}",
            "method": method,
            "params": params,
        }

        retries = 10
        delay = 5

        try:
            for attempt in range(retries):
                pending = []
                for url in self.urls:
                    pending.append(asyncio.create_task(self._call_single(url, payload)))

                errors = []
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            result = task.result()
                            # Success! Cancel others
                            for p in pending:
                                p.cancel()
                            return result
                        except Exception as e:
                            errors.append(e)

                # If we are here, all servers failed for this attempt
                if attempt == retries - 1:
                    if errors:
                        print(f"All RPC servers failed. Last error: {errors[-1]}")
                        raise errors[-1]
                    raise Exception("All RPC servers failed with no error captured.")

                # Wait before global retry
                await asyncio.sleep(delay)
                delay *= 2

        finally:
            self.active_calls -= 1

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
