import asyncio
import json
import traceback
import hashlib
from rich import print
from src.config import HOST, PORT
from src.rpc import BitcoinRPC
from src.indexer import Indexer

VERSION = "1.1"
BANNER = "CoreIndex Electrum Server 1.1 (with BIP-158 & SP Tweaks)"

class ElectrumSession:
    def __init__(self, reader, writer, server):
        self.reader = reader
        self.writer = writer
        self.server = server
        self.peer_name = writer.get_extra_info('peername')
        self.subscriptions = set() # scripthashes

    async def notify(self, scripthash, status):
        notification = {
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [scripthash, status]
        }
        try:
            self.writer.write((json.dumps(notification) + '\n').encode('utf-8'))
            await self.writer.drain()
            print(f"[magenta]Notification sent to {self.peer_name}:[/magenta] {scripthash[:10]}...")
        except Exception as e:
            print(f"Failed to notify {self.peer_name}: {e}")

    async def notify_header(self, result):
        notification = {
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [result]
        }
        try:
            self.writer.write((json.dumps(notification) + '\n').encode('utf-8'))
            await self.writer.drain()
            print(f"[magenta]Header notification sent to {self.peer_name}:[/magenta] height={result.get('height')}")
        except Exception as e:
            print(f"Failed to notify header to {self.peer_name}: {e}")

    async def handle(self):
        self.server.add_session(self)
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                
                line = data.decode('utf-8').strip()
                if not line:
                    continue
                
                try:
                    request = json.loads(line)
                    response = await self.process_request(request)
                    if response:
                        self.writer.write((json.dumps(response) + '\n').encode('utf-8'))
                        await self.writer.drain()
                except json.JSONDecodeError:
                    print(f"Invalid JSON from {self.peer_name}")
                    break
                except Exception as e:
                    print(f"Error handling request: {e}")
                    traceback.print_exc()
        except Exception as e:
            print(f"Connection error {self.peer_name}: {e}")
        finally:
            self.server.remove_session(self)
            self.server.remove_header_session(self)
            self.writer.close()
            await self.writer.wait_closed()

    async def process_request(self, req):
        if 'id' not in req:
            return None # Notification (ignore for now)
            
        method = req.get('method')
        params = req.get('params', [])
        req_id = req.get('id')
        
        # Log request with params (truncated)
        params_str = str(params)
        if len(params_str) > 100:
            params_str = params_str[:100] + "..."
        print(f"[blue]Request:[/blue] {method} id={req_id} params={params_str}")
        
        result = None
        error = None

        try:
            if method == 'server.version':
                result = [BANNER, VERSION]
            elif method == 'server.banner':
                result = BANNER
            elif method == 'server.ping':
                result = None
            elif method == 'blockchain.headers.subscribe':
                self.server.add_header_session(self)
                height = self.server.indexer.height
                if height < 0: height = 0
                
                try:
                    block_hash = await self.server.rpc.get_block_hash(height)
                    block_header = await self.server.rpc.call("getblockheader", [block_hash, False])
                    result = {"hex": block_header, "height": height}
                except Exception as e:
                    print(f"Error fetching header for subscription: {e}")
                    result = None

            elif method == 'blockchain.block.header':
                height = params[0]
                header = self.server.indexer.get_header(height)
                if header:
                    result = header
                else:
                    block_hash = await self.server.rpc.get_block_hash(height)
                    result = await self.server.rpc.call("getblockheader", [block_hash, False])
            elif method == 'blockchain.block.get_filter':
                height = params[0]
                result = self.server.indexer.get_filter(height)
            elif method == 'blockchain.estimatefee':
                blocks = params[0]
                res = await self.server.rpc.estimate_smart_fee(blocks)
                result = res.get("feerate", -1)
            elif method == 'blockchain.relayfee':
                result = 0.00001
            elif method == 'blockchain.scripthash.get_balance':
                sh = params[0]
                result = self.server.indexer.get_balance(sh)
            elif method == 'blockchain.scripthash.get_history':
                sh = params[0]
                result = self.server.indexer.get_history(sh)
            elif method == 'blockchain.scripthash.listunspent':
                sh = params[0]
                result = self.server.indexer.list_unspent(sh)
            elif method == 'blockchain.scripthash.subscribe':
                sh = params[0]
                self.subscriptions.add(sh)
                history = self.server.indexer.get_history(sh)
                if not history:
                    result = None
                else:
                    # Deterministic sort for status calculation
                    history.sort(key=lambda x: (x['height'], x['tx_hash']))
                    status_str = ""
                    for item in history:
                        status_str += f"{item['tx_hash']}:{item['height']}:"
                    result = hashlib.sha256(status_str.encode()).hexdigest()
            elif method == 'mempool.get_fee_histogram':
                result = []
            elif method == 'blockchain.scripthash.get_mempool':
                result = []
            elif method == 'blockchain.transaction.get':
                tx_hash = params[0]
                verbose = params[1] if len(params) > 1 else False
                result = await self.server.rpc.get_transaction(tx_hash, verbose)
            elif method == 'blockchain.transaction.broadcast':
                tx_hex = params[0]
                result = await self.server.rpc.send_raw_transaction(tx_hex)
            else:
                raise Exception(f"Method not found: {method}")

        except Exception as e:
            error = {"code": -32603, "message": str(e)}
            print(f"[bold red]Error processing {method}:[/bold red] {e}")
            traceback.print_exc()
        
        if error:
            response = {"jsonrpc": "2.0", "error": error, "id": req_id}
        else:
            response = {"jsonrpc": "2.0", "result": result, "id": req_id}
            
        print(f"[green]Response to {method}:[/green] {json.dumps(response)[:200]}...") 
        return response

class ElectrumServer:
    def __init__(self):
        self.rpc = BitcoinRPC()
        self.indexer = Indexer(self.rpc)
        self.sessions = set()
        self.header_subs = set()
        self.tasks = set()
        
    def add_session(self, session):
        self.sessions.add(session)

    def remove_session(self, session):
        self.sessions.discard(session)

    def add_header_session(self, session):
        self.header_subs.add(session)

    def remove_header_session(self, session):
        self.header_subs.discard(session)

    async def notify_subscribers(self, touched_scripthashes):
        if not touched_scripthashes:
            return

        print(f"[bold cyan]Notifying subscribers for {len(touched_scripthashes)} touched scripthashes...[/bold cyan]")
        
        # Convert bytes to hex strings for matching with subscriptions
        touched_hex = {s.hex() for s in touched_scripthashes}

        for session in list(self.sessions):
            try:
                # Find overlapping interests
                matches = session.subscriptions.intersection(touched_hex)
                for sh in matches:
                    # Calculate new status
                    history = self.indexer.get_history(sh)
                    if not history:
                        status = None
                    else:
                        history.sort(key=lambda x: (x['height'], x['tx_hash']))
                        status_str = ""
                        for item in history:
                            status_str += f"{item['tx_hash']}:{item['height']}:"
                        status = hashlib.sha256(status_str.encode()).hexdigest()
                    
                    await session.notify(sh, status)
            except Exception as e:
                print(f"Error notifying session: {e}")

    async def notify_new_block(self):
        if not self.header_subs:
            return
            
        try:
            height = self.indexer.height
            if height < 0: return

            block_hash = await self.rpc.get_block_hash(height)
            block_header = await self.rpc.call("getblockheader", [block_hash, False])
            result = {"hex": block_header, "height": height}
            
            print(f"[bold cyan]Notifying {len(self.header_subs)} header subscribers of new block {height}...[/bold cyan]")
            
            for session in list(self.header_subs):
                await session.notify_header(result)
        except Exception as e:
            print(f"Error in notify_new_block: {e}")

    async def start(self):
        sync_task = asyncio.create_task(self.run_sync())
        monitor_task = asyncio.create_task(self.monitor_status())
        cleanup_task = asyncio.create_task(self.run_cleanup())
        
        self.tasks.add(sync_task)
        self.tasks.add(monitor_task)
        self.tasks.add(cleanup_task)
        
        sync_task.add_done_callback(self.tasks.discard)
        monitor_task.add_done_callback(self.tasks.discard)
        cleanup_task.add_done_callback(self.tasks.discard)

        server = await asyncio.start_server(
            self.handle_client, HOST, PORT
        )
        print(f"[bold green]Serving on {HOST}:{PORT}[/bold green]")
        async with server:
            await server.serve_forever()

    async def monitor_status(self):
        while True:
            await asyncio.sleep(30)
            rpc_stats = self.rpc.get_stats()
            stats = (
                f"[dim]Status Report: "
                f"Sessions={len(self.sessions)} | "
                f"HeaderSubs={len(self.header_subs)} | "
                f"RPC Active={rpc_stats['active_calls']} | "
                f"RPC Total={rpc_stats['total_calls']}[/dim]"
            )
            print(stats)

    async def run_cleanup(self):
        while True:
            try:
                # Run cleanup on startup and then every hour
                print("[dim]Running cache cleanup...[/dim]")
                await self.rpc.cache.cleanup()
            except Exception as e:
                print(f"Cache cleanup error: {e}")
            await asyncio.sleep(3600)

    async def handle_client(self, reader, writer):
        session = ElectrumSession(reader, writer, self)
        await session.handle()

    async def run_sync(self):
        while True:
            try:
                async for touched in self.indexer.sync():
                    if touched:
                        await self.notify_subscribers(touched)
                    await self.notify_new_block()
            except Exception as e:
                print(f"[bold red]Sync error:[/bold red] {e}")
                # Prevent tight loop on error
                await asyncio.sleep(5)
            await asyncio.sleep(10)
