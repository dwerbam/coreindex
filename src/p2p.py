import asyncio
import socket
import struct
import time
import random
import io
import math
from rich import print
from bitcoin.core import CBlock
from bitcoin.messages import msg_version, msg_verack, msg_getdata, msg_pong, CInv
from bitcoin.net import CAddress

MAGIC_MAINNET = b'\xf9\xbe\xb4\xd9'

class P2PClient:
    def __init__(self, ip, port=8333):
        self.ip = ip
        self.port = port
        self.reader = None
        self.writer = None
        self.handshake_complete = False
        self.last_activity = 0
        self.busy = False

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            
            # Send Version (Modern Handshake)
            # 70015 = Bitcoin Core 0.13.2+ (Compact Blocks, etc)
            # Services = 1 (Network) + 8 (Witness) = 9
            version_msg = msg_version()
            version_msg.nVersion = 70015
            version_msg.nServices = 9
            version_msg.nTime = int(time.time())
            version_msg.addrTo.ip = self.ip
            version_msg.addrTo.port = self.port
            version_msg.addrTo.nServices = 9
            version_msg.addrFrom.ip = "127.0.0.1"
            version_msg.addrFrom.port = 8333
            version_msg.addrFrom.nServices = 9
            version_msg.nNonce = random.getrandbits(64)
            version_msg.strSubVer = b"/Satoshi:22.0.0/" # Mimic a standard node
            version_msg.nStartingHeight = 0
            
            await self.send_message(version_msg)
            
            # Handshake loop
            while not self.handshake_complete:
                cmd, _ = await self.read_message()
                if cmd == b'version':
                    await self.send_message(msg_verack())
                elif cmd == b'verack':
                    self.handshake_complete = True
            
            self.last_activity = time.time()
            # print(f"[dim]Connected to {self.ip}[/dim]")
            
        except Exception as e:
            self.close()
            raise e

    def close(self):
        if self.writer:
            try:
                self.writer.close()
            except:
                pass
        self.writer = None
        self.reader = None
        self.handshake_complete = False

    async def send_message(self, message):
        if not self.writer: raise Exception("Not connected")
        # message.to_bytes() returns the full packet (Magic + Header + Payload)
        # because python-bitcoinlib handles serialization fully.
        packet = message.to_bytes()
        self.writer.write(packet)
        await self.writer.drain()

    async def read_message(self):
        if not self.reader: raise Exception("Not connected")
        header_data = await self.reader.readexactly(24)
        magic, command, length, checksum = struct.unpack('<4s12sI4s', header_data)
        
        if magic != MAGIC_MAINNET:
            raise Exception("Invalid Magic")
            
        command = command.strip(b'\x00')
        payload = await self.reader.readexactly(length)
        self.last_activity = time.time()
        return command, payload

    async def fetch_blocks(self, hashes):
        """Fetches a specific list of blocks from this peer."""
        if not self.handshake_complete:
            await self.connect()
            
        self.busy = True
        try:
            print(f"[dim]Requesting {len(hashes)} blocks from {self.ip}...[/dim]")
            invs = []
            for h in hashes:
                inv = CInv()
                inv.type = 2
                inv.hash = bytes.fromhex(h)[::-1]
                invs.append(inv)
            
            await self.send_message(msg_getdata(invs))
            
            blocks = []
            pending = len(hashes)
            
            # Simple timeout for this batch
            start_wait = time.time()
            while pending > 0:
                if time.time() - start_wait > 30: # 30s timeout per batch
                    raise Exception("Timeout waiting for blocks")

                try:
                    cmd, payload = await asyncio.wait_for(self.read_message(), timeout=5.0)
                except asyncio.TimeoutError:
                    # print(f"[dim]Timeout reading from {self.ip} (pending {pending})[/dim]")
                    continue # Keep checking total timeout

                if cmd == b'block':
                    # print(f"[dim]Received block from {self.ip}[/dim]")
                    f = io.BytesIO(payload)
                    cblock = CBlock.stream_deserialize(f)
                    blocks.append(self._cblock_to_dict(cblock))
                    pending -= 1
                elif cmd == b'ping':
                    # Reply with pong (payload is nonce)
                    # Payload is 8 bytes nonce
                    if len(payload) == 8:
                        nonce = struct.unpack('<Q', payload)[0]
                        await self.send_message(msg_pong(nonce))
                elif cmd == b'inv':
                    pass
                elif cmd == b'reject':
                    print(f"[red]Peer {self.ip} rejected request[/red]")
            
            return blocks
        finally:
            self.busy = False

    def _cblock_to_dict(self, cblock):
        # ... (Same parsing logic as before, omitting for brevity in thought trace but including in file) ...
        block_hash = cblock.GetHash()[::-1].hex()
        
        txs = []
        for tx in cblock.vtx:
            txid = tx.GetHash()[::-1].hex()
            vins = []
            for vin in tx.vin:
                is_coinbase = vin.prevout.is_null()
                if is_coinbase:
                    vins.append({"coinbase": "00"})
                else:
                    vins.append({
                        "txid": vin.prevout.hash[::-1].hex(),
                        "vout": vin.prevout.n
                    })
            vouts = []
            for vout in tx.vout:
                vouts.append({
                    "value": vout.nValue / 100_000_000.0,
                    "scriptPubKey": {"hex": vout.scriptPubKey.hex()}
                })
            txs.append({"txid": txid, "vin": vins, "vout": vouts})
            
        return {"hash": block_hash, "tx": txs, "time": cblock.nTime}


class P2PManager:
    def __init__(self, max_peers=8):
        self.seeds = ['seed.bitcoin.sipa.be', 'dnsseed.bluematt.me', 'dnsseed.bitcoin.dashjr.org']
        self.max_peers = max_peers
        self.peers = [] # List of P2PClient
        self.peer_ips = set()

    async def discover_peers(self):
        print("[cyan]Discovering P2P peers...[/cyan]")
        ips = []
        for seed in self.seeds:
            try:
                infos = await asyncio.get_event_loop().getaddrinfo(seed, 8333, proto=socket.IPPROTO_TCP)
                ips.extend([i[4][0] for i in infos])
            except:
                pass
        
        random.shuffle(ips)
        return list(set(ips)) # Dedup

    async def start(self):
        potential_peers = await self.discover_peers()
        print(f"[cyan]Found {len(potential_peers)} potential peers. Connecting...[/cyan]")
        
        async def try_connect(ip):
            client = P2PClient(ip)
            try:
                await asyncio.wait_for(client.connect(), timeout=3.0)
                return client
            except:
                return None

        # Attempt to connect to a batch of peers in parallel
        # We try up to 30 peers to find our max_peers (10)
        tasks = [try_connect(ip) for ip in potential_peers[:30]]
        results = await asyncio.gather(*tasks)
        
        for client in results:
            if client and len(self.peers) < self.max_peers:
                self.peers.append(client)
                self.peer_ips.add(client.ip)
            elif client:
                client.close()
        
        print(f"[green]Connected to {len(self.peers)} P2P peers.[/green]")

    async def get_blocks(self, hashes):
        """Distributes block requests across connected peers with retry."""
        if not hashes:
            return []

        if not self.peers:
            await self.start()
            if not self.peers:
                raise Exception("No P2P peers available")

        # Distribute hashes
        num_chunks = len(self.peers)
        chunk_size = math.ceil(len(hashes) / num_chunks)
        
        # Map peer_index -> chunk
        assignments = {}
        tasks = []
        
        for i, peer in enumerate(self.peers):
            start = i * chunk_size
            end = start + chunk_size
            chunk = hashes[start:end]
            if not chunk:
                continue
            assignments[i] = chunk
            tasks.append(peer.fetch_blocks(chunk))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_blocks = []
        failed_hashes = []
        dead_peers = []
        
        for i, res in enumerate(results):
            if isinstance(res, list):
                all_blocks.extend(res)
            else:
                # Peer failed
                peer = self.peers[i]
                # print(f"[yellow]Peer {peer.ip} failed: {res}[/yellow]")
                dead_peers.append(peer)
                failed_hashes.extend(assignments[i])
        
        # Remove dead peers
        for p in dead_peers:
            p.close()
            if p in self.peers:
                self.peers.remove(p)
                self.peer_ips.discard(p.ip)
                
        # Retry failed
        if failed_hashes:
            # print(f"[yellow]Retrying {len(failed_hashes)} blocks...[/yellow]")
            if not self.peers:
                # Try to reconnect if everyone died
                await self.start()
                if not self.peers:
                     raise Exception("All P2P peers failed and could not reconnect")
            
            # Recursive retry
            # (Note: In production, add depth limit)
            retried_blocks = await self.get_blocks(failed_hashes)
            all_blocks.extend(retried_blocks)
                
        return all_blocks