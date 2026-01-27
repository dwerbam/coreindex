import asyncio
import json
import secrets
import time
import datetime
import coincurve
import argparse
from typing import Optional, Tuple
from rich.console import Console
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.live import Live
from rich.align import Align

# Configuration
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 50001
NETWORK_TYPE = "mainnet" 

console = Console()

class BandwidthSimulator:
    def __init__(self, rate_bps: float = 0):
        self.rate_bps = rate_bps  # 0 means unlimited
        self.bytes_read = 0
    
    async def simulate_latency(self, num_bytes: int):
        if self.rate_bps <= 0:
            return
        
        bits = num_bytes * 8
        delay = bits / self.rate_bps
        if delay > 0.01: # Only sleep for meaningful delays
            await asyncio.sleep(delay)

class SilentPaymentClient:
    def __init__(self):
        self.set_random_keys()
        self.reader = None
        self.writer = None
        self.bandwidth = BandwidthSimulator(0) # Unlimited
        self.connected = False
        self.chain_tip = 0

    def set_random_keys(self):
        self.scan_priv = secrets.token_bytes(32)
        self.update_pubkeys()

    def set_scan_key(self, scan_priv_hex: str):
        try:
            priv = bytes.fromhex(scan_priv_hex)
            if len(priv) != 32:
                return False, "Key must be 32 bytes (64 hex characters)."
            self.scan_priv = priv
            self.update_pubkeys()
            return True, "Scan Key updated successfully."
        except Exception as e:
            return False, f"Invalid key: {e}"

    def update_pubkeys(self):
        self.scan_pub = coincurve.PublicKey.from_secret(self.scan_priv).format(compressed=True)
        # For simulation, we just regenerate spend key or keep it random since we don't use it for scanning 
        # (only derivation of shared secret check technically needs it if we were doing full verification)
        self.spend_priv = secrets.token_bytes(32) 
        self.spend_pub = coincurve.PublicKey.from_secret(self.spend_priv).format(compressed=True)

    def set_bandwidth(self, name: str):
        rates = {
            "Unlimited": 0,
            "4G (Average)": 20_000_000, # 20 Mbps
            "3G (Good)": 3_000_000,     # 3 Mbps
            "3G (Poor)": 700_000,       # 700 Kbps
            "2G (Edge)": 150_000,       # 150 Kbps
            "Satellite": 50_000,        # 50 Kbps
        }
        self.bandwidth.rate_bps = rates.get(name, 0)
        return self.bandwidth.rate_bps

    async def connect(self, host: str, port: int):
        self.host = host
        self.port = port
        try:
            with console.status(f"[bold green]Connecting to {host}:{port}..."):
                # Increase buffer limit to 2GB to handle large JSON responses (many tweaks)
                self.reader, self.writer = await asyncio.open_connection(host, port, limit=2*1024*1024*1024)
                self.connected = True
                
                # Get initial banner and version
                console.print("[dim]Requesting banner...[/dim]")
                await self.request("server.banner", timeout=5)
                
                console.print("[dim]Requesting version...[/dim]")
                await self.request("server.version", ["ClientPOC", "1.0"], timeout=5)
                
                # Subscribe to headers to get tip
                console.print("[dim]Subscribing to headers...[/dim]")
                res = await self.request("blockchain.headers.subscribe", timeout=10)
                if res and 'result' in res:
                    self.chain_tip = res['result'].get('height', 0)
                    
            console.print(f"[bold green]Connected! Chain Tip: {self.chain_tip}[/bold green]")
            return True
        except Exception as e:
            console.print(f"[bold red]Connection failed: {e}[/bold red]")
            return False

    async def reconnect(self):
        console.print("[yellow]Reconnecting...[/yellow]")
        await self.close()
        while True:
            if await self.connect(self.host, self.port):
                return
            await asyncio.sleep(5)

    async def request(self, method, params=None, timeout=30):
        if not self.writer: return None
        if params is None: params = []
        
        req_id = secrets.randbits(16)
        req = {"jsonrpc": "2.0", "method": method, "params": params, "id": req_id}
        payload = (json.dumps(req) + '\n').encode('utf-8')
        
        try:
            self.writer.write(payload)
            await self.writer.drain()
            
            # Simulate upload bandwidth
            await self.bandwidth.simulate_latency(len(payload))

            while True:
                data = await asyncio.wait_for(self.reader.readline(), timeout=timeout)
                if not data: raise ConnectionError("Connection closed")
                
                # Simulate download bandwidth
                await self.bandwidth.simulate_latency(len(data))
                
                try:
                    response = json.loads(data)
                except json.JSONDecodeError:
                    continue

                # Check if this is the response to our request
                if response.get('id') == req_id:
                    return response
                
                # Ignore notifications or other messages
                
        except Exception as e:
            # console.print(f"[bold red]Request Exception: {e}[/bold red]")
            raise e

    async def find_height_by_date(self, target_date_str: str) -> int:
        """Binary search for the block height closest to the given date (YYYY-MM-DD)."""
        try:
            target_ts = time.mktime(datetime.datetime.strptime(target_date_str, "%Y-%m-%d").timetuple())
        except ValueError:
            console.print("[red]Invalid date format. Use YYYY-MM-DD.[/red]")
            return -1

        low = 0
        high = self.chain_tip
        closest_height = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            transient=True
        ) as progress:
            task = progress.add_task("🔍 Searching blockchain for date...", total=100) # Pseudo-total
            
            # Optimization: Check checks points if possible, but pure binary search is robust
            while low <= high:
                mid = (low + high) // 2
                try:
                    res = await self.request("blockchain.block.header", [mid])
                    if not res or 'result' not in res:
                        break # Logic break, not net error
                except Exception:
                    await self.reconnect()
                    continue

                header_hex = res['result']
                # Block header is 80 bytes. Timestamp is at offset 68 (4 bytes, little endian)
                # Hex string: 2 chars per byte. Offset 68 * 2 = 136. Length 8 chars.
                if isinstance(header_hex, str) and len(header_hex) >= 160:
                    ts_hex = header_hex[136:144]
                    block_ts = int.from_bytes(bytes.fromhex(ts_hex), 'little')
                elif isinstance(header_hex, dict):
                    # In case server returns dict (some impls do)
                    block_ts = header_hex.get('time', 0)
                else:
                    block_ts = 0

                if block_ts < target_ts:
                    closest_height = mid
                    low = mid + 1
                else:
                    high = mid - 1
                
                # Visual update
                progress.update(task, advance=5) # fake advance

        return closest_height

    async def scan_blocks(self, start_height: int, end_height: int, batch_size: int = 1000):
        count = end_height - start_height + 1
        console.print(f"[bold cyan]Requesting tweaks for {count} blocks ({start_height} to {end_height}) in batches of {batch_size}...[/bold cyan]")
        
        start_time = time.time()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total} Blocks"),
            TextColumn("• [bold green]{task.fields[tweak_rate]}[/bold green]"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task_dl = progress.add_task("Downloading Tweaks...", total=count, tweak_rate="0 Tweaks/s")
            
            processed = 0
            found_secrets = []
            total_tweaks = 0

            for batch_start in range(start_height, end_height + 1, batch_size):
                # Calculate actual batch size (don't go past end_height)
                current_batch_size = min(batch_size, end_height - batch_start + 1)
                
                while True: # Retry loop
                    try:
                        # RPC Call
                        response = await self.request("blockchain.silentscanning.get_tweaks", [batch_start, current_batch_size], timeout=60)
                        
                        if not response or 'result' not in response:
                            if response and 'error' in response:
                                console.print(f"[bold red]RPC Error:[/bold red] {response['error']}")
                                break # Logic error, don't retry? Or maybe retry on temporary server error?
                            else:
                                # Network glitch or empty response
                                raise ConnectionError("Empty or invalid response")
                        
                        tweaks = response['result']
                        num_tweaks = len(tweaks) if tweaks else 0
                        total_tweaks += num_tweaks
                        
                        # Calculate rate
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            rate = total_tweaks / elapsed
                            rate_str = f"{rate:,.0f} Tweaks/s"
                        else:
                            rate_str = "? Tweaks/s"

                        progress.update(task_dl, advance=current_batch_size, tweak_rate=rate_str)
                        
                        # Process Logic
                        if tweaks:
                            for item in tweaks:
                                tweak_data = bytes.fromhex(item['tweak'])
                                try:
                                    input_sum_point = coincurve.PublicKey(tweak_data)
                                    shared_point = input_sum_point.multiply(self.scan_priv)
                                    found_secrets.append((item['height'], item['tx_hash'], shared_point.format(compressed=True).hex()))
                                except:
                                    pass
                        
                        break # Success, move to next batch

                    except Exception as e:
                        # console.print(f"[red]Error fetching batch: {e}. Retrying...[/red]")
                        await self.reconnect()
                        # Retry loop continues

        duration = time.time() - start_time
        avg_speed = total_tweaks / duration if duration > 0 else 0
        
        # Results
        console.print(Panel(f"Scan Complete in {duration:.2f}s\nTotal Tweaks: {total_tweaks:,}\nAvg Speed: {avg_speed:,.0f} Tweaks/s", title="Results", style="green"))
        if found_secrets:
            table = Table(title="Found Shared Secrets (POC)")
            table.add_column("Height", style="cyan")
            table.add_column("Tx Hash", style="magenta")
            table.add_column("Shared Secret (Derived)", style="green")
            
            for h, tx, secret in found_secrets[:10]: # Show top 10
                table.add_row(str(h), tx[:12]+"...", secret[:16]+"...")
            
            if len(found_secrets) > 10:
                table.add_row("...", "...", f"... and {len(found_secrets)-10} more")
                
            console.print(table)
        else:
            console.print("[yellow]No relevant tweaks/payments found in this range.[/yellow]")

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()


async def main():
    global DEFAULT_PORT, NETWORK_TYPE, DEFAULT_HOST

    parser = argparse.ArgumentParser(description="CoreIndex Silent Payments Client POC")
    parser.add_argument("--testnet", action="store_true", help="Use Testnet defaults (Port 51001)")
    parser.add_argument("--host", type=str, help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, help="Server port")
    args = parser.parse_args()

    if args.testnet:
        DEFAULT_PORT = 51001
        NETWORK_TYPE = "testnet"
    
    if args.host:
        DEFAULT_HOST = args.host
        
    if args.port:
        DEFAULT_PORT = args.port

    client = SilentPaymentClient()
    
    # Header
    console.print(Panel.fit(
        f"[bold magenta]CoreIndex Silent Payments Client (POC)[/bold magenta]\n"
        f"Network: [cyan]{NETWORK_TYPE}[/cyan] | Default Port: [cyan]{DEFAULT_PORT}[/cyan]\n"
        "Simulates a mobile wallet scanning for private payments.",
        border_style="magenta"
    ))

    # Connection Loop
    while not client.connected:
        host = Prompt.ask("Server Host", default=DEFAULT_HOST)
        port = IntPrompt.ask("Server Port", default=DEFAULT_PORT)
        await client.connect(host, port)
        if not client.connected:
            if not Confirm.ask("Retry connection?"):
                return

    # Main Menu
    while True:
        console.print(Panel(
            f"[bold]Current Wallet State:[/bold]\n"
            f"Scan PubKey: [cyan]{client.scan_pub.hex()}[/cyan]\n"
            f"(Use option 4 to import your own Scan Private Key)",
            title="Wallet Info",
            style="blue"
        ))

        console.print("\n[bold]Menu:[/bold]")
        console.print("1. [cyan]Start Scan (Block Height)[/cyan]")
        console.print("2. [cyan]Start Scan (Date)[/cyan]")
        console.print("3. [cyan]Set Network Speed Simulation[/cyan]")
        console.print("4. [yellow]Configure Wallet (Scan Key)[/yellow]")
        console.print("5. [red]Exit[/red]")
        
        choice = Prompt.ask("Select Option", choices=["1", "2", "3", "4", "5"], default="1")
        
        if choice == "1":
            start = IntPrompt.ask("Start Height", default=client.chain_tip - 1000 if client.chain_tip > 1000 else 0)
            end = IntPrompt.ask("End Height", default=client.chain_tip)
            batch = IntPrompt.ask("Batch Size", default=1000)
            
            if end < start:
                console.print("[red]End height must be >= Start height[/red]")
            else:
                await client.scan_blocks(start, end, batch)
            
        elif choice == "2":
            date_str = Prompt.ask("Start Date (YYYY-MM-DD)", default=datetime.date.today().strftime("%Y-%m-%d"))
            height = await client.find_height_by_date(date_str)
            if height >= 0:
                console.print(f"[green]Date {date_str} maps to approx Height {height}[/green]")
                end = IntPrompt.ask("End Height", default=client.chain_tip)
                batch = IntPrompt.ask("Batch Size", default=1000)
                await client.scan_blocks(height, end, batch)
                
        elif choice == "3":
            options = ["Unlimited", "4G (Average)", "3G (Good)", "3G (Poor)", "2G (Edge)", "Satellite"]
            speed_menu = "\n".join([f"{i+1}. {opt}" for i, opt in enumerate(options)])
            console.print(Panel(speed_menu, title="Select Bandwidth Profile"))
            
            sel = IntPrompt.ask("Choice", default=1)
            if 1 <= sel <= len(options):
                name = options[sel-1]
                rate = client.set_bandwidth(name)
                console.print(f"[green]Bandwidth set to: {name} ({rate if rate > 0 else '∞'} bps)[/green]")

        elif choice == "4":
             key = Prompt.ask("Enter Hex Scan Private Key (leave empty for random)", password=True)
             if key.strip():
                 success, msg = client.set_scan_key(key.strip())
                 if success:
                     console.print(f"[green]{msg}[/green]")
                 else:
                     console.print(f"[red]{msg}[/red]")
             else:
                 client.set_random_keys()
                 console.print("[yellow]Generated new random keys.[/yellow]")
        
        elif choice == "5":
            await client.close()
            console.print("[bold]Goodbye![/bold]")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")