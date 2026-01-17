import asyncio
import sys
import argparse
import signal
from rich.console import Console
from rich.traceback import install
from src.server import ElectrumServer
from src.indexer import Indexer
from src.rpc import BitcoinRPC
from src.config import RPC_URLS

install()
console = Console()

def handle_signal(signum, frame):
    console.print(f"\n[bold red]Received signal {signum}, shutting down...[/bold red]")
    raise KeyboardInterrupt

async def run_compaction():
    console.print("[bold green]Starting Compaction...[/bold green]")
    rpc = BitcoinRPC()
    indexer = Indexer(rpc)
    # The init_db called in __init__ will handle migration if needed
    # But we want to run compaction explicitly
    indexer.compact_addr_index()
    console.print("[bold green]Done.[/bold green]")

async def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    parser = argparse.ArgumentParser(description="CoreIndex Electrum Server")
    parser.add_argument("--compact", action="store_true", help="Compact the address index and exit")
    args = parser.parse_args()

    if args.compact:
        await run_compaction()
        return

    console.print("[bold green]Starting CoreIndex...[/bold green]")
    console.print(f"Connecting to {len(RPC_URLS)} Bitcoin Core nodes: [cyan]{', '.join(RPC_URLS)}[/cyan]")
    
    server = ElectrumServer()
    try:
        await server.start()
    except KeyboardInterrupt:
        console.print("[bold red]Shutting down...[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Fatal error:[/bold red] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
