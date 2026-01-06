import asyncio
import sys
import argparse
from rich.console import Console
from rich.traceback import install
from src.server import ElectrumServer
from src.indexer import Indexer
from src.rpc import BitcoinRPC
from src.config import RPC_URL

install()
console = Console()

async def run_compaction():
    console.print("[bold green]Starting Compaction...[/bold green]")
    rpc = BitcoinRPC()
    indexer = Indexer(rpc)
    # The init_db called in __init__ will handle migration if needed
    # But we want to run compaction explicitly
    indexer.compact_addr_index()
    console.print("[bold green]Done.[/bold green]")

async def main():
    parser = argparse.ArgumentParser(description="CoreIndex Electrum Server")
    parser.add_argument("--compact", action="store_true", help="Compact the address index and exit")
    args = parser.parse_args()

    if args.compact:
        await run_compaction()
        return

    console.print("[bold green]Starting CoreIndex...[/bold green]")
    console.print(f"Connecting to Bitcoin Core at [cyan]{RPC_URL}[/cyan]")
    
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
