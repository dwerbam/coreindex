import asyncio
from src.rpc import BitcoinRPC
from src.indexer import Indexer

async def main():
    print("Checking status...")
    rpc = BitcoinRPC()
    try:
        # Get Core Height
        core_height = await rpc.get_block_count()
        
        # Get Indexer Height
        # We initialize indexer (which reads DB files)
        indexer = Indexer(rpc)
        index_height = indexer.height
        
        print(f"\n[bold]Status Report[/bold]")
        print(f"Bitcoin Core Height: [cyan]{core_height}[/cyan]")
        print(f"CoreIndex Height:    [green]{index_height}[/green]")
        
        diff = core_height - index_height
        if diff == 0:
            print("\n[bold green]✅ Fully Synced[/bold green]")
        else:
            print(f"\n[bold yellow]⚠️  Syncing... {diff} blocks behind[/bold yellow]")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await rpc.close()

if __name__ == "__main__":
    from rich import print
    asyncio.run(main())

