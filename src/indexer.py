import polars as pl
import hashlib
import asyncio
import time
import datetime
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import print
from src.config import DATA_DIR
from src.rpc import BitcoinRPC

class ETATimeRemainingColumn(TimeRemainingColumn):
    def render(self, task) -> str:
        base = super().render(task)
        return f"ETA {base}"

def get_scripthash(script_hex: str) -> str:
    """Calculate Electrum scripthash (SHA256 of scriptPubKey, little-endian)."""
    if not script_hex:
        return None
    return hashlib.sha256(bytes.fromhex(script_hex)).digest()[::-1].hex()

class Indexer:
    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc
        self.headers_path = DATA_DIR / "headers.parquet"
        self.history_path = DATA_DIR / "history.parquet"
        self.utxo_path = DATA_DIR / "utxo.parquet"
        self.addr_index_path = DATA_DIR / "addr_index"
        
        # Batch buffers
        self.batch_headers = []
        self.batch_history = []
        self.batch_new_utxos = []
        self.batch_spent_outpoints = [] # (prev_hash, prev_index, spending_txid, height)
        
        self.init_db()

    def init_db(self):
        # Headers: height, hash, hex (Partitioned storage, just schema here)
        self.headers_schema = {"height": pl.UInt32, "hash": pl.String, "hex": pl.String}
        
        # History: scripthash, tx_hash, height (Partitioned storage, just schema here)
        self.history_schema = {"scripthash": pl.String, "tx_hash": pl.String, "height": pl.Int32}

        # UTXOs: scripthash, tx_hash, tx_pos, value, height (Kept in memory)
        if self.utxo_path.exists():
            self.utxo = pl.read_parquet(self.utxo_path)
        else:
            self.utxo = pl.DataFrame({
                "scripthash": [], "tx_hash": [], "tx_pos": [], "value": [], "height": []
            }, schema={"scripthash": pl.String, "tx_hash": pl.String, "tx_pos": pl.UInt32, "value": pl.UInt64, "height": pl.Int32})

        # Migration: Rename monolithic files to partitioned format if they exist
        current_max_h = self.utxo["height"].max() if not self.utxo.is_empty() else 0
        
        legacy_headers = DATA_DIR / "headers.parquet"
        if legacy_headers.exists():
            new_name = DATA_DIR / f"headers_0_{current_max_h}.parquet"
            print(f"[bold yellow]Migrating {legacy_headers} to {new_name}...[/bold yellow]")
            legacy_headers.rename(new_name)

        legacy_history = DATA_DIR / "history.parquet"
        if legacy_history.exists():
            new_name = DATA_DIR / f"history_0_{current_max_h}.parquet"
            print(f"[bold yellow]Migrating {legacy_history} to {new_name}...[/bold yellow]")
            legacy_history.rename(new_name)

        # Migration: Populate address index if missing but history exists
        self.migrate_to_addr_index()

    def migrate_to_addr_index(self):
        # We don't return early anymore. We check for legacy files and process/delete them.
        history_files = list(DATA_DIR.glob("history_*.parquet"))
        if not history_files:
            return

        print(f"[bold yellow]Optimizing storage: Migrating {len(history_files)} history files to address index...[/bold yellow]")
        self.addr_index_path.mkdir(parents=True, exist_ok=True)
        
        for f in history_files:
            try:
                # Parse range from filename: history_START_END.parquet
                parts = f.stem.split('_')
                if len(parts) >= 3:
                    h_min, h_max = parts[1], parts[2]
                    
                    df = pl.read_parquet(f)
                    if not df.is_empty():
                        # Add prefix
                        df = df.with_columns(pl.col("scripthash").str.slice(0, 2).alias("sh_prefix"))
                        
                        # Write partitioned
                        for (prefix,), data in df.partition_by("sh_prefix", as_dict=True).items():
                            path = self.addr_index_path / f"sh_prefix={prefix}"
                            path.mkdir(parents=True, exist_ok=True)
                            data.drop("sh_prefix").write_parquet(path / f"history_{h_min}_{h_max}.parquet", compression="zstd")
                
                # Delete the legacy file after successful processing (or if empty)
                f.unlink()
                print(f"[dim]Migrated and deleted {f.name}[/dim]")
                
            except Exception as e:
                print(f"[red]Failed to migrate {f}: {e}[/red]")

    def compact_addr_index(self):
        """Merges small history files in the address index into larger ones."""
        if not self.addr_index_path.exists():
            print("[yellow]Address index path does not exist, nothing to compact.[/yellow]")
            return

        prefixes = list(self.addr_index_path.glob("sh_prefix=*"))
        if not prefixes:
            print("[yellow]No prefixes found in address index.[/yellow]")
            return

        print(f"[bold cyan]Compacting {len(prefixes)} address index partitions...[/bold cyan]")
        
        total_files_removed = 0
        total_space_saved = 0
        
        for p in prefixes:
            files = list(p.glob("*.parquet"))
            # Only compact if we have multiple files
            if len(files) <= 1:
                continue
                
            try:
                # Calculate original size
                orig_size = sum(f.stat().st_size for f in files)
                
                # Load all data
                df = pl.scan_parquet(p / "*.parquet").collect()
                
                if df.is_empty():
                    for f in files:
                        f.unlink()
                    continue

                # Define new filename
                # Find min/max height for the filename
                h_min = df["height"].min()
                h_max = df["height"].max()
                timestamp = int(time.time())
                new_file = p / f"history_{h_min}_{h_max}_compacted_{timestamp}.parquet"
                
                # Write compacted file with compression
                df.write_parquet(new_file, compression="zstd")
                
                # Verify new file exists
                if new_file.exists() and new_file.stat().st_size > 0:
                    new_size = new_file.stat().st_size
                    
                    # Delete old files
                    for f in files:
                        if f != new_file:
                            f.unlink()
                            
                    total_files_removed += len(files) - 1
                    total_space_saved += max(0, orig_size - new_size)
                else:
                    print(f"[red]Failed to verify compacted file for {p.name}, aborting cleanup.[/red]")
                    
            except Exception as e:
                print(f"[red]Error compacting {p.name}: {e}[/red]")
        
        print(f"[bold green]Compaction complete![/bold green]")
        print(f"Merged files count reduction: {total_files_removed}")
        print(f"Estimated space saved: {total_space_saved / 1024 / 1024:.2f} MB")

    def save_db(self):
        touched = self.flush_batch()
        self.utxo.write_parquet(self.utxo_path)
        return touched

    @property
    def height(self) -> int:
        if not self.utxo.is_empty():
            return self.utxo["height"].max()
        
        # Fallback to checking files
        files = list(DATA_DIR.glob("headers_*_*.parquet"))
        if not files:
            return -1
        
        max_h = -1
        for f in files:
            try:
                parts = f.stem.split('_')
                end = int(parts[2])
                if end > max_h:
                    max_h = end
            except:
                pass
        return max_h

    async def sync(self):
        core_height = await self.rpc.get_block_count()
        current_height = self.height

        print(f"Index height: {current_height}, Core height: {core_height}")

        fetch_concurrency = 50

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[bold yellow]{task.fields[status]}"),
            TextColumn("[dim cyan]{task.fields[block_date]}"),
            ETATimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Syncing...", total=core_height, completed=max(0, current_height), status="Starting", block_date="--/--/--")
            
            latencies = []
            while current_height < core_height:
                start_height = current_height + 1
                end_height = min(core_height, current_height + fetch_concurrency)
                count = end_height - start_height + 1
                
                # Update status to fetching
                progress.update(task, status=f"⬇️ Fetching {count}")
                
                start_time = time.time()
                
                # 1. Fetch Hashes in parallel
                heights = list(range(start_height, end_height + 1))
                hashes = await asyncio.gather(*[self.rpc.get_block_hash(h) for h in heights])
                
                # 2. Fetch Blocks in parallel
                blocks = await asyncio.gather(*[self.rpc.get_block(h, verbosity=2) for h in hashes])
                
                # 3. Process Sequentially
                for i, block in enumerate(blocks):
                    h = heights[i]
                    
                    # Extract block date
                    try:
                        b_time = block.get("time", 0)
                        date_str = datetime.datetime.fromtimestamp(b_time).strftime('%Y-%m-%d')
                    except:
                        date_str = "??"

                    # Update status to indexing with date
                    progress.update(task, completed=h, status="⚙️ Indexing", block_date=date_str)
                    
                    self.process_block(block, h)
                    
                    if h % 2000 == 0 or h == core_height:
                        touched = self.save_db()
                        if touched:
                            yield touched
                    
                batch_time = time.time() - start_time
                avg_latency = batch_time / count if count > 0 else 0
                latencies.extend([avg_latency] * count)
                while len(latencies) > 200: # Keep more for stability with batching
                    latencies.pop(0)
                
                current_height = end_height
            
        touched = self.save_db() # Final save
        if touched:
            yield touched

    def process_block(self, block: dict, height: int):
        # 1. Update Headers
        self.batch_headers.append({
            "height": height,
            "hash": block["hash"],
            "hex": "" # TODO: Construct or fetch header hex
        })

        for tx in block["tx"]:
            txid = tx["txid"]
            
            # Inputs (Spends)
            for vin in tx["vin"]:
                if "coinbase" in vin:
                    continue
                self.batch_spent_outpoints.append({
                    "tx_hash": vin["txid"], 
                    "tx_pos": vin["vout"], 
                    "spending_txid": txid,
                    "height": height
                })
                
            # Outputs (New UTXOs)
            for i, vout in enumerate(tx["vout"]):
                script_hex = vout["scriptPubKey"]["hex"]
                sh = get_scripthash(script_hex)
                if sh:
                    val = int(vout["value"] * 100_000_000) # Satoshis
                    
                    self.batch_new_utxos.append({
                        "scripthash": sh,
                        "tx_hash": txid,
                        "tx_pos": i,
                        "value": val,
                        "height": height
                    })
                    
                    self.batch_history.append({
                        "scripthash": sh,
                        "tx_hash": txid,
                        "height": height
                    })

    def flush_batch(self):
        if not self.batch_headers:
            return set()

        min_h = self.batch_headers[0]['height']
        max_h = self.batch_headers[-1]['height']
        
        print(f"[bold yellow]Flushing batch {min_h}-{max_h} ({len(self.batch_headers)} blocks)...[/bold yellow]")

        # 1. Headers (Partitioned)
        new_headers = pl.DataFrame(self.batch_headers, schema=self.headers_schema)
        new_headers.write_parquet(DATA_DIR / f"headers_{min_h}_{max_h}.parquet", compression="zstd")
        self.batch_headers = []

        # 2. Prepare DataFrames
        batch_new_utxos_df = pl.DataFrame(self.batch_new_utxos, schema=self.utxo.schema) if self.batch_new_utxos else pl.DataFrame(schema=self.utxo.schema)
        
        batch_spent_df = pl.DataFrame(self.batch_spent_outpoints, schema={
            "tx_hash": pl.String, 
            "tx_pos": pl.UInt32, 
            "spending_txid": pl.String,
            "height": pl.Int32
        }) if self.batch_spent_outpoints else pl.DataFrame(schema={
            "tx_hash": pl.String, 
            "tx_pos": pl.UInt32, 
            "spending_txid": pl.String,
            "height": pl.Int32
        })

        batch_history_adds = []
        if self.batch_history:
             batch_history_adds.extend(self.batch_history)

        # 3. Resolve intra-batch spends (inputs spending outputs created in the same batch)
        if not batch_spent_df.is_empty() and not batch_new_utxos_df.is_empty():
            # Find matches
            intra_spends = batch_new_utxos_df.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="inner")
            
            if not intra_spends.is_empty():
                for row in intra_spends.iter_rows(named=True):
                    batch_history_adds.append({
                        "scripthash": row["scripthash"],
                        "tx_hash": row["spending_txid"],
                        "height": row["height_right"] # Height of spending tx
                    })
                
                # Remove spent from new_utxos
                batch_new_utxos_df = batch_new_utxos_df.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="anti")
                
                # Remove handled spends from batch_spent_df
                batch_spent_df = batch_spent_df.join(intra_spends.select(["tx_hash", "tx_pos"]), on=["tx_hash", "tx_pos"], how="anti")

        # 4. Resolve DB spends (inputs spending outputs from DB)
        if not batch_spent_df.is_empty() and not self.utxo.is_empty():
            # Find matches in DB
            db_spends = self.utxo.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="inner")
            
            if not db_spends.is_empty():
                for row in db_spends.iter_rows(named=True):
                    batch_history_adds.append({
                        "scripthash": row["scripthash"],
                        "tx_hash": row["spending_txid"],
                        "height": row["height_right"]
                    })

                # Remove spent from DB
                self.utxo = self.utxo.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="anti")

        # 5. Commit Updates
        # Add remaining new UTXOs to DB
        if not batch_new_utxos_df.is_empty():
            self.utxo = pl.concat([self.utxo, batch_new_utxos_df])

        # Add History (Partitioned)
        touched_scripthashes = set()
        if batch_history_adds:
            new_hist_df = pl.DataFrame(batch_history_adds, schema=self.history_schema)
            
            # Add History (Partitioned by Scripthash Prefix - Read Optimized)
            new_hist_df = new_hist_df.with_columns(pl.col("scripthash").str.slice(0, 2).alias("sh_prefix"))
            for (prefix,), data in new_hist_df.partition_by("sh_prefix", as_dict=True).items():
                path = self.addr_index_path / f"sh_prefix={prefix}"
                path.mkdir(parents=True, exist_ok=True)
                data.drop("sh_prefix").write_parquet(path / f"history_{min_h}_{max_h}.parquet", compression="zstd")
            
            for item in batch_history_adds:
                touched_scripthashes.add(item['scripthash'])

        # Clear batch buffers
        self.batch_new_utxos = []
        self.batch_history = []
        self.batch_spent_outpoints = []
        
        return touched_scripthashes

    # Queries
    def get_balance(self, scripthash: str):
        # Confirmed only for this MVP (since we index blocks)
        res = self.utxo.filter(pl.col("scripthash") == scripthash)
        confirmed = res["value"].sum()
        return {"confirmed": confirmed, "unconfirmed": 0}

    def get_history(self, scripthash: str):
        try:
            prefix = scripthash[:2]
            partition_path = self.addr_index_path / f"sh_prefix={prefix}"
            
            if not partition_path.exists():
                return []
                
            res = pl.scan_parquet(partition_path / "*.parquet").filter(pl.col("scripthash") == scripthash).collect()
            return [{"tx_hash": r["tx_hash"], "height": r["height"]} for r in res.iter_rows(named=True)]
        except Exception:
            return []

    def list_unspent(self, scripthash: str):
        res = self.utxo.filter(pl.col("scripthash") == scripthash)
        return [{"tx_hash": r["tx_hash"], "tx_pos": r["tx_pos"], "value": r["value"], "height": r["height"]} for r in res.iter_rows(named=True)]

    def get_header(self, height: int):
        try:
            if not list(DATA_DIR.glob("headers_*.parquet")):
                return None
                
            res = pl.scan_parquet(DATA_DIR / "headers_*.parquet").filter(pl.col("height") == height).collect()
            if res.is_empty():
                return None
            return res["hex"][0] # TODO: Need actual hex
        except Exception:
            return None