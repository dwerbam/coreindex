import polars as pl
import hashlib
import asyncio
import time
import datetime
import gc
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import print
from src.config import DATA_DIR, FLUSH_INTERVAL, INDEX_START_HEIGHT, NETWORK
from src.rpc import BitcoinRPC
from src.utxo_db import UtxoDB
import src.crypto as crypto

class PerformanceMonitor:
    def __init__(self):
        self.fetch_wait = 0.0
        self.process_time = 0.0
        self.db_write_time = 0.0
        self.blocks_processed = 0
        self.start_time = time.time()

    def reset(self):
        self.fetch_wait = 0.0
        self.process_time = 0.0
        self.db_write_time = 0.0
        self.blocks_processed = 0
        self.start_time = time.time()

    def report(self):
        elapsed = time.time() - self.start_time
        if elapsed == 0: return ""
        bps = self.blocks_processed / elapsed
        return f"[dim]Stats (last {self.blocks_processed}): Wait={self.fetch_wait:.2f}s | Proc={self.process_time:.2f}s | DB={self.db_write_time:.2f}s | {bps:.1f} blk/s[/dim]"

class ETATimeRemainingColumn(TimeRemainingColumn):
    def render(self, task) -> str:
        base = super().render(task)
        return f"ETA {base}"

def get_scripthash(script_hex: str) -> bytes:
    """Calculate Electrum scripthash (SHA256 of scriptPubKey, little-endian)."""
    if not script_hex:
        return None
    return hashlib.sha256(bytes.fromhex(script_hex)).digest()[::-1]

class Indexer:
    START_BLOCK = INDEX_START_HEIGHT

    GENESIS_HASHES = {
        "mainnet": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
        "testnet": "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
    }

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc
        self.addr_index_path = DATA_DIR / "addr_index"
        self.tweaks_dir = DATA_DIR / "tweaks"
        
        # Batch buffers
        self.batch_headers = []
        self.batch_history = []
        self.batch_new_utxos = []
        self.batch_spent_outpoints = []
        self.batch_tweaks = []
        self.batch_filters = []
        
        self.utxo_db = UtxoDB(DATA_DIR)
        self.init_db()

    async def check_network(self):
        """Verifies that the connected Bitcoin Core node matches the configured network."""
        try:
            genesis_hash = await self.rpc.get_block_hash(0)
            expected = self.GENESIS_HASHES.get(NETWORK)
            if expected and genesis_hash != expected:
                print(f"[bold red]CRITICAL NETWORK MISMATCH[/bold red]")
                print(f"Configured for: [yellow]{NETWORK}[/yellow] (Genesis: {expected})")
                print(f"Connected node: [red]{genesis_hash}[/red]")
                raise RuntimeError(f"Network mismatch: Configured {NETWORK} but node has genesis {genesis_hash}")
            elif not expected:
                print(f"[yellow]Warning: Unknown network '{NETWORK}', skipping genesis check.[/yellow]")
        except Exception as e:
            print(f"[red]Failed to verify network genesis: {e}[/red]")
            # We don't crash here if RPC fails, might be temporary, but it will likely fail later.
            raise

    def init_db(self):
        self.headers_schema = {"height": pl.UInt32, "hash": pl.Binary, "hex": pl.Binary}
        self.history_schema = {"scripthash": pl.Binary, "tx_hash": pl.Binary, "height": pl.Int32}
        
        self.tweaks_schema = {
            "height": pl.UInt32, 
            "block_hash": pl.Binary, 
            "tx_hash": pl.Binary, 
            "tweak": pl.Binary
        }

        self.filters_schema = {
            "height": pl.UInt32,
            "block_hash": pl.Binary,
            "filter": pl.Binary
        }
        
        self.tweaks_dir.mkdir(parents=True, exist_ok=True)

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
        # self.utxo_db handles writes internally during flush_batch (update)
        return touched

    @property
    def height(self) -> int:
        def get_max_height(pattern, search_dir=DATA_DIR):
            files = list(search_dir.glob(pattern))
            if not files: return -1
            max_h = -1
            for f in files:
                try:
                    parts = f.stem.split('_')
                    end = int(parts[-1])
                    if end > max_h: max_h = end
                except: pass
            return max_h

        h_headers = get_max_height("headers_*_*.parquet")
        h_filters = get_max_height("filters_*_*.parquet")
        h_tweaks = get_max_height("tweaks_*_*.parquet", search_dir=self.tweaks_dir)
        
        if h_headers == -1:
            return -1
            
        if h_filters == -1 or h_tweaks == -1:
            print("[yellow]Filters or tweaks missing. Forcing re-sync/backfill.[/yellow]")
            return -1
            
        return min(h_headers, h_filters, h_tweaks)

    async def _fetch_worker(self, queue: asyncio.Queue, start_height: int, end_height: int, fetch_concurrency: int, progress, task):
        current_height = start_height
        while current_height <= end_height:
            batch_end = min(end_height, current_height + fetch_concurrency - 1)
            count = batch_end - current_height + 1

            while True:  # Retry loop for the current batch
                try:
                    # Update progress only if not already done in previous retry
                    progress.update(task, status=f"⬇️ Fetching {count}")

                    # 1. Fetch Hashes
                    heights = list(range(current_height, batch_end + 1))
                    hashes = await asyncio.gather(*[self.rpc.get_block_hash(h) for h in heights])

                    # 2. Fetch Blocks
                    blocks = await asyncio.gather(*[self.rpc.get_block(h, verbosity=2, use_cache=True) for h in hashes])

                    # Put blocks into queue
                    for i, block in enumerate(blocks):
                        await queue.put((heights[i], block))

                    # Success - move to next batch
                    current_height = batch_end + 1
                    break

                except Exception as e:
                    print(f"[red]Fetch error (retrying in 5s): {e}[/red]")
                    await asyncio.sleep(5)
                    # Retry the same batch

        # Signal completion
        await queue.put(None)

    async def sync(self):
        await self.check_network()
        core_height = await self.rpc.get_block_count()
        current_height = self.height

        # Start from Oct 1, 2021 (Block ~703000) if no data
        if current_height < self.START_BLOCK:
            print(f"[bold yellow]Skipping historical blocks. Starting sync from block {self.START_BLOCK} (Oct 2021)...[/bold yellow]")
            current_height = self.START_BLOCK - 1

        print(f"Index height: {current_height}, Core height: {core_height}")

        fetch_concurrency = 50
        queue = asyncio.Queue(maxsize=fetch_concurrency * 3)

        perf = PerformanceMonitor()

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[bold yellow]{task.fields[status]}"),
            TextColumn("[cyan]{task.fields[block_date]}"),
            ETATimeRemainingColumn(),
            TextColumn("{task.fields[perf_stats]}"), # Add perf stats column
        ) as progress:
            task = progress.add_task("Syncing...", total=core_height, completed=max(0, current_height),
                                   status="Starting", block_date="--/--/--", perf_stats="")

            fetch_task = asyncio.create_task(self._fetch_worker(queue, current_height + 1, core_height, fetch_concurrency, progress, task))

            try:
                while True:
                    t0 = time.time()
                    item = await queue.get()
                    perf.fetch_wait += time.time() - t0

                    if item is None:
                        break

                    h, block = item

                    # Extract block date
                    try:
                        b_time = block.get("time", 0)
                        date_str = datetime.datetime.fromtimestamp(b_time).strftime('%Y-%m-%d')
                    except:
                        date_str = "??"

                    progress.update(task, completed=h, status="⚙️ Indexing", block_date=date_str)

                    t1 = time.time()
                    self.process_block(block, h)
                    perf.process_time += time.time() - t1
                    perf.blocks_processed += 1

                    if h % 100 == 0: # Update stats every 100 blocks
                        progress.update(task, perf_stats=perf.report())
                        perf.reset()

                    if h % FLUSH_INTERVAL == 0 or h == core_height:
                        t2 = time.time()
                        touched = self.save_db()
                        perf.db_write_time += time.time() - t2
                        if touched:
                            yield touched
            finally:
                # Ensure worker is cleaned up
                if not fetch_task.done():
                    fetch_task.cancel()
                    try:
                        await fetch_task
                    except asyncio.CancelledError:
                        pass
                else:
                    await fetch_task

        touched = self.save_db()
        if touched:
            yield touched

    def process_block(self, block: dict, height: int):
        block_hash_bytes = bytes.fromhex(block["hash"])
        
        self.batch_headers.append((
            height,
            block_hash_bytes,
            b"" 
        ))

        output_scripts = []

        for tx in block["tx"]:
            txid = tx["txid"]
            txid_bytes = bytes.fromhex(txid)
            
            has_p2tr = False
            for vout in tx["vout"]:
                spk = vout["scriptPubKey"]["hex"]
                if len(spk) == 68 and spk.startswith("5120"):
                    has_p2tr = True
                    break

            if has_p2tr:
                input_pubkeys = []
                for vin in tx["vin"]:
                    if "coinbase" in vin:
                        continue
                    
                    script_sig = bytes.fromhex(vin.get("scriptSig", {}).get("hex", ""))
                    witness = vin.get("txinwitness", [])
                    
                    pk = crypto.get_pubkey_from_input(script_sig, witness)
                    if pk:
                        input_pubkeys.append(pk)
                
                tweak = crypto.compute_tweak(input_pubkeys)
                if tweak:
                    self.batch_tweaks.append((
                        height,
                        block_hash_bytes,
                        txid_bytes,
                        tweak
                    ))

            for vin in tx["vin"]:
                if "coinbase" in vin:
                    continue
                self.batch_spent_outpoints.append((
                    bytes.fromhex(vin["txid"]),
                    vin["vout"],
                    txid_bytes,
                    height
                ))

            for i, vout in enumerate(tx["vout"]):
                script_hex = vout["scriptPubKey"]["hex"]
                script_bytes = bytes.fromhex(script_hex)
                output_scripts.append(script_bytes)

                sh = get_scripthash(script_hex)
                if sh:
                    val = int(vout["value"] * 100_000_000)

                    self.batch_new_utxos.append((
                        sh,
                        txid_bytes,
                        i,
                        val,
                        height
                    ))

                    self.batch_history.append((
                        sh,
                        txid_bytes,
                        height
                    ))
        
        block_filter = crypto.create_block_filter(block["hash"], output_scripts)
        self.batch_filters.append((
            height,
            block_hash_bytes,
            block_filter
        ))

    def flush_batch(self):
        if not self.batch_headers:
            return set()

        min_h = self.batch_headers[0][0]
        max_h = self.batch_headers[-1][0]

        print(f"[bold yellow]Flushing batch {min_h}-{max_h} ({len(self.batch_headers)} blocks)...[/bold yellow]")

        # 1. Headers
        new_headers = pl.DataFrame(self.batch_headers, schema=["height", "hash", "hex"], orient="row")
        new_headers = new_headers.cast(self.headers_schema)
        new_headers.write_parquet(DATA_DIR / f"headers_{min_h}_{max_h}.parquet", compression="zstd")
        self.batch_headers = []

        # 2. Tweaks
        if self.batch_tweaks:
            new_tweaks = pl.DataFrame(self.batch_tweaks, schema=["height", "block_hash", "tx_hash", "tweak"], orient="row")
            new_tweaks = new_tweaks.cast(self.tweaks_schema)
            new_tweaks.write_parquet(self.tweaks_dir / f"tweaks_{min_h}_{max_h}.parquet", compression="zstd")
            self.batch_tweaks = []

        # 3. Filters
        if self.batch_filters:
            new_filters = pl.DataFrame(self.batch_filters, schema=["height", "block_hash", "filter"], orient="row")
            new_filters = new_filters.cast(self.filters_schema)
            new_filters.write_parquet(DATA_DIR / f"filters_{min_h}_{max_h}.parquet", compression="zstd")
            self.batch_filters = []

        # 4. Prepare DataFrames
        batch_new_utxos_df = pl.DataFrame(self.batch_new_utxos, schema=["scripthash", "tx_hash", "tx_pos", "value", "height"], orient="row") if self.batch_new_utxos else pl.DataFrame(schema=self.utxo_db.schema)
        if not batch_new_utxos_df.is_empty():
            batch_new_utxos_df = batch_new_utxos_df.cast(self.utxo_db.schema)

        batch_spent_df = pl.DataFrame(self.batch_spent_outpoints, schema=["tx_hash", "tx_pos", "spending_txid", "height"], orient="row") if self.batch_spent_outpoints else pl.DataFrame(schema={
            "tx_hash": pl.Binary,
            "tx_pos": pl.UInt32,
            "spending_txid": pl.Binary,
            "height": pl.Int32
        })
        if not batch_spent_df.is_empty():
            batch_spent_df = batch_spent_df.with_columns([
                pl.col("tx_hash").cast(pl.Binary),
                pl.col("tx_pos").cast(pl.UInt32),
                pl.col("spending_txid").cast(pl.Binary),
                pl.col("height").cast(pl.Int32)
            ])

        batch_history_adds = []
        if self.batch_history:
             batch_history_adds.extend(self.batch_history)

        # 5. Resolve intra-batch spends
        if not batch_spent_df.is_empty() and not batch_new_utxos_df.is_empty():
            intra_spends = batch_new_utxos_df.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="inner")
            if not intra_spends.is_empty():
                for row in intra_spends.iter_rows(named=True):
                    batch_history_adds.append((
                        row["scripthash"],
                        row["spending_txid"],
                        row["height_right"]
                    ))
                batch_new_utxos_df = batch_new_utxos_df.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="anti")
                batch_spent_df = batch_spent_df.join(intra_spends.select(["tx_hash", "tx_pos"]), on=["tx_hash", "tx_pos"], how="anti")

        # 6. Resolve DB spends
        if not batch_spent_df.is_empty():
            db_spends = self.utxo_db.resolve_spends(batch_spent_df)
            if not db_spends.is_empty():
                joined_spends = db_spends.join(batch_spent_df, on=["tx_hash", "tx_pos"], how="inner")
                for row in joined_spends.iter_rows(named=True):
                    batch_history_adds.append((
                        row["scripthash"],
                        row["spending_txid"],
                        row["height_right"]
                    ))

        # 7. Commit Updates
        touched_from_spends = self.utxo_db.update(batch_new_utxos_df, batch_spent_df)

        touched_scripthashes = set()
        if batch_history_adds:
            new_hist_df = pl.DataFrame(batch_history_adds, schema=["scripthash", "tx_hash", "height"], orient="row")
            new_hist_df = new_hist_df.cast(self.history_schema)

            new_hist_df = new_hist_df.with_columns(
                pl.col("scripthash").bin.encode("hex").str.slice(0, 2).alias("sh_prefix")
            )
            for (prefix,), data in new_hist_df.partition_by("sh_prefix", as_dict=True).items():
                path = self.addr_index_path / f"sh_prefix={prefix}"
                path.mkdir(parents=True, exist_ok=True)
                data.drop("sh_prefix").write_parquet(path / f"history_{min_h}_{max_h}.parquet", compression="zstd")

            for item in batch_history_adds:
                touched_scripthashes.add(item[0])
        
        touched_scripthashes.update(touched_from_spends)

        self.batch_new_utxos = []
        self.batch_history = []
        self.batch_spent_outpoints = []

        gc.collect()
        print(f"[bold green]Batch {min_h}-{max_h} flushed successfully.[/bold green]")

        return touched_scripthashes

    # Queries
    def get_balance(self, scripthash: str):
        # 1. Get history to find potential TXs
        try:
            sh_bytes = bytes.fromhex(scripthash)
        except ValueError:
            return {"confirmed": 0, "unconfirmed": 0}

        history = self._get_history_bytes(sh_bytes)
        if not history:
            return {"confirmed": 0, "unconfirmed": 0}
            
        tx_hashes = [item["tx_hash"] for item in history]
        
        # 2. Fetch UTXOs for these transactions
        utxos = self.utxo_db.get_utxos_for_tx_hashes(tx_hashes)
        
        # 3. Filter by scripthash (since tx may have multiple outputs)
        if not utxos.is_empty():
            res = utxos.filter(pl.col("scripthash") == sh_bytes)
            confirmed = res["value"].sum()
        else:
            confirmed = 0
            
        return {"confirmed": confirmed, "unconfirmed": 0}

    def get_history(self, scripthash: str):
        try:
            sh_bytes = bytes.fromhex(scripthash)
        except ValueError:
            return []
            
        return self._get_history_bytes(sh_bytes, return_hex=True)

    def _get_history_bytes(self, scripthash_bytes: bytes, return_hex=False):
        try:
            # Prefix from bytes
            prefix = scripthash_bytes.hex()[:2]
            partition_path = self.addr_index_path / f"sh_prefix={prefix}"

            if not partition_path.exists():
                return []

            res = pl.scan_parquet(partition_path / "*.parquet").filter(pl.col("scripthash") == scripthash_bytes).collect()
            
            rows = []
            for r in res.iter_rows(named=True):
                tx_hash = r["tx_hash"].hex() if return_hex else r["tx_hash"]
                rows.append({"tx_hash": tx_hash, "height": r["height"]})
                
            # Sort by height, then tx_hash for determinism
            rows.sort(key=lambda x: (x['height'], x['tx_hash']))
            return rows
        except Exception:
            return []

    def list_unspent(self, scripthash: str):
        try:
            sh_bytes = bytes.fromhex(scripthash)
        except ValueError:
            return []

        # 1. Get history
        history = self._get_history_bytes(sh_bytes, return_hex=False)
        if not history:
            return []
            
        tx_hashes = [item["tx_hash"] for item in history]
        
        # 2. Fetch UTXOs
        utxos = self.utxo_db.get_utxos_for_tx_hashes(tx_hashes)
        
        # 3. Filter and format
        if utxos.is_empty():
            return []
            
        res = utxos.filter(pl.col("scripthash") == sh_bytes)
        return [{"tx_hash": r["tx_hash"].hex(), "tx_pos": r["tx_pos"], "value": r["value"], "height": r["height"]} for r in res.iter_rows(named=True)]

    def get_header(self, height: int):
        try:
            if not list(DATA_DIR.glob("headers_*.parquet")):
                return None

            res = pl.scan_parquet(DATA_DIR / "headers_*.parquet").filter(pl.col("height") == height).collect()
            if res.is_empty():
                return None
            return res["hex"][0].hex() # Convert bytes to hex string
        except Exception:
            return None

    def get_filter(self, height: int):
        try:
            if not list(DATA_DIR.glob("filters_*.parquet")):
                return None
            
            # Simple scan (optimization: use partitioning if needed, but height is monotonic)
            # Actually, partitioning by height range is already done by flush logic
            res = pl.scan_parquet(DATA_DIR / "filters_*.parquet").filter(pl.col("height") == height).collect()
            
            if res.is_empty():
                return None
            return res["filter"][0].hex()
        except Exception:
            return None