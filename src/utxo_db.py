import polars as pl
from pathlib import Path

class UtxoDB:
    def __init__(self, data_dir: Path):
        self.base_path = data_dir / "utxo"
        self.base_path.mkdir(parents=True, exist_ok=True)
        # Schema: scripthash, tx_hash, tx_pos, value, height
        self.schema = {
            "scripthash": pl.Binary,
            "tx_hash": pl.Binary,
            "tx_pos": pl.UInt32,
            "value": pl.UInt64,
            "height": pl.Int32
        }

    def get_partition_path(self, key: str) -> Path:
        return self.base_path / f"part_{key}.parquet"

    def update(self, new_utxos_df: pl.DataFrame, spent_utxos_df: pl.DataFrame) -> set:
        """
        Updates the UTXO set with new UTXOs and removes spent ones.
        Returns a set of touched scripthashes (from spends).
        """
        touched_scripthashes = set()
        
        # Add partition key to inputs (use hex representation of first byte)
        if not new_utxos_df.is_empty():
            new_utxos_df = new_utxos_df.with_columns(
                pl.col("tx_hash").bin.encode("hex").str.slice(0, 2).alias("pkey")
            )
        
        if not spent_utxos_df.is_empty():
            spent_utxos_df = spent_utxos_df.with_columns(
                pl.col("tx_hash").bin.encode("hex").str.slice(0, 2).alias("pkey")
            )

        # Identify all affected partitions
        pkeys = set()
        if not new_utxos_df.is_empty():
            pkeys.update(new_utxos_df["pkey"].unique().to_list())
        if not spent_utxos_df.is_empty():
            pkeys.update(spent_utxos_df["pkey"].unique().to_list())

        # Process each partition
        for pkey in pkeys:
            path = self.get_partition_path(pkey)
            
            # Load existing
            if path.exists():
                current_df = pl.read_parquet(path)
            else:
                current_df = pl.DataFrame(schema=self.schema)

            # 1. Remove Spends
            # We need to find which scripthashes are being spent to report them
            if not spent_utxos_df.is_empty():
                spends_in_partition = spent_utxos_df.filter(pl.col("pkey") == pkey)
                if not spends_in_partition.is_empty():
                    # Find the UTXOs being spent to get their scripthash
                    spending_utxos = current_df.join(
                        spends_in_partition, on=["tx_hash", "tx_pos"], how="inner"
                    )
                    
                    if not spending_utxos.is_empty():
                        touched_scripthashes.update(spending_utxos["scripthash"].to_list())
                        
                        # Remove them
                        current_df = current_df.join(
                            spends_in_partition, on=["tx_hash", "tx_pos"], how="anti"
                        )

            # 2. Add New
            if not new_utxos_df.is_empty():
                adds_in_partition = new_utxos_df.filter(pl.col("pkey") == pkey)
                if not adds_in_partition.is_empty():
                    adds_in_partition = adds_in_partition.drop("pkey")
                    current_df = pl.concat([current_df, adds_in_partition])
                    # Dedup to handle re-syncs
                    current_df = current_df.unique(subset=["tx_hash", "tx_pos"], keep="last")

            # Write back
            if not current_df.is_empty():
                tmp_path = path.with_suffix(".tmp")
                current_df.write_parquet(tmp_path, compression="zstd")
                tmp_path.replace(path) # Atomic replacement
            elif path.exists():
                path.unlink() # Empty partition

        return touched_scripthashes

    def resolve_spends(self, potential_spends_df: pl.DataFrame) -> pl.DataFrame:
        """
        Checks which of the potential spends are actually in the UTXO set.
        Returns a DataFrame of valid spends joined with UTXO info (scripthash, etc).
        """
        if potential_spends_df.is_empty():
            return pl.DataFrame(schema=self.schema)

        potential_spends_df = potential_spends_df.with_columns(
            pl.col("tx_hash").bin.encode("hex").str.slice(0, 2).alias("pkey")
        )
        
        valid_spends = []
        
        pkeys = potential_spends_df["pkey"].unique().to_list()
        for pkey in pkeys:
            path = self.get_partition_path(pkey)
            if not path.exists():
                continue
                
            current_df = pl.read_parquet(path)
            subset = potential_spends_df.filter(pl.col("pkey") == pkey)
            
            # Find matches
            matched = current_df.join(subset, on=["tx_hash", "tx_pos"], how="inner")
            if not matched.is_empty():
                valid_spends.append(matched)

        if not valid_spends:
            return pl.DataFrame(schema=self.schema)
            
        return pl.concat(valid_spends).select(["scripthash", "tx_hash", "tx_pos", "value", "height"])

    def get_utxos_for_tx_hashes(self, tx_hashes: list) -> pl.DataFrame:
        """
        Retrieves UTXOs for a list of transaction hashes (list of bytes).
        Efficiently loads only relevant partitions.
        """
        if not tx_hashes:
             return pl.DataFrame(schema=self.schema)

        # Create lookup df
        lookup_df = pl.DataFrame({"tx_hash": tx_hashes})
        # Generate pkey from bytes
        lookup_df = lookup_df.with_columns(
            pl.col("tx_hash").bin.encode("hex").str.slice(0, 2).alias("pkey")
        )
        
        results = []
        pkeys = lookup_df["pkey"].unique().to_list()
        
        for pkey in pkeys:
            path = self.get_partition_path(pkey)
            if not path.exists():
                continue
            
            # We filter the partition by the specific tx_hashes we are looking for
            subset_hashes = lookup_df.filter(pl.col("pkey") == pkey)["tx_hash"]
            
            df = pl.read_parquet(path).filter(pl.col("tx_hash").is_in(subset_hashes))
            if not df.is_empty():
                results.append(df)
                
        if not results:
             return pl.DataFrame(schema=self.schema)
             
        return pl.concat(results)