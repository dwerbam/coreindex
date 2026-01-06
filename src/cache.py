import hashlib
import json
import gzip
import time
import asyncio
import shutil
from pathlib import Path

class PersistentCache:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        # Limit concurrent file operations to avoid "Too many open files"
        # macOS default limit is often 256, so 50 is safe leaving room for sockets/other files
        self.semaphore = asyncio.Semaphore(50)

    def _get_path(self, key: str) -> Path:
        # Hash the key to create a safe filename and distribution
        h = hashlib.sha256(key.encode()).hexdigest()
        # Sharding: use first 2 chars for subdir
        subdir = self.cache_dir / h[:2]
        subdir.mkdir(exist_ok=True)
        return subdir / f"{h}.json.gz"

    async def get(self, key: str):
        async with self.semaphore:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._get_sync, key)

    def _get_sync(self, key: str):
        try:
            path = self._get_path(key)
            if not path.exists():
                return None

            # Check TTL (using mtime) - default 1 day relative to file age
            # Note: The caller passes TTL to set(), but here we enforce a global max age or
            # we rely on cleanup(). The original code checked TTL on read.
            # Let's assume files older than 1 day are stale if we want to mimic previous logic,
            # or we can store expiry in the file wrapper.
            # Storing expiry is safer.

            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)

            if time.time() > data["expires_at"]:
                # Lazy delete
                try:
                    path.unlink()
                except:
                    pass
                return None

            return data["value"]
        except Exception as e:
            # print(f"Cache read error: {e}")
            return None

    async def set(self, key: str, value: any, ttl: int = 86400):
        async with self.semaphore:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._set_sync, key, value, ttl)

    def _set_sync(self, key: str, value: any, ttl: int):
        try:
            path = self._get_path(key)
            wrapper = {
                "value": value,
                "expires_at": time.time() + ttl
            }
            # Atomic write pattern: write to temp then rename
            temp_path = path.with_suffix(".tmp")
            with gzip.open(temp_path, "wt", encoding="utf-8") as f:
                json.dump(wrapper, f)
            temp_path.replace(path)
        except Exception as e:
            print(f"Cache write error: {e}")

    async def cleanup(self):
        # Run in background, also use semaphore if we were reading, but here we iterate
        # Iterating doesn't open files until we stat/unlink, but let's leave it separate
        # as it runs rarely.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._cleanup_sync)

    def _cleanup_sync(self):
        # This can be slow for millions of files, should be run rarely
        now = time.time()
        try:
            for subdir in self.cache_dir.iterdir():
                if subdir.is_dir():
                    for p in subdir.glob("*.json.gz"):
                        try:
                            # Optimization: check mtime first to avoid reading file
                            # If mtime + max_ttl < now, surely expired?
                            # But we don't know TTL per file without reading.
                            # However, we can assume a max TTL or just read.
                            # Reading every file is heavy.
                            # Let's check mtime. If file is older than 12 hours, delete.
                            if p.stat().st_mtime < now - 43200: # 12 hours
                                p.unlink()
                        except:
                            pass
        except Exception:
            pass
