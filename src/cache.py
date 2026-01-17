import sqlite3
import time
import json
import asyncio
from pathlib import Path

class PersistentCache:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    expires_at REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expires_at ON cache (expires_at)")
            conn.commit()

    async def get(self, key: str):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_sync, key)

    def _get_sync(self, key: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("SELECT value, expires_at FROM cache WHERE key = ?", (key,))
                row = cur.fetchone()
                if row:
                    value, expires_at = row
                    if time.time() < expires_at:
                        return json.loads(value)
                    else:
                        # Lazy delete
                        conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                        conn.commit()
        except Exception as e:
            print(f"Cache read error: {e}")
        return None

    async def set(self, key: str, value: any, ttl: int = 86400):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._set_sync, key, value, ttl)

    def _set_sync(self, key: str, value: any, ttl: int):
        try:
            expires_at = time.time() + ttl
            value_json = json.dumps(value)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO cache (key, value, expires_at)
                    VALUES (?, ?, ?)
                """, (key, value_json, expires_at))
                conn.commit()
        except Exception as e:
            print(f"Cache write error: {e}")

    async def cleanup(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._cleanup_sync)

    def _cleanup_sync(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM cache WHERE expires_at < ?", (time.time(),))
                conn.commit()
        except Exception:
            pass
