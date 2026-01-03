"""
SQLite Cache Manager
-------------------
High-performance caching backend using SQLite.
Replaces JSON-based caching for better performance with large datasets.
"""
import sqlite3
import json
import time
import logging
from pathlib import Path
from typing import Any, Optional, Dict
from datetime import datetime, timedelta

logger = logging.getLogger('sqlite_cache')

class SQLiteCache:
    """
    SQLite-based cache manager for improved performance.
    
    Features:
    - Faster lookups than JSON (indexed queries)
    - Automatic expiry management
    - Atomic operations
    - Better handling of large datasets
    """
    
    def __init__(self, db_path: str = ".cache/spotifaj.db", default_expiry_days: int = 7):
        """
        Initialize SQLite cache.
        
        Args:
            db_path: Path to SQLite database file
            default_expiry_days: Default cache expiration in days
        """
        self.db_path = Path(db_path)
        self.default_expiry_days = default_expiry_days
        
        # Create cache directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    metadata TEXT,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    version TEXT DEFAULT '2.0'
                )
            """)
            
            # Create index for expiry lookups
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at 
                ON cache(expires_at)
            """)
            
            conn.commit()
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT value, expires_at FROM cache WHERE key = ?",
                (key,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            value_json, expires_at = row
            
            # Check expiry
            if time.time() > expires_at:
                # Delete expired entry
                conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
                return None
            
            try:
                return json.loads(value_json)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode cached value for key: {key}")
                return None
    
    def set(self, key: str, value: Any, metadata: Optional[Dict] = None, 
            expiry_days: Optional[int] = None):
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache (must be JSON-serializable)
            metadata: Optional metadata dict
            expiry_days: Expiration in days (default: self.default_expiry_days)
        """
        if expiry_days is None:
            expiry_days = self.default_expiry_days
        
        now = time.time()
        expires_at = now + (expiry_days * 24 * 3600)
        
        try:
            value_json = json.dumps(value)
            metadata_json = json.dumps(metadata) if metadata else None
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO cache 
                    (key, value, metadata, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (key, value_json, metadata_json, now, expires_at))
                conn.commit()
        except (TypeError, json.JSONEncoder) as e:
            logger.error(f"Failed to cache value for key {key}: {e}")
    
    def delete(self, key: str):
        """Delete entry from cache."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()
    
    def clear_expired(self):
        """Remove all expired entries from cache."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM cache WHERE expires_at < ?",
                (time.time(),)
            )
            deleted = cursor.rowcount
            conn.commit()
            
            if deleted > 0:
                logger.info(f"Cleared {deleted} expired cache entries")
            
            return deleted
    
    def clear_all(self):
        """Clear entire cache."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cache")
            conn.commit()
            logger.info("Cleared all cache entries")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with sqlite3.connect(self.db_path) as conn:
            # Total entries
            total = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
            
            # Expired entries
            expired = conn.execute(
                "SELECT COUNT(*) FROM cache WHERE expires_at < ?",
                (time.time(),)
            ).fetchone()[0]
            
            # Database size
            db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            return {
                'total_entries': total,
                'expired_entries': expired,
                'active_entries': total - expired,
                'db_size_bytes': db_size,
                'db_size_mb': db_size / (1024 * 1024),
                'db_path': str(self.db_path)
            }
    
    def load_from_cache(self, key: str) -> Optional[Any]:
        """Alias for get() to maintain compatibility with CacheManager interface."""
        return self.get(key)
    
    def save_to_cache(self, key: str, value: Any, metadata: Optional[Dict] = None):
        """Alias for set() to maintain compatibility with CacheManager interface."""
        self.set(key, value, metadata)
