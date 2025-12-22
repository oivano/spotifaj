"""
Cache Manager
------------
Handles caching of API results to minimize API calls.
"""
import os
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger('cache_manager')

class CacheManager:
    """Manages caching of API results with versioning and expiration."""
    
    def __init__(self, cache_dir='cache', expiry_days=7):
        """
        Initialize with the given cache directory and default expiry.
        
        Args:
            cache_dir: Directory to store cache files
            expiry_days: Default number of days until cache entries expire
        """
        self.cache_dir = cache_dir
        self.default_expiry_days = expiry_days
        os.makedirs(cache_dir, exist_ok=True)
        logger.debug(f"Initialized CacheManager with directory: {cache_dir}, expiry: {expiry_days} days")
    
    def get_cache_path(self, cache_key):
        """Get the file path for a cache key."""
        safe_key = cache_key.replace('/', '_').replace('\\', '_')
        return os.path.join(self.cache_dir, f"{safe_key}.json")
    
    def load_from_cache(self, cache_key, max_age_days=None):
        """
        Load data from cache if it exists and is not too old.
        
        Args:
            cache_key: Unique identifier for the cached data
            max_age_days: Maximum age in days (None to use default expiry_days)
            
        Returns:
            Cached data or None if not found/expired
        """
        # Use default expiry if none specified
        if max_age_days is None:
            max_age_days = self.default_expiry_days
            
        cache_file = self.get_cache_path(cache_key)
        
        if not os.path.exists(cache_file):
            logger.debug(f"Cache miss: {cache_key} (file not found)")
            return None
            
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                
            # Check cache version
            version = cache_data.get('version', '1.0')
            if version < '2.0':
                logger.warning(f"Cache {cache_key} uses outdated format version {version}")
                # Handle legacy format
                if 'data' not in cache_data:
                    cache_data = {'data': cache_data, 'timestamp': datetime.now().isoformat()}
            
            # Check age if specified
            if max_age_days is not None:
                try:
                    timestamp = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))
                    age = datetime.now() - timestamp
                    if age > timedelta(days=max_age_days):
                        logger.debug(f"Cache expired: {cache_key} (age: {age.days} days > {max_age_days} days)")
                        return None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid timestamp in cache: {cache_key}")
                    return None
            
            # Check explicit expiry
            metadata = cache_data.get('metadata', {})
            if 'expires' in metadata:
                try:
                    expiry = datetime.fromisoformat(metadata['expires'])
                    if datetime.now() > expiry:
                        logger.debug(f"Cache expired: {cache_key} (explicit expiry: {expiry.isoformat()})")
                        return None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid expiry in cache: {cache_key}")
            
            logger.debug(f"Cache hit: {cache_key}")
            return cache_data.get('data')
            
        except json.JSONDecodeError:
            logger.warning(f"Corrupted cache file: {cache_file}")
            return None
        except Exception as e:
            logger.error(f"Error loading cache {cache_key}: {e}")
            return None
    
    def save_to_cache(self, cache_key, data, metadata=None):
        """
        Save data to cache with metadata.
        
        Args:
            cache_key: Unique identifier for the cached data
            data: The data to cache
            metadata: Optional dict of additional metadata
        """
        cache_file = self.get_cache_path(cache_key)
        
        try:
            cache_data = {
                'data': data,
                'metadata': metadata or {},
                'timestamp': datetime.now().isoformat(),
                'version': '2.0',
            }
            
            # Add default expiry based on self.default_expiry_days if not specified
            if 'expires' not in cache_data['metadata']:
                cache_data['metadata']['expires'] = (
                    datetime.now() + timedelta(days=self.default_expiry_days)
                ).isoformat()
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
            logger.debug(f"Saved to cache: {cache_key} ({len(str(data))} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to cache {cache_key}: {e}")
            return False
            
    def clear_cache(self, cache_key=None):
        """
        Clear specific cache entry or all cache if key is None.
        
        Args:
            cache_key: Specific cache key to clear, or None for all
            
        Returns:
            Number of files deleted
        """
        count = 0
        
        if cache_key:
            # Clear specific cache entry
            cache_file = self.get_cache_path(cache_key)
            if os.path.exists(cache_file):
                try:
                    os.remove(cache_file)
                    count = 1
                    logger.debug(f"Cleared cache: {cache_key}")
                except Exception as e:
                    logger.error(f"Error clearing cache {cache_key}: {e}")
        else:
            # Clear all cache files
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.json'):
                    try:
                        os.remove(os.path.join(self.cache_dir, filename))
                        count += 1
                    except Exception as e:
                        logger.error(f"Error removing cache file {filename}: {e}")
            
            logger.info(f"Cleared all cache files: {count} removed")
            
        return count
