"""
Auto-update playlist tracker.

Tracks last update timestamp for smart playlists to enable incremental updates.
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
from config import logger

class AutoUpdateTracker:
    """Tracks last update times for playlists."""
    
    def __init__(self, cache_file: str = ".auto_update_cache.json"):
        """
        Initialize tracker.
        
        Args:
            cache_file: Path to cache file (relative to current directory)
        """
        self.cache_file = Path(cache_file)
        self.cache: Dict[str, Dict] = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """Load cache from file."""
        if not self.cache_file.exists():
            return {}
        
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load auto-update cache: {e}")
            return {}
    
    def _save_cache(self):
        """Save cache to file."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except IOError as e:
            logger.error(f"Could not save auto-update cache: {e}")
    
    def get_last_update(self, playlist_id: str) -> Optional[str]:
        """
        Get last update timestamp for a playlist.
        
        Args:
            playlist_id: Spotify playlist ID
            
        Returns:
            ISO format timestamp string or None if never updated
        """
        entry = self.cache.get(playlist_id, {})
        return entry.get('last_update')
    
    def set_last_update(self, playlist_id: str, timestamp: Optional[str] = None):
        """
        Set last update timestamp for a playlist.
        
        Args:
            playlist_id: Spotify playlist ID
            timestamp: ISO format timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        if playlist_id not in self.cache:
            self.cache[playlist_id] = {}
        
        self.cache[playlist_id]['last_update'] = timestamp
        self._save_cache()
    
    def get_metadata(self, playlist_id: str, key: str) -> Optional[str]:
        """
        Get metadata value for a playlist.
        
        Args:
            playlist_id: Spotify playlist ID
            key: Metadata key
            
        Returns:
            Metadata value or None
        """
        entry = self.cache.get(playlist_id, {})
        metadata = entry.get('metadata', {})
        return metadata.get(key)
    
    def set_metadata(self, playlist_id: str, key: str, value: str):
        """
        Set metadata for a playlist.
        
        Args:
            playlist_id: Spotify playlist ID
            key: Metadata key
            value: Metadata value
        """
        if playlist_id not in self.cache:
            self.cache[playlist_id] = {}
        
        if 'metadata' not in self.cache[playlist_id]:
            self.cache[playlist_id]['metadata'] = {}
        
        self.cache[playlist_id]['metadata'][key] = value
        self._save_cache()
    
    def get_all_tracked(self) -> Dict[str, Dict]:
        """Get all tracked playlists with their metadata."""
        return self.cache.copy()
