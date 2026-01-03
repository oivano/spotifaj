"""
Constants for Spotifaj Application
----------------------------------
Centralized configuration values and magic numbers to improve maintainability.
Loads from config.yaml if available, otherwise uses defaults.
"""
import os
import yaml
from pathlib import Path
from typing import Any, Dict

def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml or return empty dict if not found."""
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Could not load config.yaml: {e}")
    return {}

# Load configuration
_config = load_config()

# Helper function to get nested config values with defaults
def _get_config(path: str, default: Any) -> Any:
    """Get configuration value from nested dict using dot notation."""
    keys = path.split('.')
    value = _config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    return value

# Confidence Thresholds
CONFIDENCE_THRESHOLD_AUTO_ACCEPT = _get_config('confidence.auto_accept', 70)
CONFIDENCE_HIGH = _get_config('confidence.high', 90)
CONFIDENCE_MEDIUM = _get_config('confidence.medium', 60)
CONFIDENCE_LOW = _get_config('confidence.low', 30)
CONFIDENCE_PLAYLIST_DISCOVERY = _get_config('confidence.playlist_discovery', 45)
CONFIDENCE_LABEL_SEARCH = _get_config('confidence.label_search', 50)
SIMILARITY_THRESHOLD_ARTIST_INTENT = _get_config('confidence.artist_intent', 0.8)
SIMILARITY_THRESHOLD_ALBUM_INTENT = _get_config('confidence.album_intent', 0.9)

# API Rate Limiting
DISCOGS_RATE_LIMIT_PER_MINUTE = _get_config('rate_limiting.discogs.per_minute', 25)
DISCOGS_MAX_RETRIES = _get_config('rate_limiting.discogs.max_retries', 5)
DISCOGS_RETRY_AFTER_BUFFER = _get_config('rate_limiting.discogs.retry_after_buffer', 1.0)
SPOTIFY_MIN_REQUEST_INTERVAL = _get_config('rate_limiting.spotify.min_request_interval', 0.5)
SPOTIFY_BURST_LIMIT = _get_config('rate_limiting.spotify.burst_limit', 25)
SPOTIFY_BURST_COOLDOWN = _get_config('rate_limiting.spotify.burst_cooldown', 1.0)
SPOTIFY_RETRY_BACKOFF_BASE = _get_config('rate_limiting.spotify.retry_backoff_base', 1.5)
SPOTIFY_MAX_RETRIES = _get_config('rate_limiting.spotify.max_retries', 3)
SPOTIFY_REQUEST_TIMEOUT = _get_config('rate_limiting.spotify.request_timeout', 20)
SPOTIFY_DEFAULT_DELAY = _get_config('rate_limiting.spotify.default_delay', 0.2)

# Batch Sizes
SPOTIFY_PLAYLIST_ADD_BATCH_SIZE = _get_config('batch_sizes.spotify_playlist_add', 100)
SPOTIFY_ALBUM_FETCH_BATCH_SIZE = _get_config('batch_sizes.spotify_album_fetch', 20)
SPOTIFY_SEARCH_DEFAULT_LIMIT = _get_config('batch_sizes.spotify_search_default', 50)
SPOTIFY_SEARCH_RESULT_LIMIT = _get_config('batch_sizes.spotify_search_max', 1000)
DISCOGS_SEARCH_PAGE_LIMIT = _get_config('batch_sizes.discogs_search_pages', 40)
TRACK_DEDUPLICATOR_BATCH_SIZE = _get_config('batch_sizes.track_deduplicator', 50)
RELEASE_PROCESSING_BATCH_SIZE = _get_config('batch_sizes.release_processing', 10)

# Display and UI
DEFAULT_DISPLAY_LIMIT = _get_config('display.default_limit', 20)
DISPLAY_INDENT_WIDTH = _get_config('display.indent_width', 2)
MAX_PLAYLIST_DESCRIPTION_WIDTH = _get_config('display.max_playlist_width', 45)

# Cache Settings
CACHE_DEFAULT_EXPIRY_DAYS = _get_config('cache.expiry_days', 7)
CACHE_VERSION = _get_config('cache.version', "2.0")
CACHE_TYPE = _get_config('cache.type', "json")  # "json" or "sqlite"
CACHE_SQLITE_PATH = _get_config('cache.sqlite_path', ".cache/spotifaj.db")

# Search and Matching
YEAR_SEARCH_START = _get_config('search.year_start', 1950)
YEAR_SEARCH_END = _get_config('search.year_end', 2026)
DURATION_BUCKET_SIZE_MS = _get_config('search.duration_bucket_ms', 1000)
MAX_PLAYLISTS_FOR_DISCOVERY = _get_config('search.max_playlists_discovery', 10)

# Error Recovery
BATCH_PROCESSING_MIN_TIME = _get_config('error_recovery.batch_min_time', 1.0)
BATCH_COOLDOWN_BETWEEN = _get_config('error_recovery.batch_cooldown', 0.5)

# General Settings
DEFAULT_COUNTRY_CODE = _get_config('general.country_code', 'US')
LOG_LEVEL = _get_config('general.log_level', 'INFO')

# Performance Profiling
PROFILING_ENABLED = _get_config('profiling.enabled', False)
PROFILING_OUTPUT_FILE = _get_config('profiling.output_file', 'performance.log')
PROFILING_TRACK_HOT_PATHS = _get_config('profiling.track_hot_paths', True)
