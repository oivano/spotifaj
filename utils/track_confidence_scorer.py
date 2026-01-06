"""
Track Confidence Scorer
----------------------
Multi-signal verification system for scoring track confidence against a target label.

Uses:
- Label field matching
- Copyright information
- Distributor detection (filters false positives)
- Album caching for performance
"""
import logging
from spotifaj_functions import _spotify_call

logger = logging.getLogger('track_confidence_scorer')

class TrackConfidenceScorer:
    """
    Scores individual tracks using multi-signal verification.
    
    Confidence Signals:
    - Discovery method: base 40
    - Label field: +30 exact, +25 prefix, +10 substring
    - Copyright: +50 exact, +25 partial
    
    Final score: clamped to 0-100
    
    Note: Distributor detection was removed because:
    - No authoritative API or database exists
    - Manual lists are incomplete and unmaintainable
    - Copyright field already provides strongest signal
    """
    
    def __init__(self, spotify_client, target_label):
        """
        Initialize scorer for a specific label.
        
        Args:
            spotify_client: Authenticated Spotify client
            target_label: Label name to verify against
        """
        self.sp = spotify_client
        self.target_label = target_label.lower().strip()
        self.album_cache = {}  # Session-level cache to prevent duplicate API calls
        
        logger.debug(f"Initialized confidence scorer for label: {target_label}")
    
    def score_track(self, track, base_confidence=40):
        """
        Calculate confidence score for a single track.
        
        Args:
            track: Spotify track object with album information
            base_confidence: Starting confidence score (default: 40)
            
        Returns:
            Integer confidence score (0-100)
        """
        if not track:
            return 0
        
        album_id = track.get('album', {}).get('id')
        if not album_id:
            logger.debug(f"Track {track.get('id')} has no album ID")
            return 0
        
        # Get album with caching (prevents duplicate API calls)
        album = self._get_album_cached(album_id)
        if not album:
            logger.debug(f"Failed to fetch album {album_id}")
            return 0
        
        confidence = base_confidence
        signals = []
        
        # Signal 1: Label field match
        label_score = self._score_label_field(album)
        confidence += label_score
        if label_score > 0:
            signals.append(f"label_field:+{label_score}")
        
        # Signal 2: Copyright information (strongest indicator)
        # This is the most reliable signal as it's legally binding
        copyright_score = self._score_copyright(album)
        confidence += copyright_score
        if copyright_score > 0:
            signals.append(f"copyright:+{copyright_score}")
        
        # Clamp to valid range
        final_score = max(0, min(100, confidence))
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Track {track.get('id')}: {final_score} ({', '.join(signals) if signals else 'base only'})")
        
        return final_score
    
    def score_tracks_batch(self, tracks, base_confidence=40, min_threshold=70):
        """
        Score multiple tracks and return those meeting threshold.
        
        Args:
            tracks: List of Spotify track objects
            base_confidence: Starting confidence for each track (default: 40)
            min_threshold: Minimum score to include track (default: 70)
            
        Returns:
            Tuple of (verified_tracks, filtered_count)
        """
        verified = []
        filtered = 0
        
        for track in tracks:
            score = self.score_track(track, base_confidence)
            if score >= min_threshold:
                verified.append(track)
            else:
                filtered += 1
        
        return verified, filtered
    
    def _get_album_cached(self, album_id):
        """
        Get album data with session-level caching.
        
        Prevents duplicate API calls when multiple tracks share the same album.
        Critical for performance and rate limit compliance.
        """
        if album_id in self.album_cache:
            return self.album_cache[album_id]
        
        # Fetch album with rate limiting wrapper
        album = _spotify_call(lambda: self.sp.album(album_id))
        
        if album:
            self.album_cache[album_id] = album
        
        return album
    
    def _score_label_field(self, album):
        """
        Score based on album's label field match.
        
        Returns:
            +30 for exact match
            +25 for strict prefix
            +10 for substring match
            0 for no match
        """
        label = album.get('label', '').lower().strip()
        if not label:
            return 0
        
        # Exact match
        if label == self.target_label:
            return 30
        
        # Strict prefix (e.g., "Warp Records" matches "Warp")
        # Allows space, hyphen, slash separators
        for separator in [' ', '-', '/']:
            if label.startswith(self.target_label + separator):
                return 25
        
        # Substring match
        if self.target_label in label:
            return 10
        
        return 0
    
    def _score_copyright(self, album):
        """
        Score based on copyright information.
        
        Copyright is the strongest signal as it's legally binding.
        This is now the primary filtering mechanism.
        
        Returns:
            +50 for substantial match
            +25 for partial match
            0 for no match
        """
        copyrights = album.get('copyrights', [])
        if not copyrights:
            return 0
        
        for cr in copyrights:
            text = cr.get('text', '').lower()
            if not text:
                continue
            
            # Check for substantial match (whole word or phrase)
            # Use word boundaries to avoid false positives
            padded_text = f" {text} "
            padded_label = f" {self.target_label} "
            
            # Exact phrase match
            if padded_label in padded_text:
                return 50
            
            # Label at start or end
            if text.startswith(self.target_label) or text.endswith(self.target_label):
                return 50
            
            # Partial substring match
            if self.target_label in text:
                return 25
        
        return 0
    
    def get_cache_stats(self):
        """Return statistics about album cache usage."""
        return {
            'cached_albums': len(self.album_cache),
            'cache_size_mb': len(str(self.album_cache)) / (1024 * 1024)
        }
