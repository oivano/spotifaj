"""
Track Verifier
-------------
Implements intelligent verification of tracks based on metadata analysis.
"""
import re
import logging
import time

logger = logging.getLogger('track_verifier')

class TrackVerifier:
    """Verifies track matches between Discogs and Spotify with improved accuracy."""
    
    def __init__(self, spotify_client, cache_manager=None):
        self.sp = spotify_client
        self.logger = logging.getLogger('track_verifier')
        self.cache_manager = cache_manager
        
    def calculate_track_confidence(self, track, label_name, base_confidence=50):
        """Calculate confidence that a track belongs to a specific label."""
        confidence = base_confidence
        copyright_match = None  # Initialize this variable
        artist_match = False    # Initialize this variable

        # Convert label name to lowercase for case-insensitive comparison
        label_lower = label_name.lower()
        
        # Check track name for label mention (small boost)
        if track.get('name') and label_lower in track['name'].lower():
            confidence += 5
            
        # ENHANCED: Get album to check copyright and other metadata
        album_id = None
        if track.get('album', {}).get('id'):
            album_id = track['album']['id']
        
        # If we have an album ID, get detailed album info including copyrights
        if album_id:
            try:
                album = self.sp.album(album_id)  # Use self.sp, not self.spotify_client
                
                # 1. Check copyright information (strongest indicator)
                copyright_match = self._check_copyright_for_label(album, label_name)
                if copyright_match == 'exact':
                    confidence += 40  # Major boost for exact copyright match
                elif copyright_match == 'partial':
                    confidence += 20  # Good boost for partial match
                    
                # 2. Check artist-label association (if artist ID is available)
                artist_id = track.get('artists', [{}])[0].get('id')
                if artist_id:
                    artist_match = self._check_artist_label_association(artist_id, label_name)
                    if artist_match:
                        confidence += 10  # Boost for artist-label association
                
                # 3. Check album name for label mention (additional boost)
                album_name = album.get('name')
                if album_name and label_lower in album_name.lower():
                    confidence += 5  # Small boost for label mention in album name
                
            except Exception as e:
                logger.debug(f"Error getting album data: {e}")

        # Bonus for tracks with multiple positive signals
        if copyright_match and artist_match:
            confidence += 10
        
        return min(confidence, 100)

    def _whole_word_match(self, substring, text):
        """Check if substring exists as a whole word within text."""
        # FIX: Add null check for both parameters
        if substring is None or text is None:
            return False
            
        pattern = r'\b' + re.escape(substring) + r'(\b|$)'
        return bool(re.search(pattern, text))

    def _check_copyright(self, track, label_name):
        """Check if the label name appears in the copyright information."""
        # FIX: Add null check for label_name
        if label_name is None:
            return False
            
        label_lower = label_name.lower()
        base_label = re.sub(r'\s+(records|recordings|music|audio|productions)$', '', label_lower)
        
        # Generate variations of the label name to check
        labels_to_check = {
            label_lower,
            base_label,
            f"{base_label} recordings", 
            f"{base_label} records"
        }
        
        # Add abbreviations for multi-word labels
        words = base_label.split()
        if len(words) > 1:
            # Add initial-based abbreviation (e.g., "Basic Channel" -> "BC")
            abbrev = ''.join(word[0] for word in words)
            labels_to_check.add(abbrev)
            labels_to_check.add(abbrev.lower())
            
            # Add first-letter expanded abbreviation (e.g., "Basic Channel" -> "BCP")
            labels_to_check.add(f"{abbrev}P")
            labels_to_check.add(f"{abbrev.lower()}p")
        
        # Check copyright information
        copyright_texts = []
        # FIX: Add defensive checks for missing album or copyrights fields
        album = track.get('album', {})
        if album is None:
            album = {}
            
        for c in album.get('copyrights', []):
            if c and c.get('text'):
                copyright_texts.append(c['text'].lower())
                
        # FIX: Handle case where no copyright texts are found
        if not copyright_texts:
            return False
            
        copyright_text = ' | '.join(copyright_texts)
        
        for label in labels_to_check:
            if self._whole_word_match(label, copyright_text):
                return True
                
        return False
    
    def _check_copyright_for_label(self, album, label_name):
        """
        Check if album copyrights mention the label.
        Returns 'exact', 'partial', or None
        """
        if not album.get('copyrights'):
            return None
            
        label_patterns = [
            label_name,  # Exact match
            label_name.replace(' ', ''),  # No spaces
            label_name.replace('Recordings', 'Recording'),  # Singular/plural variations
            label_name.replace('Records', 'Record')
        ]
        
        # Common record label suffixes to check
        label_core = label_name.split(' ')[0]  # First word of label name
        
        for copyright_item in album['copyrights']:
            text = copyright_item.get('text', '').lower()
            
            # Check for exact matches first
            for pattern in label_patterns:
                if pattern.lower() in text:
                    return 'exact'
                    
            # Check for partial matches with the core label name
            if label_core.lower() in text:
                return 'partial'
                
        return None

    def _check_artist_label_association(self, artist_id, label_name):
        """
        Check if an artist has released on this label before.
        Uses cached results to avoid repeated lookups.
        """
        if not artist_id:
            return False
            
        cache_key = f"artist_label_{artist_id}_{label_name}"
        
        # Check cache first
        if hasattr(self, 'association_cache'):
            if cache_key in self.association_cache:
                return self.association_cache[cache_key]
        else:
            self.association_cache = {}
        
        # For now, assume no association (this would be improved with actual label data)
        result = False
        self.association_cache[cache_key] = result
        return result
    
    def mark_false_positive(self, track_id, label_name):
        """Store known false positives to improve future verification."""
        if self.cache_manager:
            cache_key = f"false_positive_{track_id}_{label_name}"
            self.cache_manager.save_to_cache(cache_key, True)
    
    def register_label_keywords(self, label_name, keywords):
        """Register specific keywords associated with a label."""
        if not hasattr(self, 'label_keywords'):
            self.label_keywords = {}
        self.label_keywords[label_name] = keywords
