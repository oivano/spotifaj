"""
Discogs Label Workflow
--------------------
Handles fetching releases from Discogs and finding matching tracks on Spotify.
"""
import os
import re
import time
import logging
from datetime import datetime
import json

# Fix the import to use the correct client class
from clients.discogs_client import get_discogs_client
from utils.cache_manager import CacheManager
from utils.track_verifier import TrackVerifier
from utils.track_deduplicator import deduplicate_tracks
from constants import (
    SPOTIFY_MIN_REQUEST_INTERVAL,
    SPOTIFY_BURST_LIMIT,
    SPOTIFY_BURST_COOLDOWN,
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
    CONFIDENCE_LABEL_SEARCH,
    CONFIDENCE_PLAYLIST_DISCOVERY,
    MAX_PLAYLISTS_FOR_DISCOVERY,
    RELEASE_PROCESSING_BATCH_SIZE,
    DISCOGS_SEARCH_PAGE_LIMIT,
    BATCH_PROCESSING_MIN_TIME,
    BATCH_COOLDOWN_BETWEEN,
)

logger = logging.getLogger('discogs_workflow')

class DiscogsLabelWorkflow:
    """
    Implements a workflow for finding Spotify tracks that match Discogs releases.
    
    Uses a sophisticated mapping and verification system to ensure accuracy.
    """
    
    def __init__(self, spotify_client, discogs_client=None, cache_manager=None):
        """
        Initialize the workflow with required clients.
        
        Args:
            spotify_client: Authenticated Spotify client
            discogs_client: Optional Discogs client (will create one if not provided)
            cache_manager: Optional cache manager instance
        """
        self.sp = spotify_client
        self.discogs = discogs_client or get_discogs_client()
        self.cache_manager = cache_manager or CacheManager()
        self.verifier = TrackVerifier(spotify_client, cache_manager=self.cache_manager)
        
        # Add Spotify-specific rate limiting
        self.spotify_last_request_time = 0
        self.spotify_min_request_interval = SPOTIFY_MIN_REQUEST_INTERVAL  # 2 requests per second to be safe
        self.spotify_burst_count = 0
        self.spotify_burst_limit = SPOTIFY_BURST_LIMIT
        self.spotify_burst_reset_time = 0
        self.spotify_burst_cooldown = SPOTIFY_BURST_COOLDOWN  # 1 second cooldown after hitting burst limit
        
        # Store user ID for later use
        try:
            self.user_id = self.sp.current_user()['id']
        except Exception as e:
            logger.error(f"Failed to get user ID: {e}")
            # Don't raise here, allow partial functionality
            self.user_id = None
    
    def _find_label(self, label_input):
        """
        Find a Discogs label from URL or search term.
        """
        # Check if input is a URL
        if label_input.startswith(('http://', 'https://')) and 'discogs.com' in label_input:
            return self.discogs.find_label_by_url(label_input)
        else:
            return self.discogs.find_label_by_name(label_input)
    
    def _search_spotify_for_release(self, release, max_retries=3):
        """
        Search Spotify for tracks matching a Discogs release.
        Optimized for better speed while respecting rate limits.
        """
        # Generate a cache key for this specific release
        release_id = release.get('id', '')
        cache_key = f"spotify_search_release_{release_id}"
        
        # Try to get cached results first
        if self.cache_manager:
            cached_results = self.cache_manager.load_from_cache(cache_key)
            if cached_results:
                return cached_results
    
        artist = release['artist']
        title = release['title']
        
        # Clean up artist and title for better matching
        artist = re.sub(r'\s*\([^)]*\)', '', artist)  # Remove parenthetical text
        artist = re.sub(r'\s*\[[^\]]*\]', '', artist)  # Remove bracketed text
    
        # Performance optimization: Try strategies in parallel but respect rate limits
        # Use a smarter combined query first before trying multiple queries
        strategies = [
            # Most efficient combined query first
            f'artist:"{artist}" album:"{title}"',
            
            # Only fall back to these if needed
            f'artist:"{artist}" track:"{title}"',
            f'"{artist}" "{title}"'
        ]
        
        all_tracks = []
        
        # Try the first (most efficient) strategy
        results = self._try_spotify_search(strategies[0], max_retries)
        
        if results and results['tracks']['items']:
            # Found tracks with the efficient query, no need for others
            confidence_base = CONFIDENCE_HIGH
            track_data = [(track['id'], confidence_base) for track in results['tracks']['items'] 
                         if track and 'id' in track]
            all_tracks.extend(track_data)
        else:
            # Fall back to other strategies with reduced confidence
            for i, query in enumerate(strategies[1:], 1):
                confidence_base = CONFIDENCE_HIGH - (i * 20)
                
                results = self._try_spotify_search(query, max_retries)
                
                if results and results['tracks']['items']:
                    track_data = [(track['id'], confidence_base) for track in results['tracks']['items'] 
                                 if track and 'id' in track]
                    all_tracks.extend(track_data)
        
        # Cache the results to avoid future API calls
        if self.cache_manager:
            self.cache_manager.save_to_cache(cache_key, all_tracks)
            
        return all_tracks

    def _spotify_wait_for_rate_limit(self):
        """Handle Spotify-specific rate limiting"""
        now = time.time()
        
        # If we're in a burst cooldown, wait until it's over
        if self.spotify_burst_count >= self.spotify_burst_limit:
            if now - self.spotify_burst_reset_time < self.spotify_burst_cooldown:
                sleep_time = self.spotify_burst_cooldown - (now - self.spotify_burst_reset_time)
                time.sleep(sleep_time)
                self.spotify_burst_count = 0
                self.spotify_burst_reset_time = time.time()
            else:
                # Cooldown period is over
                self.spotify_burst_count = 0
        
        # Apply normal request interval
        elapsed = now - self.spotify_last_request_time
        if elapsed < self.spotify_min_request_interval:
            time.sleep(self.spotify_min_request_interval - elapsed)
        
        # Update tracking
        self.spotify_last_request_time = time.time()
        self.spotify_burst_count += 1
        
        # If we've hit the burst limit, start cooldown
        if self.spotify_burst_count >= self.spotify_burst_limit:
            self.spotify_burst_reset_time = time.time()
    
    def _try_spotify_search(self, query, max_retries=3):
        """Helper method to search Spotify with retries and rate limiting"""
        for attempt in range(max_retries):
            try:
                # Apply Spotify-specific rate limit
                self._spotify_wait_for_rate_limit()
                
                # Fix: Don't use 'q=' parameter name - pass query as first positional argument
                results = self.sp.search(query, type='track', limit=50)
                return results
            except Exception as e:
                if "429" in str(e):
                    # Rate limit handling
                    retry_after = 1
                    if hasattr(e, 'headers') and 'Retry-After' in e.headers:
                        retry_after = int(e.headers['Retry-After'])
                    
                    logger.warning(f"Spotify rate limit hit. Waiting {retry_after}s")
                    time.sleep(retry_after)
                else:
                    # Other error handling
                    wait_time = 0.5 * (attempt + 1)
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                    else:
                        logger.warning(f"Failed to search: {query} - {e}")
        return None
    
    def _parallel_search_releases(self, releases, max_workers=1):
        """
        Process releases in batches optimized for Spotify API rate limits.
        """
        results = []
        total_releases = len(releases)
        processed = 0
        
        # Optimize batch size for Spotify - larger batches are more efficient
        batch_size = RELEASE_PROCESSING_BATCH_SIZE  # Spotify can handle larger batches
        
        logger.info(f"Starting search for {total_releases} releases...")

        for i in range(0, total_releases, batch_size):
            batch = releases[i:i+batch_size]
            batch_results = []
            batch_start_time = time.time()
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_releases + batch_size - 1)//batch_size} ({len(batch)} releases)")

            # Process the batch
            for release in batch:
                try:
                    track_data = self._search_spotify_for_release(release)
                    batch_results.extend(track_data)
                    processed += 1
                except Exception as e:
                    logger.error(f"Error searching for release: {e}")
                    processed += 1
            
            # Extend results all at once (more efficient)
            results.extend(batch_results)
            
        # Only cache if batch completed successfully
        batch_time = time.time() - batch_start_time
        
        # Add inter-batch cooldown if processing next batch
        if i + batch_size < total_releases and batch_time < BATCH_PROCESSING_MIN_TIME:
            time.sleep(BATCH_COOLDOWN_BETWEEN)
        
        return results
    
    def _get_tracks_from_label_search(self, label_name):
        """Find tracks from Spotify's label search (low confidence)."""
        low_confidence_track_data = []
        
        try:
            # Broad label search
            logger.info(f"Performing broad 'label:' search for '{label_name}' (Low-Confidence)")
            # Fix: Use positional argument instead of q=
            results = self.sp.search(f'label:"{label_name}"', type='track', limit=50)
            page_count = 0
            
            while results and page_count < DISCOGS_SEARCH_PAGE_LIMIT:  # Limit pages to prevent excessive API calls
                items = results['tracks']['items']
                if not items:
                    break
                    
                for item in items:
                    if item and 'id' in item:
                        low_confidence_track_data.append((item['id'], CONFIDENCE_LABEL_SEARCH))  # Medium confidence for label search
                        
                if results['tracks']['next']:
                    self._spotify_wait_for_rate_limit()
                    results = self.sp.next(results['tracks'])
                    page_count += 1
                else:
                    results = None
                    
        except Exception as e:
            logger.error(f"Error during broad label search: {e}")
            
        logger.info(f"Found {len(low_confidence_track_data)} tracks from label search")
        return low_confidence_track_data
    
    def get_label_tracks(self, label, force_update=False, include_playlist=None, strictness='normal'):
        """
        Get all Spotify tracks for a Discogs label.
        """
        # Add checkpointing capability
        checkpoint_file = f"checkpoint_{label.name.replace(' ', '_')}.json"
    
        # Check for existing checkpoint
        if os.path.exists(checkpoint_file) and not force_update:
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                    logger.info(f"Resuming from checkpoint for {label.name}")
                    return checkpoint_data['tracks']
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
    
        try:
            # 1. Get all releases for the label from Discogs
            cache_key = f"discogs_label_{label.id}_releases"
            discogs_releases = self.discogs.get_all_label_releases(label, cache_key, force_update)
            
            if not discogs_releases or len(discogs_releases) == 0:
                logger.warning(f"No releases found for label: {label.name}")
                return []
                
            logger.info(f"Processing label: {label.name}")
            
            # Create the all_track_data list at the beginning
            all_track_data = []
            
            # 2. Find high-confidence tracks by direct release search
            logger.info(f"Scanning sources [Matching {len(discogs_releases)} releases]")
            high_confidence_track_data = self._parallel_search_releases(discogs_releases)
            all_track_data.extend(high_confidence_track_data)
            
            # 3. Find low-confidence tracks by label search
            logger.info(f"Performing broad 'label:' search for '{label.name}' (Low-Confidence)")
            low_confidence_track_data = self._get_tracks_from_label_search(label.name)
            all_track_data.extend(low_confidence_track_data)
            
            # 4. Process playlists
            playlist_track_data = []
            try:
                # Fix: Change 'search_type' to 'type'
                playlist_results = self.sp.search(f'"{label.name}"', type='playlist', limit=MAX_PLAYLISTS_FOR_DISCOVERY)
                playlists = playlist_results['playlists']['items'] if 'playlists' in playlist_results else []
                
                if playlists:
                    logger.info(f"Scanning sources [Processing {len(playlists)} playlists]")
                    
                    # Process playlists silently
                    total_found = 0
                    for i, playlist in enumerate(playlists):
                        try:
                            # Check if playlist exists and has an id
                            if not playlist or 'id' not in playlist:
                                continue
                            
                            # Rate limiting
                            self._spotify_wait_for_rate_limit()

                            # Get playlist items with error handling and manual retry
                            playlist_tracks = None
                            for attempt in range(3):
                                try:
                                    playlist_tracks = self.sp.playlist_items(playlist['id'])
                                    break
                                except Exception as e:
                                    # Check for rate limit
                                    is_rate_limit = False
                                    retry_after = 5
                                    
                                    if hasattr(e, 'http_status') and e.http_status == 429:
                                        is_rate_limit = True
                                        if hasattr(e, 'headers') and 'Retry-After' in e.headers:
                                            try:
                                                retry_after = int(e.headers['Retry-After'])
                                            except:
                                                pass
                                    elif "429" in str(e):
                                        is_rate_limit = True
                                    
                                    if is_rate_limit:
                                        # Cap retry time to avoid huge waits
                                        if retry_after > 60:
                                            retry_after = 60
                                        
                                        if attempt < 2:
                                            time.sleep(retry_after)
                                            continue
                                    
                                    # If not rate limit or max retries reached
                                    if logger.isEnabledFor(logging.DEBUG):
                                        logger.debug(f"Error fetching playlist items: {e}")
                                    break

                            if not playlist_tracks:
                                continue
                                
                            # Process tracks silently
                            items = playlist_tracks.get('items', [])
                            playlist_found_count = 0
                            
                            for item in items:
                                if not item or 'track' not in item or not item['track']:
                                    continue
                                    
                                track = item['track']
                                if track and 'id' in track:
                                    playlist_track_data.append((track['id'], CONFIDENCE_PLAYLIST_DISCOVERY))  # Medium confidence
                                    playlist_found_count += 1
                                
                            total_found += playlist_found_count
                            
                        except Exception as e:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"Error processing playlist: {e}")
                
                # Log summary only once at the end
                logger.info(f"Found {total_found} tracks from {len(playlists)} playlists")
                
            except Exception as e:
                logger.error(f"Error searching for playlists: {e}")
    
            all_track_data.extend(playlist_track_data)
            
            # Print summary statistics
            high_count = len(high_confidence_track_data)
            low_count = len(low_confidence_track_data)
            playlist_count = len(playlist_track_data)
            
            print(f"\n📊 Tracks found: {len(all_track_data)} total ({high_count} high confidence, "
                  f"{low_count} from label search, {playlist_count} from playlists)")
            print(f"Using {strictness} matching strictness")
            
            # 5. Verify and deduplicate tracks
            if all_track_data:
                print(f"Using {strictness} matching strictness")
                
                # First pass: verify tracks
                verified_tracks = self._verify_tracks(all_track_data, label)
                
                # Second pass: deduplicate
                final_track_ids = deduplicate_tracks(self.sp, verified_tracks, display_progress=False)
                
                # Cache the result
                if final_track_ids:
                    cache_key = f"discogs_label_{label.id}_final_tracks"
                    self.cache_manager.save_to_cache(cache_key, final_track_ids, {
                        'label': label.name,
                        'count': len(final_track_ids),
                        'timestamp': datetime.now().isoformat(),
                    })
                    
                # Save checkpoint with verified tracks
                try:
                    checkpoint_data = {
                        'label': label.name,
                        'timestamp': time.time(),
                        'tracks': final_track_ids
                    }
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint_data, f)
                    logger.info(f"Saved checkpoint for {label.name}")
                except Exception as e:
                    logger.warning(f"Failed to save checkpoint: {e}")
    
                return final_track_ids
            
            return []
        except Exception as e:
            logger.error(f"Error in Discogs workflow: {e}")
            return []
    
    def create_label_playlist(self, track_ids, label_name, playlist_name=None, description=None):
        """
        Create a new playlist with the found tracks.
        """
        if not track_ids:
            logger.warning("No tracks provided, playlist not created")
            return None
        
        # Default playlist name and description if not provided
        if not playlist_name:
            playlist_name = f"{label_name} - Discogs Verified"
        
        if not description:
            description = f"Verified releases for {label_name} from Discogs."
        
        # Create empty playlist
        try:
            logger.info(f"Creating playlist '{playlist_name}'")
            playlist = self.sp.user_playlist_create(self.user_id, playlist_name, public=False, 
                                                   description=description)
            
            # Add tracks in batches of 100 (Spotify API limit)
            for i in range(0, len(track_ids), 100):
                chunk = track_ids[i:i+100]
                self.sp.playlist_add_items(playlist['id'], chunk)
                time.sleep(0.5)  # Gentle rate limiting
        
            logger.info(f"Successfully created playlist with {len(track_ids)} tracks")
            logger.info(f"Playlist URL: {playlist['external_urls']['spotify']}")
            return playlist['id']
            
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None

    def _verify_tracks(self, track_ids_with_confidence, label):
        """
        Verify tracks and adjust their confidence scores.
        """
        # Extract the label name
        if hasattr(label, 'name'):
            label_name = label.name
        else:
            label_name = str(label)
        
        logger.info("Verifying & deduplicating tracks...")
        
        # Pre-process to get unique track IDs with highest confidence
        track_confidence = {}
        for track_id, confidence in track_ids_with_confidence:
            if track_id in track_confidence:
                track_confidence[track_id] = max(track_confidence[track_id], confidence)
            else:
                track_confidence[track_id] = confidence
    
        verified_tracks = []
        unique_ids = list(track_confidence.keys())  # Extract IDs only
    
        # Verify tracks in batches
        for i in range(0, len(unique_ids), 300):
            chunk = unique_ids[i:i+300]  # These are now string IDs, not tuples
            try:
                # Pass string IDs to Spotify API, not tuples
                tracks_details = self.sp.tracks(chunk)
                
                for track in tracks_details['tracks']:
                    if not track:
                        continue
                    
                    track_id = track['id']
                    base_confidence = track_confidence[track_id]
                    
                    try:
                        final_confidence = self.verifier.calculate_track_confidence(
                            track, label_name, base_confidence
                        )
                        
                        # Only include tracks with sufficient confidence
                        if final_confidence >= 50:
                            verified_tracks.append(track_id)
                    except Exception as e:
                        logger.error(f"Error verifying track {track_id}: {e}")
            except Exception as e:
                logger.error(f"Error verifying track batch: {e}")
    
        return verified_tracks
