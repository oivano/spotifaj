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
from utils.sqlite_cache import SQLiteCache
from spotifaj_functions import _spotify_call, find_playlist_by_name, get_playlist_track_ids, add_song_to_spotify_playlist, confirm

try:
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

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
        self.checkpoint_cache = SQLiteCache(db_path="cache/checkpoints.db", default_expiry_days=30)
        self.verifier = TrackVerifier(spotify_client, cache_manager=self.cache_manager)
        
        # Album cache to prevent duplicate API calls (many tracks share albums)
        self.album_cache = {}
        
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
            # CRITICAL: Validate that tracks actually match the release
            # Spotify search is fuzzy and returns approximate matches
            for track in results['tracks']['items']:
                if not track or 'id' not in track:
                    continue
                
                # Check if the album name matches the Discogs release title
                album_name = track.get('album', {}).get('name', '').lower()
                release_title_lower = title.lower()
                
                # Strict matching: album name must closely match release title
                # Allow for minor variations like "Deluxe Edition", "Remastered", etc.
                if (album_name == release_title_lower or 
                    album_name.startswith(release_title_lower + ' ') or
                    album_name.startswith(release_title_lower + '-') or
                    album_name.startswith(release_title_lower + '(') or
                    release_title_lower in album_name):
                    track_data = [(track['id'], CONFIDENCE_HIGH)]
                    all_tracks.extend(track_data)
                else:
                    # Album name doesn't match - this is a false positive from fuzzy search
                    logger.debug(f"Rejecting track '{track['name']}' - album '{album_name}' doesn't match release '{title}'")
        else:
            # Fall back to other strategies with stricter validation
            for query in strategies[1:]:
                results = self._try_spotify_search(query, max_retries)
                
                if results and results['tracks']['items']:
                    for track in results['tracks']['items']:
                        if not track or 'id' not in track:
                            continue
                        
                        # For fallback strategies, require even stricter matching
                        album_name = track.get('album', {}).get('name', '').lower()
                        release_title_lower = title.lower()
                        
                        # Only accept if album name closely matches
                        if (album_name == release_title_lower or 
                            album_name.startswith(release_title_lower + ' ')):
                            track_data = [(track['id'], CONFIDENCE_HIGH)]
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
        """Find tracks from Spotify's label search using smart year-based search."""
        low_confidence_track_data = []
        
        try:
            # Smart exhaustive search: Start recent and work backwards until no results
            logger.info(f"Performing exhaustive 'label:' search for '{label_name}' (smart year-based)")
            
            current_year = datetime.now().year
            seen_ids = set()
            empty_years_in_row = 0
            max_empty_years = 5  # Stop if 5 consecutive years have no results
            
            # Search backwards from current year to find all tracks
            for year in range(current_year + 1, 1949, -1):  # Go backwards
                try:
                    # CRITICAL: Rate limit between years to prevent ban
                    self._spotify_wait_for_rate_limit()
                    
                    query = f'label:"{label_name}" year:{year}'
                    results = _spotify_call(lambda: self.sp.search(query, limit=50, type='track'))
                    
                    if not results or not results.get('tracks'):
                        empty_years_in_row += 1
                        if empty_years_in_row >= max_empty_years:
                            logger.debug(f"Stopping label search: {max_empty_years} consecutive empty years (stopped at {year})")
                            break
                        continue
                    
                    page = results['tracks']
                    items = page.get('items', [])
                    
                    if not items:
                        empty_years_in_row += 1
                        if empty_years_in_row >= max_empty_years:
                            logger.debug(f"Stopping label search: {max_empty_years} consecutive empty years (stopped at {year})")
                            break
                        continue
                    
                    # Found results - reset counter
                    empty_years_in_row = 0
                    
                    # Process first page
                    for item in items:
                        if item and 'id' in item and item['id'] not in seen_ids:
                            seen_ids.add(item['id'])
                            low_confidence_track_data.append((item['id'], CONFIDENCE_LABEL_SEARCH))
                    
                    # Process remaining pages for this year
                    while page.get('next'):
                        self._spotify_wait_for_rate_limit()
                        page = _spotify_call(lambda: self.sp.next(page))
                        if not page:
                            break
                        
                        items = page.get('items', [])
                        for item in items:
                            if item and 'id' in item and item['id'] not in seen_ids:
                                seen_ids.add(item['id'])
                                low_confidence_track_data.append((item['id'], CONFIDENCE_LABEL_SEARCH))
                
                except Exception as e:
                    logger.debug(f"Error searching year {year}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error during exhaustive label search: {e}")
        
        logger.info(f"Found {len(low_confidence_track_data)} tracks from exhaustive label search")
        return low_confidence_track_data
    
    def get_label_tracks(self, label, force_update=False, include_playlist=None, strictness='normal'):
        """
        Get all Spotify tracks for a Discogs label.
        """
        # Add checkpointing capability using SQLite
        checkpoint_key = f"checkpoint_label_{label.id}_{label.name}"
    
        # Check for existing checkpoint
        if not force_update:
            checkpoint_data = self.checkpoint_cache.get(checkpoint_key)
            if checkpoint_data:
                logger.info(f"Resuming from checkpoint for {label.name}")
                return checkpoint_data.get('tracks', [])
    
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
            
            # 4. DISABLED: Playlist discovery causes excessive API calls and rate limiting
            # The Discogs matching + exhaustive label search are already comprehensive
            # If needed in future, add --enable-playlist-discovery flag
            playlist_track_data = []
            logger.debug("Playlist discovery disabled to prevent rate limiting")
            all_track_data.extend(playlist_track_data)
            
            # Print summary statistics
            high_count = len(high_confidence_track_data)
            low_count = len(low_confidence_track_data)
            playlist_count = len(playlist_track_data)
            
            # DEBUG: Check for overlap before merging
            discogs_ids = set(tid for tid, _ in high_confidence_track_data)
            label_search_ids = set(tid for tid, _ in low_confidence_track_data)
            overlap_count = len(discogs_ids & label_search_ids)
            
            print(f"\n📊 Tracks found: {len(all_track_data)} total ({high_count} high confidence, "
                  f"{low_count} from label search, {playlist_count} from playlists)")
            logger.debug(f"DEBUG: {overlap_count} tracks appear in both Discogs and label search")
            logger.debug(f"DEBUG: Unique Discogs IDs: {len(discogs_ids)}, Unique label search IDs: {len(label_search_ids)}")
            print(f"Using {strictness} matching strictness")
            
            # 5. Verify and deduplicate tracks
            if all_track_data:
                print(f"Using {strictness} matching strictness")
                
                # First pass: verify tracks (returns high-confidence and low-confidence separately)
                high_conf_ids, low_conf_verified_ids = self._verify_tracks(all_track_data, label)
                
                # Review low-confidence tracks before accepting
                if low_conf_verified_ids:
                    from spotifaj_functions import validate_tracks_list
                    
                    logger.info(f"\n{len(low_conf_verified_ids)} low-confidence tracks passed verification.")
                    logger.info("These tracks were found via label: search and may include false positives.")
                    
                    # Fetch full track details for review
                    low_conf_tracks = []
                    for i in range(0, len(low_conf_verified_ids), 50):
                        chunk = low_conf_verified_ids[i:i+50]
                        tracks_details = _spotify_call(lambda: self.sp.tracks(chunk))
                        if tracks_details:
                            low_conf_tracks.extend([t for t in tracks_details['tracks'] if t])
                    
                    # Show interactive validation
                    accepted_low_conf_ids = validate_tracks_list(
                        self.sp, 
                        low_conf_tracks, 
                        label.name if hasattr(label, 'name') else str(label)
                    )
                    
                    if accepted_low_conf_ids is None:
                        logger.info("Low-confidence tracks rejected. Using only Discogs-verified tracks.")
                        verified_tracks = high_conf_ids
                    else:
                        verified_tracks = high_conf_ids + accepted_low_conf_ids
                else:
                    verified_tracks = high_conf_ids
                
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
                    
                # Save checkpoint with verified tracks to SQLite
                try:
                    checkpoint_data = {
                        'label': label.name,
                        'label_id': label.id,
                        'timestamp': time.time(),
                        'tracks': final_track_ids,
                        'track_count': len(final_track_ids)
                    }
                    self.checkpoint_cache.set(
                        checkpoint_key,
                        checkpoint_data,
                        metadata={'label': label.name, 'count': len(final_track_ids)},
                        expiry_days=30
                    )
                    logger.info(f"Saved checkpoint for {label.name} to SQLite cache")
                except Exception as e:
                    logger.warning(f"Failed to save checkpoint: {e}")
    
                return final_track_ids
            
            return []
        except Exception as e:
            logger.error(f"Error in Discogs workflow: {e}")
            return []
    
    def create_label_playlist(self, track_ids, label_name, playlist_name=None, description=None):
        """
        Create a new playlist with the found tracks or add to existing playlist.
        """
        if not track_ids:
            logger.warning("No tracks provided, playlist not created")
            return None
        
        # Default playlist name and description if not provided
        if not playlist_name:
            playlist_name = f"{label_name} - Discogs Verified"
        
        if not description:
            description = f"Verified releases for {label_name} from Discogs."
        
        try:
            # Check if playlist already exists
            existing_playlist_id = find_playlist_by_name(self.user_id, playlist_name)
            
            if existing_playlist_id:
                # Playlist exists - get current tracks
                existing_track_ids = get_playlist_track_ids(self.user_id, existing_playlist_id)
                
                # Find new tracks to add
                new_tracks = [tid for tid in track_ids if tid not in existing_track_ids]
                
                if new_tracks:
                    logger.info(f"Adding {len(new_tracks)} new tracks to existing playlist")
                    add_song_to_spotify_playlist(self.user_id, new_tracks, existing_playlist_id, sp=self.sp)
                    logger.info(f"Successfully added {len(new_tracks)} tracks to playlist")
                    
                    # Get playlist URL
                    playlist_info = self.sp.playlist(existing_playlist_id)
                    logger.info(f"Playlist URL: {playlist_info['external_urls']['spotify']}")
                    return existing_playlist_id
                else:
                    logger.info(f"All tracks already in playlist. No new tracks to add.")
                
                return existing_playlist_id
            else:
                # Create new playlist
                logger.info(f"Creating new playlist '{playlist_name}'")
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
            logger.error(f"Error creating/updating playlist: {e}")
            return None

    def _verify_track_belongs_to_label(self, track, label_name, known_discogs_albums=None):
        """
        Strict verification that a track actually belongs to a label.
        Used for playlist discovery to filter out false positives.
        
        FIXED: Now uses _spotify_call() wrapper and album caching to prevent rate limiting.
        Returns True only if we have strong evidence the track is from this label.
        """
        if not track:
            return False
        
        label_lower = label_name.lower()
        
        # Get album information
        album_id = track.get('album', {}).get('id')
        if not album_id:
            return False
        
        try:
            # CRITICAL FIX: Check cache first to avoid duplicate API calls
            if album_id in self.album_cache:
                album = self.album_cache[album_id]
            else:
                # CRITICAL FIX: Use _spotify_call() wrapper for proper rate limiting
                album = _spotify_call(lambda: self.sp.album(album_id))
                if not album:
                    return False
                # Cache the album to prevent duplicate lookups
                self.album_cache[album_id] = album
            
            # CHECK 1: Copyright exact match (strongest signal)
            copyrights = album.get('copyrights', [])
            for copyright in copyrights:
                copyright_text = copyright.get('text', '').lower()
                # Require exact label name match in copyright (not just substring)
                if f" {label_lower} " in f" {copyright_text} " or \
                   copyright_text.startswith(label_lower + " ") or \
                   copyright_text.endswith(" " + label_lower):
                    return True
            
            # CHECK 2: Album matches a known Discogs release
            if known_discogs_albums:
                album_name = album.get('name', '').lower().strip()
                if album_name in known_discogs_albums:
                    return True
            
            # CHECK 3: Label explicitly listed in album metadata
            if album.get('label'):
                album_label = album['label'].lower()
                if label_lower == album_label or label_lower in album_label.split(','):
                    return True
            
            # If none of the strict checks pass, reject the track
            return False
            
        except Exception as e:
            logger.debug(f"Error verifying track {track.get('id')}: {e}")
            return False
    
    def _verify_tracks(self, track_ids_with_confidence, label):
        """
        Verify tracks and adjust their confidence scores.
        Returns tuple: (high_confidence_ids, low_confidence_verified_ids)
        """
        # Extract the label name
        if hasattr(label, 'name'):
            label_name = label.name
        else:
            label_name = str(label)
        
        logger.info("Verifying & deduplicating tracks...")
        
        # Pre-process to get unique track IDs with highest confidence
        track_confidence = {}
        duplicates_found = 0
        confidence_upgrades = 0
        
        for track_id, confidence in track_ids_with_confidence:
            if track_id in track_confidence:
                duplicates_found += 1
                old_conf = track_confidence[track_id]
                new_conf = max(track_confidence[track_id], confidence)
                track_confidence[track_id] = new_conf
                if new_conf > old_conf:
                    confidence_upgrades += 1
            else:
                track_confidence[track_id] = confidence
        
        # Count confidence distribution AFTER deduplication
        high_conf_count = sum(1 for c in track_confidence.values() if c >= CONFIDENCE_HIGH)
        low_conf_count = sum(1 for c in track_confidence.values() if c < CONFIDENCE_HIGH)
    
        logger.info(f"After deduplication: {len(track_confidence)} unique tracks (from {len(track_ids_with_confidence)} total)")
        logger.debug(f"DEBUG: Found {duplicates_found} duplicate track IDs, {confidence_upgrades} got upgraded to higher confidence")
        logger.debug(f"DEBUG: Confidence distribution after dedup: {high_conf_count} high (>={CONFIDENCE_HIGH}), {low_conf_count} low")
        
        verified_tracks = []
        
        # CRITICAL FIX: Separate high-confidence (Discogs) from low-confidence (need verification)
        high_confidence_ids = []
        low_confidence_ids = []
        
        for track_id, confidence in track_confidence.items():
            if confidence >= CONFIDENCE_HIGH:
                # Discogs-verified - trust immediately, no API calls needed
                high_confidence_ids.append(track_id)
                verified_tracks.append(track_id)
            else:
                # Low-confidence - needs verification
                low_confidence_ids.append(track_id)
        
        logger.info(f"Auto-verified {len(high_confidence_ids)} high-confidence tracks (Discogs)")
        
        if len(high_confidence_ids) < high_conf_count:
            logger.warning(f"⚠️  Expected {high_conf_count} high-confidence tracks but only found {len(high_confidence_ids)} "
                         f"(missing {high_conf_count - len(high_confidence_ids)} tracks)")
        
        if not low_confidence_ids:
            logger.info("No low-confidence tracks to verify")
            return high_confidence_ids, []
        
        logger.info(f"Verifying {len(low_confidence_ids)} low-confidence tracks...")
        total_batches = (len(low_confidence_ids) + 49) // 50
        
        low_confidence_verified = []
    
        # Only fetch details for low-confidence tracks that need verification
        if RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
            ) as progress:
                task = progress.add_task(f"Verifying low-confidence tracks...", total=total_batches)
                
                for i in range(0, len(low_confidence_ids), 50):
                    chunk = low_confidence_ids[i:i+50]
                    batch_num = (i // 50) + 1
                    progress.update(task, description=f"Verifying batch {batch_num}/{total_batches}")
                    
                    # CRITICAL: Longer delay between batches to prevent burst detection
                    # Standard _spotify_wait_for_rate_limit() is too aggressive for large batch operations
                    if i > 0:  # Skip first batch
                        time.sleep(2.0)  # 2 seconds between batches prevents Spotify burst detection
                    
                    try:
                        # Use centralized _spotify_call with proper retry/backoff for 429 errors
                        tracks_details = _spotify_call(lambda: self.sp.tracks(chunk))
                        
                        if tracks_details:
                            for track in tracks_details['tracks']:
                                if not track:
                                    continue
                                
                                track_id = track['id']
                                base_confidence = track_confidence[track_id]
                                
                                try:
                                    # Low-confidence source - require Spotify verification
                                    final_confidence = self.verifier.calculate_track_confidence(
                                        track, label_name, base_confidence
                                    )
                                    
                                    # Require copyright OR label match (>= 70)
                                    if final_confidence >= 70:
                                        low_confidence_verified.append(track_id)
                                except Exception as e:
                                    logger.error(f"Error verifying track {track_id}: {e}")
                    except Exception as e:
                        logger.error(f"Error verifying track batch: {e}")
                    
                    progress.update(task, advance=1)
        else:
            # Fallback without progress bar
            for i in range(0, len(low_confidence_ids), 50):
                chunk = low_confidence_ids[i:i+50]
                
                # CRITICAL: Rate limit between batches
                if i > 0:
                    self._spotify_wait_for_rate_limit()
                
                try:
                    # Use centralized _spotify_call with proper retry/backoff for 429 errors
                    tracks_details = _spotify_call(lambda: self.sp.tracks(chunk))
                    
                    if tracks_details:
                        for track in tracks_details['tracks']:
                            if not track:
                                continue
                            
                            track_id = track['id']
                            base_confidence = track_confidence[track_id]
                            
                            try:
                                # Low-confidence source - require Spotify verification
                                final_confidence = self.verifier.calculate_track_confidence(
                                    track, label_name, base_confidence
                                )
                                
                                if final_confidence >= 70:
                                    low_confidence_verified.append(track_id)
                            except Exception as e:
                                logger.error(f"Error verifying track {track_id}: {e}")
                except Exception as e:
                    logger.error(f"Error verifying track batch: {e}")
    
        # Log cache statistics if debug enabled
        if logger.isEnabledFor(logging.DEBUG):
            cache_stats = self.verifier.get_cache_stats()
            logger.debug(f"Album cache: {cache_stats['cached_albums']} albums cached "
                        f"({cache_stats['cache_size_mb']:.2f} MB)")
    
        return high_confidence_ids, low_confidence_verified
