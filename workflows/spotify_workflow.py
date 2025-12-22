"""
Spotify Label Workflow
--------------------
Handles searching Spotify for tracks associated with a specific label,
using confidence scoring and intelligent verification.
"""
import os
import time
import logging
import itertools
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import spotipy
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn

from utils.cache_manager import CacheManager
from utils.track_verifier import TrackVerifier
from utils.track_deduplicator import deduplicate_tracks

logger = logging.getLogger('spotify_workflow')

class SpotifyLabelWorkflow:
    """
    Implements a robust workflow for finding tracks associated with a label on Spotify.
    Uses a confidence scoring system to categorize tracks and optimize search results.
    """
    
    def __init__(self, spotify_client, cache_manager=None):
        """
        Initialize the workflow with required clients.
        
        Args:
            spotify_client: Authenticated Spotify client
            cache_manager: Optional cache manager (will create one if not provided)
        """
        self.sp = spotify_client
        self.cache_manager = cache_manager or CacheManager()
        self.verifier = TrackVerifier(spotify_client, cache_manager=self.cache_manager)
        
        # Store user ID for later use
        try:
            self.user_id = self.sp.current_user()['id']
        except Exception as e:
            logger.error(f"Failed to get user ID: {e}")
            # Continue anyway, might be using client credentials
            self.user_id = None

    def _get_years_to_scan(self, year_input):
        """Parse and return the years to scan based on user input."""
        current_year = datetime.now().year
        
        if str(year_input) == '1':  # All years
            return list(range(1960, current_year + 1))
        elif str(year_input) == '2':  # Specific year
            # This should be handled by the caller usually, but for safety:
            return [current_year] 
        elif str(year_input) == '3':  # Custom range
            # Also should be handled by caller
            return [current_year]
        
        # Handle direct year input or range string
        try:
            if '-' in str(year_input):
                start, end = map(int, str(year_input).split('-'))
                return list(range(start, end + 1))
            else:
                return [int(year_input)]
        except:
            return [current_year]

    def _search_with_backoff(self, query, search_type='track', limit=50, max_retries=3):
        """Perform a search with exponential backoff for rate limiting."""
        for attempt in range(max_retries):
            try:
                return self.sp.search(query, type=search_type, limit=limit)
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429 and attempt < max_retries - 1:
                    retry_after = int(e.headers.get('Retry-After', 1 + attempt))
                    logger.warning(f"Rate limited. Waiting {retry_after}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_after)
                elif attempt < max_retries - 1:
                    logger.warning(f"Search error: {e}. Retrying in {2**attempt}s...")
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Failed after {max_retries} attempts: {e}")
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Unexpected error: {e}. Retrying...")
                    time.sleep(2)
                else:
                    logger.error(f"Unrecoverable error: {e}")
                    raise

    def _get_high_confidence_tracks(self, label_name, years_to_scan, progress=None, task_id=None):
        """
        Find high-confidence tracks using structured Spotify metadata searches.
        """
        high_confidence_patterns = [
            f'label:"{label_name}"'
        ]
        search_tasks = list(itertools.product(high_confidence_patterns, years_to_scan))
        
        logger.info(f"Performing high-confidence searches with {len(search_tasks)} queries")
        tracks = []
        
        for i, (pattern, year) in enumerate(search_tasks):
            try:
                query = f'{pattern} year:{year}'
                results = self._search_with_backoff(query)
                page_count = 0
                
                while results and page_count < 20:
                    items = results['tracks']['items']
                    if not items:
                        break
                        
                    tracks.extend(items)
                    
                    if results['tracks']['next'] and page_count < 19:
                        time.sleep(0.1)
                        results = self.sp.next(results['tracks'])
                        page_count += 1
                    else:
                        results = None
            except Exception as e:
                logger.error(f"Error in high-confidence search for year {year}: {e}")
            
            if progress and task_id:
                progress.update(task_id, advance=1)
                    
        track_data = [(track['id'], 90) for track in tracks if track and 'id' in track]
        return track_data

    def _get_medium_confidence_tracks(self, label_name, years_to_scan, progress=None, task_id=None):
        """
        Find medium-confidence tracks using broader but still structured searches.
        """
        medium_confidence_patterns = [
            f'album:"{label_name}"',
            f'label:"{label_name.split()[0]}"'
        ]
        search_tasks = list(itertools.product(medium_confidence_patterns, years_to_scan))
        
        logger.info(f"Performing medium-confidence searches with {len(search_tasks)} queries")
        tracks = []
        
        for i, (pattern, year) in enumerate(search_tasks):
            try:
                query = f'{pattern} year:{year}'
                results = self._search_with_backoff(query)
                page_count = 0
                
                while results and page_count < 10:
                    items = results['tracks']['items']
                    if not items:
                        break
                        
                    tracks.extend(items)
                    
                    if results['tracks']['next'] and page_count < 9:
                        time.sleep(0.1)
                        results = self.sp.next(results['tracks'])
                        page_count += 1
                    else:
                        results = None
            except Exception as e:
                logger.error(f"Error in medium-confidence search for year {year}: {e}")
            
            if progress and task_id:
                progress.update(task_id, advance=1)
                    
        track_data = [(track['id'], 60) for track in tracks if track and 'id' in track]
        return track_data

    def _get_low_confidence_tracks(self, label_name, years_to_scan, progress=None, task_id=None):
        """
        Find low-confidence tracks using broader text searches.
        """
        low_confidence_patterns = [
            f'"{label_name} Recordings"',
            f'"{label_name} Records"',
        ]
        # Limit years for efficiency in low confidence
        limited_years = years_to_scan[:10] if len(years_to_scan) > 10 else years_to_scan
        search_tasks = list(itertools.product(low_confidence_patterns, limited_years))
        
        logger.info(f"Performing low-confidence searches with {len(search_tasks)} queries")
        tracks = []
        
        for i, (pattern, year) in enumerate(search_tasks):
            try:
                query = f'{pattern} year:{year}'
                results = self._search_with_backoff(query)
                page_count = 0
                
                while results and page_count < 5:
                    items = results['tracks']['items']
                    if not items:
                        break
                        
                    tracks.extend(items)
                    
                    if results['tracks']['next'] and page_count < 4:
                        time.sleep(0.1)
                        results = self.sp.next(results['tracks'])
                        page_count += 1
                    else:
                        results = None
            except Exception as e:
                logger.error(f"Error in low-confidence search for year {year}: {e}")
            
            if progress and task_id:
                progress.update(task_id, advance=1)
                    
        track_data = [(track['id'], 30) for track in tracks if track and 'id' in track]
        return track_data

    def get_label_tracks(self, label_name, year_input='1', use_cache=True, cache_days=7):
        """
        Main method to find tracks for a label with confidence scoring.
        """
        cache_key = f"spotify_label_{label_name.lower().replace(' ', '_')}"
        
        if use_cache:
            cached_tracks = self.cache_manager.load_from_cache(cache_key, max_age_days=cache_days)
            if cached_tracks:
                logger.info(f"Using {len(cached_tracks)} tracks from cache")
                return cached_tracks
        
        years_to_scan = self._get_years_to_scan(year_input)
        logger.info(f"Searching years: {min(years_to_scan)} to {max(years_to_scan)}")
        
        all_tracks_with_confidence = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        ) as progress:
            
            # High Confidence
            task_high = progress.add_task("[green]High Confidence Search...", total=len(years_to_scan))
            high_conf_tracks = self._get_high_confidence_tracks(label_name, years_to_scan, progress, task_high)
            all_tracks_with_confidence.extend(high_conf_tracks)
            
            # Medium Confidence
            task_med = progress.add_task("[yellow]Medium Confidence Search...", total=len(years_to_scan) * 2)
            med_conf_tracks = self._get_medium_confidence_tracks(label_name, years_to_scan, progress, task_med)
            all_tracks_with_confidence.extend(med_conf_tracks)
            
            # Low Confidence
            limited_years_count = min(len(years_to_scan), 10)
            task_low = progress.add_task("[red]Low Confidence Search...", total=limited_years_count * 2)
            low_conf_tracks = self._get_low_confidence_tracks(label_name, years_to_scan, progress, task_low)
            all_tracks_with_confidence.extend(low_conf_tracks)

        # Verify and Deduplicate
        logger.info(f"Found {len(all_tracks_with_confidence)} total candidates. Verifying...")
        
        # Extract unique IDs, keeping highest confidence
        track_confidence = {}
        for track_id, confidence in all_tracks_with_confidence:
            if track_id in track_confidence:
                track_confidence[track_id] = max(track_confidence[track_id], confidence)
            else:
                track_confidence[track_id] = confidence
                
        unique_ids = list(track_confidence.keys())
        verified_tracks = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
        ) as progress:
            task_verify = progress.add_task("Verifying tracks...", total=len(unique_ids))
            
            for i in range(0, len(unique_ids), 50):
                chunk = unique_ids[i:i+50]
                try:
                    tracks_details = self.sp.tracks(chunk)
                    for track in tracks_details['tracks']:
                        if not track:
                            continue
                        
                        track_id = track['id']
                        base_confidence = track_confidence[track_id]
                        
                        final_confidence = self.verifier.calculate_track_confidence(
                            track, label_name, base_confidence
                        )
                        
                        if final_confidence >= 50:
                            verified_tracks.append(track_id)
                except Exception as e:
                    logger.error(f"Error verifying batch: {e}")
                
                progress.update(task_verify, advance=len(chunk))

        # Deduplicate
        final_unique_ids = deduplicate_tracks(self.sp, verified_tracks)
        
        if final_unique_ids:
            self.cache_manager.save_to_cache(cache_key, final_unique_ids, {
                'label': label_name,
                'years': years_to_scan,
                'timestamp': datetime.now().isoformat(),
                'count': len(final_unique_ids)
            })
            
        return final_unique_ids

    def create_label_playlist(self, label_name, track_ids, playlist_name=None, description=None):
        """Create a new playlist with the found tracks."""
        if not track_ids:
            return None
            
        if not playlist_name:
            playlist_name = f"{label_name} - Spotify Verified"
            
        if not description:
            description = f"Tracks from {label_name} label, created by Spotifaj"
            
        try:
            logger.info(f"Creating playlist '{playlist_name}'")
            playlist = self.sp.user_playlist_create(self.user_id, playlist_name, public=False, 
                                                  description=description)
            
            for i in range(0, len(track_ids), 100):
                chunk = track_ids[i:i+100]
                self.sp.playlist_add_items(playlist['id'], chunk)
                time.sleep(0.5)
                
            return playlist['id']
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None
