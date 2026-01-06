"""
Helper functions for Spotify interactions.
"""

import datetime
import time
from typing import Optional, List, Dict, Any, Callable, Set, Tuple
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from config import settings, logger
from constants import (
    SPOTIFY_MAX_RETRIES,
    SPOTIFY_RETRY_BACKOFF_BASE,
    SPOTIFY_REQUEST_TIMEOUT,
    SPOTIFY_PLAYLIST_ADD_BATCH_SIZE,
    SPOTIFY_ALBUM_FETCH_BATCH_SIZE,
    DEFAULT_COUNTRY_CODE,
    YEAR_SEARCH_START,
    YEAR_SEARCH_END,
    DURATION_BUCKET_SIZE_MS,
)

# Import advanced deduplication logic
try:
    from utils.track_deduplicator import generate_track_signature as advanced_signature
except ImportError:
    advanced_signature = None

try:
    from rich.console import Console
    from rich.table import Table
    console = Console()
except ImportError:
    console = None

# Default scope for user operations
DEFAULT_SCOPE = (
    "user-read-private "
    "playlist-read-private "
    "playlist-modify-private "
    "playlist-modify-public "
    "playlist-read-collaborative "
    "user-library-modify "
    "user-modify-playback-state "
    "user-library-read "
    "ugc-image-upload"
)

COUNTRY = DEFAULT_COUNTRY_CODE


def _spotify_call(fn: Callable, retries: int = SPOTIFY_MAX_RETRIES, backoff: float = SPOTIFY_RETRY_BACKOFF_BASE) -> Optional[Any]:
    """Call a Spotify API function with simple retry/backoff for 429/5xx."""
    for attempt in range(retries):
        try:
            return fn()
        except spotipy.exceptions.SpotifyException as e:
            status = getattr(e, 'http_status', None)
            retry_after = None
            if hasattr(e, 'headers'):
                retry_after = e.headers.get('Retry-After')
            if status == 429 and attempt < retries - 1:
                sleep_for = int(retry_after) if retry_after else backoff ** attempt
                time.sleep(sleep_for)
                continue
            if status and 500 <= status < 600 and attempt < retries - 1:
                time.sleep(backoff ** attempt)
                continue
            logger.error(f"Spotify API error (status {status}): {e}")
            return None
        except Exception as e:  # pragma: no cover - unexpected runtime errors
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
                continue
            logger.error(f"Unexpected Spotify call error: {e}")
            return None

def get_spotify_client(username: Optional[str] = None, scope: str = DEFAULT_SCOPE) -> Optional[spotipy.Spotify]:
    """
    Returns an authenticated spotipy.Spotify client.
    
    If username is provided, attempts to use User Authorization (OAuth).
    Otherwise, falls back to Client Credentials (public data only).
    """
    if username:
        # User Authorization with OAuth (supports automatic token refresh)
        try:
            from spotipy.oauth2 import SpotifyOAuth
            
            auth_manager = SpotifyOAuth(
                client_id=settings.spotipy_client_id,
                client_secret=settings.spotipy_client_secret,
                redirect_uri=settings.spotipy_redirect_uri,
                scope=scope,
                username=username,
                cache_path=f".cache-{username}"
            )
            
            # Disable internal retries to handle them manually and avoid long hangs
            return spotipy.Spotify(auth_manager=auth_manager, requests_timeout=SPOTIFY_REQUEST_TIMEOUT, retries=0)
        except Exception as e:
            logger.error(f"Error creating OAuth client: {e}")
            # Fallback to old method
            try:
                token = util.prompt_for_user_token(
                    username,
                    scope,
                    client_id=settings.spotipy_client_id,
                    client_secret=settings.spotipy_client_secret,
                    redirect_uri=settings.spotipy_redirect_uri
                )
                if token:
                    return spotipy.Spotify(auth=token, requests_timeout=SPOTIFY_REQUEST_TIMEOUT, retries=0)
                else:
                    logger.error(f"Can't get token for {username}")
                    return None
            except Exception as e2:
                logger.error(f"Error getting token: {e2}")
                return None
    else:
        # Client Credentials
        client_credentials_manager = SpotifyClientCredentials(
            client_id=settings.spotipy_client_id,
            client_secret=settings.spotipy_client_secret
        )
        # Disable internal retries to handle them manually and avoid long hangs
        return spotipy.Spotify(client_credentials_manager=client_credentials_manager, requests_timeout=SPOTIFY_REQUEST_TIMEOUT, retries=0)

def confirm(prompt: Optional[str] = None, default: bool = False) -> bool:
    """
    Prompts for yes or no response from the user.
    Returns True for yes and False for no.
    
    If stdin is closed (e.g., after Ctrl+D), returns the default value.
    """
    if prompt is None:
        prompt = 'Create NEW playlist and add found tracks to it?'

    if default:
        prompt = '%s [%s]|%s: ' % (prompt, 'Y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'N', 'y')

    while True:
        try:
            ans = input(prompt).strip().lower()
            if not ans:
                return default
            if ans in ['y', 'yes']:
                return True
            if ans in ['n', 'no']:
                return False
            print('Please enter y or n.')
        except (EOFError, OSError):
            # stdin is closed or unavailable, use default
            return default

def create_playlist(username: str, playlist_name: str, public: bool = False, sp: Optional[spotipy.Spotify] = None) -> Optional[str]:
    """Create a playlist for a user."""
    if not sp:
        sp = get_spotify_client(username=username)
    if sp:
        try:
            playlist = _spotify_call(lambda: sp.user_playlist_create(username, playlist_name, public=public))
            return playlist['id']
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None
    return None

def change_playlist_name(username: str, playlist_id: str, new_name: str) -> None:
    """Changes a playlist's name."""
    sp = get_spotify_client(username=username)
    if sp:
        _spotify_call(lambda: sp.user_playlist_change_details(username, playlist_id, name=new_name))

def show_all_playlists(username: str) -> Optional[Dict[str, Any]]:
    """Returns all playlists for a user."""
    sp = get_spotify_client(username=username)
    if sp:
        return _spotify_call(lambda: sp.user_playlists(username))
    return None

def find_playlist_by_name(username: str, playlist_name: str) -> Optional[str]:
    """
    Finds a playlist ID by exact name match for a user.
    Searches through all playlists the user has access to, including Spotify-created ones.
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return None
    
    # Use current_user_playlists() to get ALL playlists including Spotify-created ones
    # This works better than user_playlists(username) for finding playlists like "Discover Weekly"
    try:
        playlists = _spotify_call(lambda: sp.current_user_playlists())
    except:
        # Fallback to user_playlists if current_user fails
        playlists = _spotify_call(lambda: sp.user_playlists(username))
    
    while playlists:
        for playlist in playlists['items']:
            if playlist['name'] == playlist_name:
                return playlist['id']
        
        if playlists['next']:
            playlists = _spotify_call(lambda: sp.next(playlists))
        else:
            playlists = None
    return None

def find_playlist_by_name_fuzzy(username: str, playlist_name: str) -> Optional[str]:
    """
    Finds a playlist ID with case-insensitive matching.
    First tries exact match, then falls back to case-insensitive.
    """
    # Try exact match first
    result = find_playlist_by_name(username, playlist_name)
    if result:
        return result
    
    # Try case-insensitive match
    sp = get_spotify_client(username=username)
    if not sp:
        return None
    
    search_name_lower = playlist_name.lower()
    
    try:
        playlists = _spotify_call(lambda: sp.current_user_playlists())
    except:
        playlists = _spotify_call(lambda: sp.user_playlists(username))
    
    while playlists:
        for playlist in playlists['items']:
            if playlist['name'].lower() == search_name_lower:
                return playlist['id']
        
        if playlists['next']:
            playlists = _spotify_call(lambda: sp.next(playlists))
        else:
            playlists = None
    return None

def get_playlist_track_ids(username, playlist_id):
    """Returns a set of track IDs currently in the playlist."""
    sp = get_spotify_client(username=username)
    if not sp:
        return set()
    
    track_ids = set()
    results = sp.user_playlist_tracks(username, playlist_id)
    while results:
        for item in results['items']:
            if item['track']:
                track_ids.add(item['track']['id'])
        
        if results['next']:
            results = sp.next(results)
        else:
            results = None
            
    return track_ids

def get_playlist_track_signatures(username, playlist_id):
    """
    Returns a set of track signatures (name, artist, duration) currently in the playlist.
    Used for fuzzy duplicate detection.
    Signature format: (track_name_lower, artist_name_lower, duration_ms_rounded)
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return set()
    
    signatures = set()
    results = sp.user_playlist_tracks(username, playlist_id)
    while results:
        for item in results['items']:
            if item['track']:
                track = item['track']
                name = track['name'].lower().strip()
                # Use first artist for signature
                artist = track['artists'][0]['name'].lower().strip() if track['artists'] else ""
                # Round duration to nearest second (DURATION_BUCKET_SIZE_MS) to account for minor variations
                duration = round(track['duration_ms'] / DURATION_BUCKET_SIZE_MS)
                
                signatures.add((name, artist, duration))
        
        if results['next']:
            results = sp.next(results)
        else:
            results = None
            
    return signatures

def create_track_signature(track: Dict[str, Any]) -> Tuple[str, str, int]:
    """Creates a signature tuple for a track object."""
    if advanced_signature:
        return advanced_signature(track)
        
    name = track['name'].lower().strip()
    artist = track['artists'][0]['name'].lower().strip() if track['artists'] else ""
    duration = round(track['duration_ms'] / DURATION_BUCKET_SIZE_MS)
    return (name, artist, duration)

def add_song_to_spotify_playlist(username: str, track_ids: List[str], playlist_id: str, sp: Optional[spotipy.Spotify] = None) -> None:
    """Adds songs to a playlist in Spotify."""
    if not sp:
        sp = get_spotify_client(username=username)
    if not sp:
        return

    # Spotify API limit is SPOTIFY_PLAYLIST_ADD_BATCH_SIZE tracks per request
    for i in range(0, len(track_ids), SPOTIFY_PLAYLIST_ADD_BATCH_SIZE):
        chunk = track_ids[i:i + SPOTIFY_PLAYLIST_ADD_BATCH_SIZE]
        try:
            sp.user_playlist_add_tracks(username, playlist_id, chunk)
        except Exception as e:
            logger.error(f"Error adding tracks: {e}")

def remove_song_from_spotify_playlist(username, track_id, playlist_id):
    """Removes a song from a playlist in Spotify."""
    sp = get_spotify_client(username=username)
    if sp:
        sp.user_playlist_remove_all_occurrences_of_tracks(username, playlist_id, [track_id])

def sync_playlists(username: str, source_playlist_id: str, target_playlist_id: str, 
                   remove_extra: bool = False, preserve_order: bool = False, 
                   dry_run: bool = False) -> Dict[str, Any]:
    """
    Sync target playlist with source playlist.
    
    Args:
        username: Spotify username
        source_playlist_id: ID of the source playlist to sync from
        target_playlist_id: ID of the target playlist to sync to
        remove_extra: If True, remove tracks from target that aren't in source
        preserve_order: If True, reorder target to match source order
        dry_run: If True, only report what would be done without making changes
    
    Returns:
        Dict with keys: 'to_add', 'to_remove', 'source_count', 'target_count'
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return {'error': 'Failed to initialize Spotify client'}
    
    # Fetch source playlist tracks
    source_tracks = []
    results = sp.user_playlist_tracks(username, source_playlist_id)
    while results:
        for item in results['items']:
            if item and item['track']:
                source_tracks.append(item['track'])
        if results['next']:
            results = sp.next(results)
        else:
            results = None
    
    # Fetch target playlist tracks
    target_tracks = []
    results = sp.user_playlist_tracks(username, target_playlist_id)
    while results:
        for item in results['items']:
            if item and item['track']:
                target_tracks.append(item['track'])
        if results['next']:
            results = sp.next(results)
        else:
            results = None
    
    # Create sets of track IDs for comparison
    source_ids = [t['id'] for t in source_tracks]
    target_ids = [t['id'] for t in target_tracks]
    
    source_id_set = set(source_ids)
    target_id_set = set(target_ids)
    
    # Find tracks to add (in source but not in target)
    to_add_ids = [tid for tid in source_ids if tid not in target_id_set]
    
    # Find tracks to remove (in target but not in source)
    to_remove_ids = [tid for tid in target_ids if tid not in source_id_set]
    
    result = {
        'source_count': len(source_tracks),
        'target_count': len(target_tracks),
        'to_add': to_add_ids,
        'to_remove': to_remove_ids,
        'to_add_tracks': [t for t in source_tracks if t['id'] in to_add_ids],
        'to_remove_tracks': [t for t in target_tracks if t['id'] in to_remove_ids]
    }
    
    if dry_run:
        return result
    
    # Add missing tracks
    if to_add_ids:
        logger.info(f"Adding {len(to_add_ids)} tracks to target playlist...")
        add_song_to_spotify_playlist(username, to_add_ids, target_playlist_id, sp=sp)
    
    # Remove extra tracks if requested
    if remove_extra and to_remove_ids:
        logger.info(f"Removing {len(to_remove_ids)} extra tracks from target playlist...")
        for i in range(0, len(to_remove_ids), 100):
            chunk = to_remove_ids[i:i+100]
            try:
                sp.user_playlist_remove_all_occurrences_of_tracks(username, target_playlist_id, chunk)
            except Exception as e:
                logger.error(f"Error removing tracks: {e}")
    
    # Reorder if requested and if tracks match
    if preserve_order and not to_add_ids and not (remove_extra and to_remove_ids):
        # Only reorder if playlists have same tracks (just in different order)
        if source_id_set == target_id_set:
            logger.info("Reordering target playlist to match source...")
            # Replace all tracks in the same order as source
            try:
                # Clear target playlist
                sp.user_playlist_replace_tracks(username, target_playlist_id, [])
                # Add all tracks in source order
                add_song_to_spotify_playlist(username, source_ids, target_playlist_id, sp=sp)
            except Exception as e:
                logger.error(f"Error reordering playlist: {e}")
    
    return result

def copy_missing_tracks_with_dedup(username: str, source_playlist_id: str, target_playlist_id: str,
                                   keep_best: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Copy missing tracks from source to target playlist and deduplicate the target.
    
    Args:
        username: Spotify username
        source_playlist_id: ID of the source playlist to copy from
        target_playlist_id: ID of the target playlist to copy to
        keep_best: Deduplication strategy ('popularity', 'explicit', 'clean', 'longest', 'shortest')
        dry_run: If True, only report what would be done without making changes
    
    Returns:
        Dict with keys: 'added_count', 'duplicates_removed', 'source_count', 'target_count_before', 'target_count_after'
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return {'error': 'Failed to initialize Spotify client'}
    
    # Fetch source playlist tracks
    source_tracks = []
    results = sp.user_playlist_tracks(username, source_playlist_id)
    while results:
        for item in results['items']:
            if item and item['track']:
                source_tracks.append(item['track'])
        if results['next']:
            results = sp.next(results)
        else:
            results = None
    
    # Fetch target playlist tracks (before adding)
    target_tracks_before = []
    results = sp.user_playlist_tracks(username, target_playlist_id)
    while results:
        for item in results['items']:
            if item and item['track']:
                target_tracks_before.append(item['track'])
        if results['next']:
            results = sp.next(results)
        else:
            results = None
    
    # Find tracks to add (by ID and signature to avoid duplicates)
    target_ids = set(t['id'] for t in target_tracks_before)
    target_signatures = set(create_track_signature(t) for t in target_tracks_before)
    
    to_add = []
    for track in source_tracks:
        # Check both ID and signature to avoid all types of duplicates
        if track['id'] not in target_ids:
            sig = create_track_signature(track)
            if sig not in target_signatures:
                to_add.append(track)
                # Add to sets to prevent duplicates within the batch
                target_ids.add(track['id'])
                target_signatures.add(sig)
    
    result = {
        'source_count': len(source_tracks),
        'target_count_before': len(target_tracks_before),
        'to_add': [t['id'] for t in to_add],
        'to_add_tracks': to_add,
        'duplicates_removed': 0,
        'target_count_after': len(target_tracks_before) + len(to_add)
    }
    
    if dry_run:
        # In dry run, estimate duplicates that would be found
        # Don't actually scan since we're not making changes
        return result
    
    # Add missing tracks
    if to_add:
        logger.info(f"Adding {len(to_add)} new tracks to target playlist...")
        to_add_ids = [t['id'] for t in to_add]
        add_song_to_spotify_playlist(username, to_add_ids, target_playlist_id, sp=sp)
    
    # Run deduplication on target playlist
    logger.info("Scanning for duplicates in target playlist...")
    duplicates = find_duplicates_in_playlist(username, target_playlist_id)
    
    if duplicates:
        logger.info(f"Found {len(duplicates)} duplicates.")
        
        # Apply keep-best logic if specified
        if keep_best:
            from collections import defaultdict
            dup_groups = defaultdict(list)
            
            for d in duplicates:
                orig = d['original']
                sig = create_track_signature(orig)
                dup_groups[sig].append(d)
            
            refined_duplicates = []
            
            for sig, group in dup_groups.items():
                # Get all versions (original + duplicates)
                all_versions = [g['original'] for g in group] + [g['duplicate'] for g in group]
                
                # Deduplicate by URI
                unique_versions = {}
                for v in all_versions:
                    unique_versions[v['uri']] = v
                
                versions = list(unique_versions.values())
                
                if len(versions) < 2:
                    continue
                
                # Select best based on criteria
                if keep_best == 'popularity':
                    best = max(versions, key=lambda t: t.get('popularity', 0))
                elif keep_best == 'explicit':
                    explicit_versions = [v for v in versions if v.get('explicit', False)]
                    if explicit_versions:
                        best = max(explicit_versions, key=lambda t: t.get('popularity', 0))
                    else:
                        best = max(versions, key=lambda t: t.get('popularity', 0))
                elif keep_best == 'clean':
                    clean_versions = [v for v in versions if not v.get('explicit', False)]
                    if clean_versions:
                        best = max(clean_versions, key=lambda t: t.get('popularity', 0))
                    else:
                        best = max(versions, key=lambda t: t.get('popularity', 0))
                elif keep_best == 'longest':
                    best = max(versions, key=lambda t: t.get('duration_ms', 0))
                elif keep_best == 'shortest':
                    best = min(versions, key=lambda t: t.get('duration_ms', float('inf')))
                else:
                    best = versions[0]
                
                # Mark all other versions as duplicates to remove
                for g in group:
                    if g['duplicate']['uri'] != best['uri']:
                        refined_duplicates.append(g)
            
            duplicates = refined_duplicates
        
        # Remove duplicates
        if duplicates:
            logger.info(f"Removing {len(duplicates)} duplicates...")
            removal_map = {}  # uri -> list of positions
            for d in duplicates:
                uri = d['duplicate']['uri']
                pos = d['position']
                if uri not in removal_map:
                    removal_map[uri] = []
                removal_map[uri].append(pos)
            
            tracks_to_remove = [{'uri': uri, 'positions': positions} for uri, positions in removal_map.items()]
            remove_specific_occurrences(username, target_playlist_id, tracks_to_remove)
            
            result['duplicates_removed'] = len(duplicates)
            result['target_count_after'] = result['target_count_after'] - len(duplicates)
    
    return result

def get_artist_info(sp, artist_spotify_id):
    """Returns all artist data: albums, songs, etc."""
    artist_albums = _spotify_call(lambda: sp.artist_albums(artist_spotify_id, country=COUNTRY))
    album_ids = []

    # Paginate through all albums to avoid truncation
    while artist_albums:
        album_ids.extend([album['id'] for album in artist_albums.get('items', [])])
        if artist_albums.get('next'):
            artist_albums = _spotify_call(lambda: sp.next(artist_albums))
        else:
            artist_albums = None

    full_artist_info = _spotify_call(lambda: sp.artist(artist_spotify_id)) or {}

    return {
        'album_data': get_album_info(sp, album_ids),
        'artist_data': parse_artists([full_artist_info])
    }

def parse_artists(all_artists):
    """Parses and returns data about artists into expected format."""
    artists = []
    for artist in all_artists:
        artists.append({
            'artist_spotify_id': artist['id'],
            'artist_name': artist['name'],
            'artist_url': artist['external_urls']['spotify']
        })
    return artists

def get_album_info(sp, album_spotify_ids):
    """Returns all data about albums."""
    all_results = []
    
    # Spotify allows fetching multiple albums (up to SPOTIFY_ALBUM_FETCH_BATCH_SIZE)
    # We should chunk if album_spotify_ids > SPOTIFY_ALBUM_FETCH_BATCH_SIZE
    for i in range(0, len(album_spotify_ids), SPOTIFY_ALBUM_FETCH_BATCH_SIZE):
        chunk = album_spotify_ids[i:i+SPOTIFY_ALBUM_FETCH_BATCH_SIZE]
        albums_resp = _spotify_call(lambda: sp.albums(chunk))
        if not albums_resp:
            continue
        albums = albums_resp

        for album_info in albums['albums']:
            album_data = parse_album(album_info)
            album_data['artists'] = parse_artists(album_info['artists'])

            track_results = {}
            for item in album_info['tracks']['items']:
                track_info = get_track_info(item, album_data)
                track_results[track_info['id']] = track_info

            all_results.append({
                'album_data': album_data,
                'track_results': track_results
            })

    return all_results

def parse_album(album_info):
    """Parses album info into expected format."""
    return {
        'album_spotify_id': album_info['id'],
        'album_name': album_info['name'],
        'album_url': album_info['external_urls']['spotify']
    }

def get_track_info(item, album_data=None):
    """Gets and returns all track info and parses it into expected format."""
    artists = parse_artists(item['artists'])

    if album_data is None:
        album_data = parse_album(item['album'])
    
    track_info = {
        'spotify_id': item['id'],
        'name': item['name'],
        'preview': item.get('preview_url'),
        'spotify_url': item['external_urls']['spotify'],
        'artists': artists
    }
    
    track_info.update(album_data)
    
    # Compatibility with original code structure
    track_info['id'] = track_info['spotify_id']

    return track_info

def search(sp, query, limit=50, offset=0, fetch_all=False):
    """Searches given users input and returns results.
    
    Args:
        sp: Spotify client
        query: Search query
        limit: Max number of results to return.
        offset: Starting offset
        fetch_all: If True, attempts to fetch ALL results, bypassing 1000 limit if needed.
    """
    all_results = {}
    tracks = []
    
    try:
        # Initial search to get total
        first_limit = 50 if (fetch_all or limit is None) else min(limit, 50)
        results = _spotify_call(lambda: sp.search(query, limit=first_limit, offset=offset, market=COUNTRY, type='track'))
        if not results:
            logger.error("Search failed to return results.")
            all_results['tracks'] = []
            return all_results
        items = results['tracks']['items']
        total = results['tracks']['total']
        tracks.extend(items)
        
        target_limit = limit if limit is not None else 50
        if fetch_all:
            target_limit = total
            
        # If we need more results
        if total > len(tracks) and (fetch_all or len(tracks) < target_limit):
            
            # STRATEGY 1: Year-based exhaustive search (if > 1000 results and fetch_all is True)
            if fetch_all and total >= 1000:
                logger.info(f"Query matches {total} tracks. Using year-based search to bypass 1000 limit...")
                # Reset tracks to ensure clean exhaustive list
                tracks = [] 
                seen_ids = set()
                
                current_year = datetime.datetime.now().year
                start_year = 1950 
                
                # Use a progress indicator if possible, or just log
                for year in range(start_year, current_year + 2):
                    year_query = f"{query} year:{year}"
                    year_offset = 0
                    
                    while True:
                        try:
                            yr = _spotify_call(lambda: sp.search(year_query, limit=50, offset=year_offset, market=COUNTRY, type='track'))
                            if not yr:
                                break
                            y_items = yr['tracks']['items']
                            if not y_items:
                                break
                            
                            for item in y_items:
                                if item['id'] not in seen_ids:
                                    seen_ids.add(item['id'])
                                    tracks.append(item)
                            
                            if yr['tracks']['next']:
                                year_offset += 50
                                # Safety break for year-specific 1000 limit (unlikely to hit for one year)
                                if year_offset >= 1000: 
                                    break
                            else:
                                break
                        except Exception as e:
                            # Rate limit or other error
                            logger.warning(f"Error searching year {year}: {e}")
                            break
            
            # STRATEGY 2: Standard Pagination (if < 1000 results or fetch_all=False)
            else:
                current_offset = offset + len(items)
                
                while len(tracks) < target_limit:
                    remaining = target_limit - len(tracks)
                    fetch_size = min(50, remaining)
                    
                    # Check 1000 limit
                    if current_offset + fetch_size > 1000:
                        fetch_size = 1000 - current_offset
                        if fetch_size <= 0:
                            if fetch_all:
                                logger.warning("Reached Spotify's 1000 item limit. Use year-specific queries to get more.")
                            break
                    
                    res = _spotify_call(lambda: sp.search(query, limit=fetch_size, offset=current_offset, market=COUNTRY, type='track'))
                    if not res:
                        break
                    new_items = res['tracks']['items']
                    if not new_items:
                        break
                        
                    tracks.extend(new_items)
                    current_offset += len(new_items)
                    
                    if res['tracks']['total'] <= current_offset:
                        break
                        
    except Exception as e:
        logger.error(f"Search error: {e}")
        
    all_results['tracks'] = tracks
    return all_results

def search_tracks_by_year(sp, label, year, market='US'):
    """
    Helper to search for tracks in a specific year.
    """
    query = f"label:\"{label}\" year:{year}"
    tracks = []
    try:
        results = _spotify_call(lambda: sp.search(query, limit=50, type='track', market=market))
        if not results:
            return []

        page = results['tracks']
        tracks.extend(page.get('items', []))
        while page.get('next'):
            page = _spotify_call(lambda: sp.next(page))
            if not page:
                break
            tracks.extend(page.get('items', []))
    except Exception as e:
        logger.error(f"Error searching year {year}: {e}")
    
    return tracks

def search_tracks_exhaustive(sp, label, market='US'):
    """
    Searches for tracks by label, iterating through years to bypass the 1000-item search limit.
    """
    # Use %s formatting to avoid issues with special characters like $ in label names
    logger.info("Searching for tracks by label: '%s' (Exhaustive Search)...", label)
    
    all_tracks = []
    seen_ids = set()
    
    current_year = datetime.datetime.now().year
    start_year = 1950 # Adjust if needed
    
    for year in range(start_year, current_year + 2):
        # logger.info(f"Scanning year {year}...") # Too verbose for info?
        print(f"Scanning year {year}...", end='\r') # Keep print for progress bar effect
        year_tracks = search_tracks_by_year(sp, label, year, market)
        
        for track in year_tracks:
            if track['id'] not in seen_ids:
                seen_ids.add(track['id'])
                all_tracks.append(track)
                
    logger.info(f"\nFinished scanning. Found {len(all_tracks)} unique tracks.")
    return all_tracks


def _build_label_map(sp, album_to_tracks):
    """Build map of label -> associated tracks based on album metadata."""
    label_map = {}
    album_ids = list(album_to_tracks.keys())

    for i in range(0, len(album_ids), 20):
        chunk = album_ids[i:i+20]
        albums_resp = _spotify_call(lambda: sp.albums(chunk))
        if not albums_resp:
            continue

        for album in albums_resp.get('albums', []):
            if not album:
                continue

            label = album.get('label')
            if label not in label_map:
                label_map[label] = []

            if album.get('id') in album_to_tracks:
                label_map[label].extend(album_to_tracks[album['id']])

    return label_map

def validate_tracks_list(sp, tracks, target_label, selection=None, auto_mode=False):
    """
    Validates a list of track objects against a target label.
    Returns a list of track IDs to KEEP.

    selection (optional) controls non-interactive mode:
    - None: prompt user (default, preserves current behavior)
    - 'none': keep all
    - 'suggested': exclude suggested mismatches
    - list/tuple/set of indices (1-based) to exclude
    - ('keep', [indices]) to keep only specified labels
    
    auto_mode: if True, automatically excludes all non-strict matches (quality < 2)
    """
    logger.info(f"Validating {len(tracks)} tracks against label '{target_label}'...")

    # Map album_id -> list of track objects
    album_to_tracks = {}
    for track in tracks:
        if not track or 'album' not in track: continue
        album_id = track['album']['id']
        if album_id not in album_to_tracks:
            album_to_tracks[album_id] = []
        album_to_tracks[album_id].append(track)

    logger.info(f"Fetching details for {len(album_to_tracks)} albums...")
    label_map = _build_label_map(sp, album_to_tracks)

    # Interactive Validation
    if not label_map:
        logger.info("No labels found.")
        return [t['id'] for t in tracks]

    # Sort labels by track count (descending)
    sorted_labels = sorted(label_map.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Identify likely mismatches for suggestion
    suggested_indices = []
    label_quality = []  # cache qualities for reuse in display/selection

    # Helper to determine match quality
    def get_match_quality(lbl, target):
        import re
        
        # Normalize: lowercase, strip, remove common punctuation
        def normalize(s):
            s = s.lower().strip()
            # Remove apostrophes, quotes, periods, commas
            s = re.sub(r"['\".,-]", "", s)
            # Collapse multiple spaces to single space
            s = re.sub(r'\s+', ' ', s)
            return s
        
        n_lbl = normalize(lbl or "")
        n_target = normalize(target)
        
        if n_lbl == n_target: return 2 # Exact (after normalization)
        if n_lbl.startswith(n_target + " ") or n_lbl.startswith(n_target + "/") or n_lbl.startswith(n_target + "-"): return 2 # Strict Prefix
        if n_target in n_lbl: return 1 # Loose Substring
        return 0 # No match

    for i, (label, lbl_tracks) in enumerate(sorted_labels):
        quality = get_match_quality(label, target_label)
        label_quality.append(quality)
        if quality < 2:
            suggested_indices.append(str(i + 1))

    indices_to_exclude = []
    
    # Auto mode: exclude all non-strict matches (quality < 2) silently
    if auto_mode:
        indices_to_exclude = [i for i, quality in enumerate(label_quality) if quality < 2]
        if indices_to_exclude:
            excluded_count = sum(len(sorted_labels[i][1]) for i in indices_to_exclude)
            logger.info(f"Auto-validation: excluding {len(indices_to_exclude)} non-matching labels ({excluded_count} tracks)")
        
        # Jump to the return statement
        tracks_to_keep = []
        for i, (label, lbl_tracks) in enumerate(sorted_labels):
            if i not in indices_to_exclude:
                tracks_to_keep.extend([t['id'] for t in lbl_tracks])
        return tracks_to_keep

    def _collect_selection():
        if selection is None:
            if console:
                table = Table(title=f"Found {len(sorted_labels)} Unique Labels")
                table.add_column("#", justify="right", style="cyan", no_wrap=True)
                table.add_column("Match", justify="center")
                table.add_column("Label Name", style="magenta")
                table.add_column("Tracks", justify="right", style="green")

                for i, (label, lbl_tracks) in enumerate(sorted_labels):
                    quality = label_quality[i]

                    if quality == 2:
                        match_status = "[green]✔[/green]"
                    elif quality == 1:
                        match_status = "[yellow]~[/yellow]"
                    else:
                        match_status = "[red]✘[/red]"

                    table.add_row(str(i + 1), match_status, label, str(len(lbl_tracks)))

                console.print(table)
                console.print(f"\n[bold]Suggested to remove (marked [red]✘[/red] and [yellow]~[/yellow]):[/bold] [yellow]{', '.join(suggested_indices) if suggested_indices else 'None'}[/yellow]")
            else:
                print(f"\n--- Found {len(sorted_labels)} unique labels ---")

                for i, (label, lbl_tracks) in enumerate(sorted_labels):
                    quality = label_quality[i]

                    if quality == 2:
                        match_status = "✔"
                    elif quality == 1:
                        match_status = "~"
                    else:
                        match_status = "X"

                    print(f"{i+1:3}. [{match_status}] {label} ({len(lbl_tracks)} tracks)")

                print("\nLegend: [✔] = Strict Match, [~] = Loose Match, [X] = No Match")
                print(f"Suggested to remove (marked X and ~): {', '.join(suggested_indices) if suggested_indices else 'None'}")

            print("\nOptions:")
            print(" - Enter numbers to EXCLUDE (e.g. '1, 5').")
            print(" - Type 'suggested' to exclude all [X] and [~] labels (Strict Mode).")
            print(" - Type 'keep 1,2' to KEEP ONLY specific labels.")
            print(" - Type 'none' or press Enter to keep ALL.")

            return input("> ").strip().lower()
        # Non-interactive selection provided
        if isinstance(selection, str):
            return selection.strip().lower()
        if isinstance(selection, tuple) and len(selection) == 2 and selection[0] == 'keep':
            return f"keep {','.join([str(x) for x in selection[1]])}"
        if isinstance(selection, (list, tuple, set)):
            return ','.join([str(x) for x in selection])
        return ''

    choice = _collect_selection()

    if choice.startswith('keep '):
        try:
            parts = choice[5:].split(',')
            keepers = []
            for p in parts:
                if p.strip():
                    idx = int(p.strip()) - 1
                    if 0 <= idx < len(sorted_labels):
                        keepers.append(idx)

            indices_to_exclude = [i for i in range(len(sorted_labels)) if i not in keepers]
            logger.info(f"Keeping {len(keepers)} labels, excluding {len(indices_to_exclude)}.")
        except ValueError:
            logger.error("Invalid input. Operation cancelled.")
            return None

    elif choice == 'none':
        logger.info("Excluding all tracks.")
        return []
    elif choice == '':
        logger.info("Keeping all tracks.")
        return [t['id'] for t in tracks]
    elif choice == 'suggested':
        indices_to_exclude = [int(x) - 1 for x in suggested_indices]
    else:
        try:
            parts = choice.split(',') if choice else []
            for p in parts:
                if p.strip():
                    idx = int(p.strip()) - 1
                    if 0 <= idx < len(sorted_labels):
                        indices_to_exclude.append(idx)
        except ValueError:
            logger.error("Invalid input. Operation cancelled.")
            return None

    if not indices_to_exclude:
        logger.info("No labels selected for exclusion.")
        return [t['id'] for t in tracks]

    # Collect track IDs to keep
    tracks_to_keep = []
    excluded_count = 0
    
    for i, (label, lbl_tracks) in enumerate(sorted_labels):
        if i in indices_to_exclude:
            excluded_count += len(lbl_tracks)
        else:
            tracks_to_keep.extend([t['id'] for t in lbl_tracks])
            
    logger.info(f"Excluded {excluded_count} tracks based on label validation.")
    return tracks_to_keep

def fetch_all_user_playlists(username):
    """
    Fetches ALL playlists for a user (handling pagination).
    Includes both user-created and Spotify-created playlists (like Discover Weekly).
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return []
    
    playlists = []
    
    # Try current_user_playlists first (gets all playlists including Spotify-created)
    try:
        results = sp.current_user_playlists()
    except:
        # Fallback to user_playlists if current_user fails
        results = sp.user_playlists(username)
    
    while results:
        playlists.extend(results['items'])
        if results['next']:
            time.sleep(0.2) # Rate limit protection
            results = sp.next(results)
        else:
            results = None
    return playlists

def find_duplicates_in_playlist(username, playlist_id):
    """
    Scans a playlist for duplicates based on metadata (Name, Artist, Duration).
    Returns a list of dicts: {'duplicate': track_obj, 'original': track_obj, 'position': int}
    """
    sp = get_spotify_client(username=username)
    if not sp:
        return []

    seen_signatures = {} # (name, artist, duration) -> track_obj
    duplicates = []

    # We need to handle pagination manually to process all tracks
    results = sp.user_playlist_tracks(username, playlist_id)
    position = 0
    page_count = 0
    
    while results:
        for item in results['items']:
            if not item['track']: 
                position += 1
                continue
            
            track = item['track']
            
            # Create signature
            sig = create_track_signature(track)
            
            if sig in seen_signatures:
                duplicates.append({
                    'duplicate': track,
                    'original': seen_signatures[sig],
                    'position': position
                })
            else:
                seen_signatures[sig] = track
            
            position += 1
        
        if results['next']:
            page_count += 1
            # Add delay between pagination requests to avoid rate limiting
            # Increase delay every 10 pages to be extra cautious
            if page_count % 10 == 0:
                time.sleep(0.5)
            else:
                time.sleep(0.1)
            
            try:
                results = sp.next(results)
            except Exception as e:
                # If we hit rate limit, log and return what we have
                if '429' in str(e):
                    logger.warning(f"Rate limit hit while scanning playlist. Processed {position} tracks.")
                    return duplicates
                raise
        else:
            results = None
            
    return duplicates

def remove_specific_occurrences(username, playlist_id, tracks_with_positions, progress_callback=None):
    """
    Removes specific occurrences of tracks from a playlist.
    tracks_with_positions: list of {'uri': str, 'positions': [int]}
    progress_callback: function(count) called after each chunk is removed
    """
    sp = get_spotify_client(username=username, scope="playlist-modify-private playlist-modify-public")
    if not sp:
        return

    # Spotify allows removing max 100 items per request
    # The structure for remove_specific_occurrences_of_tracks is:
    # { "tracks": [{ "uri": "spotify:track:...", "positions": [0, 2] }] }
    # But the spotipy method takes a list of items directly.
    
    # Group by URI for efficiency (though positions are unique per call usually)
    # Actually, spotipy's user_playlist_remove_specific_occurrences_of_tracks takes:
    # playlist_id, tracks
    # where tracks is a list of dicts like: { "uri": "...", "positions": [0, 2] }
    
    # Let's process in chunks of 100 items
    for i in range(0, len(tracks_with_positions), 100):
        chunk = tracks_with_positions[i:i+100]
        try:
            _spotify_call(lambda: sp.user_playlist_remove_specific_occurrences_of_tracks(username, playlist_id, chunk))
            if progress_callback:
                progress_callback(len(chunk))
            else:
                logger.debug(f"Removed chunk {i//100 + 1}...")
        except Exception as e:
            logger.error(f"Error removing tracks: {e}")

def validate_playlist_tracks(sp, playlist_id, username, target_label, selection=None):
    """
    Validates that tracks in the playlist belong to the target label.
    Removes incorrect tracks.
    selection: optional non-interactive choice (same semantics as validate_tracks_list)
    """
    logger.info("Validating playlist tracks against label...")
    
    # 1. Fetch all tracks from the playlist
    playlist_tracks = []
    results = sp.user_playlist_tracks(username, playlist_id)
    while results:
        playlist_tracks.extend(results['items'])
        if results['next']:
            results = sp.next(results)
        else:
            results = None
            
    if not playlist_tracks:
        logger.info("Playlist is empty.")
        return

    # 2. Collect Album IDs
    # Map album_id -> list of track_ids (to remove if album is invalid)
    album_to_tracks = {}
    for item in playlist_tracks:
        if not item['track']: continue
        track = item['track']
        album_id = track['album']['id']
        if album_id not in album_to_tracks:
            album_to_tracks[album_id] = []
        album_to_tracks[album_id].append(track['id'])

    logger.info(f"Fetching details for {len(album_to_tracks)} albums...")
    label_map = _build_label_map(sp, album_to_tracks)

    # 4. Interactive Validation
    if not label_map:
        logger.info("No labels found.")
        return

    # Sort labels by track count (descending)
    sorted_labels = sorted(label_map.items(), key=lambda x: len(x[1]), reverse=True)
    
    print(f"\n--- Found {len(sorted_labels)} unique labels ---")

    # Identify likely mismatches for suggestion
    suggested_indices = []

    for i, (label, tracks) in enumerate(sorted_labels):
        label_val = label or ""
        is_match = target_label.lower() in label_val.lower()
        match_status = " " if is_match else "X"

        if not is_match:
            suggested_indices.append(str(i + 1))

        print(f"{i+1:3}. [{match_status}] {label_val} ({len(tracks)} tracks)")

    print("\nLegend: [ ] = Contains search term, [X] = Does not contain search term")
    print(f"Suggested to remove (marked X): {', '.join(suggested_indices) if suggested_indices else 'None'}")

    indices_to_remove = []

    if selection is None:
        print("\nEnter the numbers of the labels you want to REMOVE (comma separated, e.g. '1, 5, 10').")
        print("Type 'suggested' to remove all [X] labels.")
        print("Type 'none' or press Enter to keep all.")
        choice = input("> ").strip().lower()
    else:
        if isinstance(selection, str):
            choice = selection.strip().lower()
        elif isinstance(selection, (list, tuple, set)):
            choice = ','.join([str(x) for x in selection])
        else:
            choice = ''

    if choice in ('none', ''):
        logger.info("Keeping all tracks.")
        return
    elif choice == 'suggested':
        indices_to_remove = [int(x) - 1 for x in suggested_indices]
    else:
        try:
            parts = choice.split(',') if choice else []
            for p in parts:
                if p.strip():
                    idx = int(p.strip()) - 1
                    if 0 <= idx < len(sorted_labels):
                        indices_to_remove.append(idx)
        except ValueError:
            logger.error("Invalid input. Operation cancelled.")
            return

    if not indices_to_remove:
        logger.info("No labels selected for removal.")
        return

    # Collect track IDs to remove
    tracks_to_remove = []
    print("\nRemoving tracks for labels:")
    for idx in indices_to_remove:
        label, t_ids = sorted_labels[idx]
        print(f"- {label}")
        tracks_to_remove.extend(t_ids)
        
    logger.info(f"\nTotal tracks to remove: {len(tracks_to_remove)}")
    
    if confirm("Proceed with removal?"):
        # We need an authenticated client to modify the playlist
        sp_auth = get_spotify_client(username=username)
        
        if not sp_auth:
            logger.error("Error: Could not authenticate to remove tracks.")
            return

        # Remove in chunks of 100
        for i in range(0, len(tracks_to_remove), 100):
            chunk = tracks_to_remove[i:i+100]
            try:
                sp_auth.user_playlist_remove_all_occurrences_of_tracks(username, playlist_id, chunk)
                logger.info(f"Removed chunk {i//100 + 1}...")
            except Exception as e:
                logger.error(f"Error removing tracks: {e}")
        logger.info("Cleanup complete.")
    else:
        logger.info("Removal cancelled.")
