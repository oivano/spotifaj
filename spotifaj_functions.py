"""
Helper functions for Spotify interactions.
"""

import datetime
import time
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
from config import settings, logger

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
    "user-library-read"
)

COUNTRY = 'US'

def get_spotify_client(username=None, scope=DEFAULT_SCOPE):
    """
    Returns an authenticated spotipy.Spotify client.
    
    If username is provided, attempts to use User Authorization (OAuth).
    Otherwise, falls back to Client Credentials (public data only).
    """
    if username:
        # User Authorization
        try:
            token = util.prompt_for_user_token(
                username,
                scope,
                client_id=settings.spotipy_client_id,
                client_secret=settings.spotipy_client_secret,
                redirect_uri=settings.spotipy_redirect_uri
            )
            if token:
                # Disable internal retries to handle them manually and avoid long hangs
                return spotipy.Spotify(auth=token, requests_timeout=20, retries=0)
            else:
                logger.error(f"Can't get token for {username}")
                return None
        except Exception as e:
            logger.error(f"Error getting token: {e}")
            return None
    else:
        # Client Credentials
        client_credentials_manager = SpotifyClientCredentials(
            client_id=settings.spotipy_client_id,
            client_secret=settings.spotipy_client_secret
        )
        # Disable internal retries to handle them manually and avoid long hangs
        return spotipy.Spotify(client_credentials_manager=client_credentials_manager, requests_timeout=20, retries=0)

def confirm(prompt=None, default=False):
    """
    Prompts for yes or no response from the user.
    Returns True for yes and False for no.
    """
    if prompt is None:
        prompt = 'Create NEW playlist and add found tracks to it?'

    if default:
        prompt = '%s [%s]|%s: ' % (prompt, 'Y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'N', 'y')

    while True:
        ans = input(prompt).strip().lower()
        if not ans:
            return default
        if ans in ['y', 'yes']:
            return True
        if ans in ['n', 'no']:
            return False
        print('Please enter y or n.')

def create_playlist(username, playlist_name, public=False):
    """Create a playlist for a user."""
    sp = get_spotify_client(username=username)
    if sp:
        try:
            playlist = sp.user_playlist_create(username, playlist_name, public=public)
            return playlist['id']
        except Exception as e:
            logger.error(f"Error creating playlist: {e}")
            return None
    return None

def change_playlist_name(username, playlist_id, new_name):
    """Changes a playlist's name."""
    sp = get_spotify_client(username=username)
    if sp:
        sp.user_playlist_change_details(username, playlist_id, name=new_name)

def show_all_playlists(username):
    """Returns all playlists for a user."""
    sp = get_spotify_client(username=username)
    if sp:
        return sp.user_playlists(username)
    return None

def find_playlist_by_name(username, playlist_name):
    """Finds a playlist ID by exact name match for a user."""
    sp = get_spotify_client(username=username)
    if not sp:
        return None
    
    playlists = sp.user_playlists(username)
    while playlists:
        for playlist in playlists['items']:
            if playlist['name'] == playlist_name:
                return playlist['id']
        
        if playlists['next']:
            playlists = sp.next(playlists)
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
                # Round duration to nearest second (1000ms) to account for minor variations
                duration = round(track['duration_ms'] / 1000)
                
                signatures.add((name, artist, duration))
        
        if results['next']:
            results = sp.next(results)
        else:
            results = None
            
    return signatures

def create_track_signature(track):
    """Creates a signature tuple for a track object."""
    if advanced_signature:
        return advanced_signature(track)
        
    name = track['name'].lower().strip()
    artist = track['artists'][0]['name'].lower().strip() if track['artists'] else ""
    duration = round(track['duration_ms'] / 1000)
    return (name, artist, duration)

def add_song_to_spotify_playlist(username, track_ids, playlist_id):
    """Adds songs to a playlist in Spotify."""
    sp = get_spotify_client(username=username)
    if not sp:
        return

    # Spotify API limit is 100 tracks per request
    for i in range(0, len(track_ids), 100):
        chunk = track_ids[i:i + 100]
        try:
            sp.user_playlist_add_tracks(username, playlist_id, chunk)
        except Exception as e:
            logger.error(f"Error adding tracks: {e}")

def remove_song_from_spotify_playlist(username, track_id, playlist_id):
    """Removes a song from a playlist in Spotify."""
    sp = get_spotify_client(username=username)
    if sp:
        sp.user_playlist_remove_all_occurrences_of_tracks(username, playlist_id, [track_id])

def get_artist_info(sp, artist_spotify_id):
    """Returns all artist data: albums, songs, etc."""
    artist_albums = sp.artist_albums(artist_spotify_id, country=COUNTRY)
    album_ids = [album['id'] for album in artist_albums['items']]
    
    full_artist_info = sp.artist(artist_spotify_id)

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
    
    # Spotify allows fetching multiple albums (up to 20)
    # We should chunk if album_spotify_ids > 20
    for i in range(0, len(album_spotify_ids), 20):
        chunk = album_spotify_ids[i:i+20]
        albums = sp.albums(chunk)

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
        results = sp.search(query, limit=first_limit, offset=offset, market=COUNTRY, type='track')
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
                            yr = sp.search(year_query, limit=50, offset=year_offset, market=COUNTRY, type='track')
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
                    
                    res = sp.search(query, limit=fetch_size, offset=current_offset, market=COUNTRY, type='track')
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
        results = sp.search(q=query, limit=50, type='track', market=market)
        if not results:
            return []
        
        tracks.extend(results['tracks']['items'])
        while results['tracks']['next']:
            results = sp.next(results['tracks'])
            tracks.extend(results['tracks']['items'])
    except Exception as e:
        logger.error(f"Error searching year {year}: {e}")
    
    return tracks

def search_tracks_exhaustive(sp, label, market='US'):
    """
    Searches for tracks by label, iterating through years to bypass the 1000-item search limit.
    """
    logger.info(f"Searching for tracks by label: '{label}' (Exhaustive Search)...")
    
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

def validate_tracks_list(sp, tracks, target_label):
    """
    Validates a list of track objects against a target label.
    Returns a list of track IDs to KEEP.
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

    # Fetch Full Album Details (to get label)
    album_ids = list(album_to_tracks.keys())
    
    # Map: label_name -> list of track objects
    label_map = {}
    
    logger.info(f"Fetching details for {len(album_ids)} albums...")
    
    # Spotify allows fetching 20 albums at a time
    for i in range(0, len(album_ids), 20):
        chunk = album_ids[i:i+20]
        try:
            albums = sp.albums(chunk)['albums']
            for album in albums:
                if not album: continue
                
                label = album['label']
                if label not in label_map:
                    label_map[label] = []
                
                # Add all tracks associated with this album
                if album['id'] in album_to_tracks:
                    label_map[label].extend(album_to_tracks[album['id']])
                    
        except Exception as e:
            logger.error(f"Error fetching albums: {e}")

    # Interactive Validation
    if not label_map:
        logger.info("No labels found.")
        return [t['id'] for t in tracks]

    # Sort labels by track count (descending)
    sorted_labels = sorted(label_map.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Identify likely mismatches for suggestion
    suggested_indices = []
    
    # Helper to determine match quality
    def get_match_quality(lbl, target):
        n_lbl = lbl.lower().strip()
        n_target = target.lower().strip()
        
        if n_lbl == n_target: return 2 # Exact
        if n_lbl.startswith(n_target + " ") or n_lbl.startswith(n_target + "/") or n_lbl.startswith(n_target + "-"): return 2 # Strict Prefix
        if n_target in n_lbl: return 1 # Loose Substring
        return 0 # No match

    if console:
        table = Table(title=f"Found {len(sorted_labels)} Unique Labels")
        table.add_column("#", justify="right", style="cyan", no_wrap=True)
        table.add_column("Match", justify="center")
        table.add_column("Label Name", style="magenta")
        table.add_column("Tracks", justify="right", style="green")

        for i, (label, lbl_tracks) in enumerate(sorted_labels):
            quality = get_match_quality(label, target_label)
            
            if quality == 2:
                match_status = "[green]✔[/green]"
            elif quality == 1:
                match_status = "[yellow]~[/yellow]"
                suggested_indices.append(str(i + 1)) # Suggest removing loose matches by default
            else:
                match_status = "[red]✘[/red]"
                suggested_indices.append(str(i + 1))
            
            table.add_row(str(i + 1), match_status, label, str(len(lbl_tracks)))
        
        console.print(table)
        console.print(f"\n[bold]Suggested to remove (marked [red]✘[/red] and [yellow]~[/yellow]):[/bold] [yellow]{', '.join(suggested_indices) if suggested_indices else 'None'}[/yellow]")
    else:
        print(f"\n--- Found {len(sorted_labels)} unique labels ---")
        
        for i, (label, lbl_tracks) in enumerate(sorted_labels):
            quality = get_match_quality(label, target_label)
            
            if quality == 2:
                match_status = "✔"
            elif quality == 1:
                match_status = "~"
                suggested_indices.append(str(i + 1))
            else:
                match_status = "X"
                suggested_indices.append(str(i + 1))
                
            print(f"{i+1:3}. [{match_status}] {label} ({len(lbl_tracks)} tracks)")

        print("\nLegend: [✔] = Strict Match, [~] = Loose Match, [X] = No Match")
        print(f"Suggested to remove (marked X and ~): {', '.join(suggested_indices) if suggested_indices else 'None'}")
    
    print("\nOptions:")
    print(" - Enter numbers to EXCLUDE (e.g. '1, 5').")
    print(" - Type 'suggested' to exclude all [X] and [~] labels (Strict Mode).")
    print(" - Type 'keep 1,2' to KEEP ONLY specific labels.")
    print(" - Type 'none' or press Enter to keep ALL.")
    
    choice = input("> ").strip().lower()
    
    indices_to_exclude = []
    
    if choice.startswith('keep '):
        # Invert selection: User lists what to KEEP
        try:
            parts = choice[5:].split(',')
            keepers = []
            for p in parts:
                if p.strip():
                    idx = int(p.strip()) - 1
                    if 0 <= idx < len(sorted_labels):
                        keepers.append(idx)
            
            # Exclude everything NOT in keepers
            indices_to_exclude = [i for i in range(len(sorted_labels)) if i not in keepers]
            logger.info(f"Keeping {len(keepers)} labels, excluding {len(indices_to_exclude)}.")
        except ValueError:
            logger.error("Invalid input. Operation cancelled.")
            return None

    elif choice in ('none', ''):
        logger.info("Keeping all tracks.")
        return [t['id'] for t in tracks]
    elif choice == 'suggested':
        indices_to_exclude = [int(x) - 1 for x in suggested_indices]
    else:
        try:
            parts = choice.split(',')
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
    """Fetches ALL playlists for a user (handling pagination)."""
    sp = get_spotify_client(username=username)
    if not sp:
        return []
    
    playlists = []
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
            results = sp.next(results)
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
            sp.user_playlist_remove_specific_occurrences_of_tracks(username, playlist_id, chunk)
            if progress_callback:
                progress_callback(len(chunk))
            else:
                logger.debug(f"Removed chunk {i//100 + 1}...")
        except Exception as e:
            logger.error(f"Error removing tracks: {e}")

def validate_playlist_tracks(sp, playlist_id, username, target_label):
    """
    Validates that tracks in the playlist belong to the target label.
    Removes incorrect tracks.
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

    # 3. Fetch Full Album Details (to get label)
    album_ids = list(album_to_tracks.keys())
    
    # Map: label_name -> list of track_ids
    label_map = {}
    
    logger.info(f"Fetching details for {len(album_ids)} albums...")
    
    # Spotify allows fetching 20 albums at a time
    for i in range(0, len(album_ids), 20):
        chunk = album_ids[i:i+20]
        try:
            albums = sp.albums(chunk)['albums']
            for album in albums:
                if not album: continue
                
                label = album['label']
                if label not in label_map:
                    label_map[label] = []
                
                # Add all tracks associated with this album
                if album['id'] in album_to_tracks:
                    label_map[label].extend(album_to_tracks[album['id']])
                    
        except Exception as e:
            logger.error(f"Error fetching albums: {e}")

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
        # Check if target label is in the label name (case-insensitive)
        is_match = target_label.lower() in label.lower()
        match_status = " " if is_match else "X" # Mark mismatches with X
        
        if not is_match:
            suggested_indices.append(str(i + 1))
            
        print(f"{i+1:3}. [{match_status}] {label} ({len(tracks)} tracks)")

    print("\nLegend: [ ] = Contains search term, [X] = Does not contain search term")
    print(f"Suggested to remove (marked X): {', '.join(suggested_indices) if suggested_indices else 'None'}")
    
    print("\nEnter the numbers of the labels you want to REMOVE (comma separated, e.g. '1, 5, 10').")
    print("Type 'suggested' to remove all [X] labels.")
    print("Type 'none' or press Enter to keep all.")
    
    choice = input("> ").strip().lower()
    
    indices_to_remove = []
    if choice in ('none', ''):
        logger.info("Keeping all tracks.")
        return
    elif choice == 'suggested':
        indices_to_remove = [int(x) - 1 for x in suggested_indices]
    else:
        try:
            parts = choice.split(',')
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
