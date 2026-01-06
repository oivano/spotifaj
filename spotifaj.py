#!/usr/bin/env python3
"""
Spotify CLI - Unified entry point for Spotify tools.
"""

import click
import sys
import os
import re
import time
import base64
import requests
import spotipy
from io import BytesIO
from difflib import SequenceMatcher
from rich.console import Console
from rich.markup import escape
from rich.table import Table, Column
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from config import settings, logger
from constants import (
    CONFIDENCE_THRESHOLD_AUTO_ACCEPT,
    SIMILARITY_THRESHOLD_ARTIST_INTENT,
    SIMILARITY_THRESHOLD_ALBUM_INTENT,
    SPOTIFY_SEARCH_DEFAULT_LIMIT,
    SPOTIFY_SEARCH_RESULT_LIMIT,
    DEFAULT_DISPLAY_LIMIT,
    SPOTIFY_DEFAULT_DELAY,
)
import spotifaj_functions
from fuzzywuzzy import fuzz

# Import new modules
try:
    from workflows.discogs_workflow import DiscogsLabelWorkflow
    from clients.discogs_client import get_discogs_client
    from utils.cache_manager import CacheManager
    from utils.track_deduplicator import deduplicate_tracks, generate_track_signature
    from utils.track_confidence_scorer import TrackConfidenceScorer
except ImportError as e:
    logger.warning(f"Could not import advanced modules: {e}")
    DiscogsLabelWorkflow = None
    TrackConfidenceScorer = None

console = Console()
__version__ = "0.0.3"

@click.group()
@click.version_option(__version__)
def spotifaj():
    """Spotify CLI tool."""
    pass

@spotifaj.command()
@click.argument('label')
@click.option('--username', default=settings.spotipy_username, help="Spotify username for playlist creation.")
@click.option('--playlist', help="Name of the playlist to create. Defaults to label name.")
@click.option('--exhaustive', is_flag=True, help="Perform an exhaustive search by year (slower, but finds >1000 tracks).")
@click.option('--year', help="Search for tracks in a specific year, range (YYYY-YYYY), or 'all' for exhaustive search.")
@click.option('--validate', is_flag=True, help="Validate playlist tracks after creation.")
@click.option('--min-confidence', default=70, type=int, help="Minimum confidence score (0-100) for automatic verification. Default: 70")
@click.option('--no-verify', is_flag=True, help="Skip automatic confidence verification (faster but less accurate).")
def search_label(label, username, playlist, exhaustive, year, validate, min_confidence, no_verify):
    """Search for tracks by label and optionally add to a playlist."""
    
    if year and str(year).lower() == 'all':
        exhaustive = True
        year = None

    playlist_name = playlist if playlist else label
    if year:
        playlist_name = f"{playlist_name} ({year})"
    
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    if year:
        if '-' in str(year):
            try:
                start_str, end_str = year.split('-')
                start_year = int(start_str)
                end_year = int(end_str)
                
                logger.info("Searching for label: '%s' in range %d-%d...", label, start_year, end_year)
                found_tracks = []
                seen_ids = set()
                
                for y in range(start_year, end_year + 1):
                    print(f"Scanning year {y}...", end='\r')
                    tracks = spotifaj_functions.search_tracks_by_year(sp, label, y)
                    for track in tracks:
                        if track['id'] not in seen_ids:
                            seen_ids.add(track['id'])
                            found_tracks.append(track)
                print() # Clear progress line
            except ValueError:
                logger.error(f"Invalid year range: {year}. Use format YYYY-YYYY.")
                sys.exit(1)
        else:
            try:
                year_int = int(year)
                logger.info("Searching for label: '%s' in year %d...", label, year_int)
                found_tracks = spotifaj_functions.search_tracks_by_year(sp, label, year_int)
            except ValueError:
                logger.error(f"Invalid year: {year}. Use a 4-digit year, range YYYY-YYYY, or 'all'.")
                sys.exit(1)
    elif exhaustive:
        found_tracks = spotifaj_functions.search_tracks_exhaustive(sp, label)
    else:
        logger.info("Searching for label: '%s' (Standard Search)...", label)
        query = f"label:\"{label}\""
        found_tracks = []
        try:
            results = sp.search(q=query, limit=50, type='track', market='US')
            if results:
                found_tracks.extend(results['tracks']['items'])
                while results['tracks']['next']:
                    results = sp.next(results['tracks'])
                    found_tracks.extend(results['tracks']['items'])
        except Exception as e:
            logger.error(f"Error during search: {e}")

    total_tracks = len(found_tracks)
    logger.info(f"Total tracks found: {total_tracks}")

    if total_tracks == 0:
        logger.info("No tracks found.")
        return
    
    # Automatic confidence verification (unless disabled)
    if not no_verify and total_tracks > 0 and TrackConfidenceScorer:
        logger.info(f"Verifying tracks with confidence threshold {min_confidence}...")
        try:
            scorer = TrackConfidenceScorer(sp, label)
            verified_tracks, filtered_count = scorer.score_tracks_batch(
                found_tracks, 
                base_confidence=40,
                min_threshold=min_confidence
            )
            
            if filtered_count > 0:
                logger.info(f"Filtered {filtered_count} low-confidence tracks (kept {len(verified_tracks)}/{total_tracks})")
                found_tracks = verified_tracks
                total_tracks = len(found_tracks)
                
                if total_tracks == 0:
                    logger.warning("All tracks filtered out. Try lowering --min-confidence or use --no-verify.")
                    return
            else:
                logger.info("All tracks passed confidence verification.")
            
            # Show cache stats if debug enabled
            stats = scorer.get_cache_stats()
            if stats['cached_albums'] > 0:
                logger.debug(f"Album cache: {stats['cached_albums']} albums cached")
        except Exception as e:
            logger.warning(f"Confidence verification failed: {e}. Proceeding with unverified tracks.")
    elif no_verify:
        logger.info("Skipping automatic verification (--no-verify enabled)")
    
    if not exhaustive and not year:
        if total_tracks >= 1000:
            logger.warning("Warning: Hit the 1000 track limit. Use --exhaustive to find more.")
        elif total_tracks > 0:
            logger.info("Tip: If you expected more tracks, try running with --exhaustive to search year-by-year.")

    track_ids = [t['id'] for t in found_tracks]

    target_playlist_id = None
    is_existing = False

    # 1. Check base playlist (e.g. "Label") if year is set
    if year and not playlist:
        base_name = label
        base_id = spotifaj_functions.find_playlist_by_name(username, base_name)
        if base_id:
            if spotifaj_functions.confirm(f"Found existing playlist '{base_name}'. Add {total_tracks} tracks to it?", default=False):
                target_playlist_id = base_id
                is_existing = True

    # 2. If not selected, check specific playlist (e.g. "Label (2019)")
    if not target_playlist_id:
        existing_id = spotifaj_functions.find_playlist_by_name(username, playlist_name)

        if existing_id:
            if spotifaj_functions.confirm(f"Playlist '{playlist_name}' already exists. Add {total_tracks} tracks to it?", default=False):
                target_playlist_id = existing_id
                is_existing = True
            else:
                if spotifaj_functions.confirm(f"Create a NEW playlist '{playlist_name}' instead?"):
                    logger.info(f"Creating playlist '{playlist_name}' for user '{username}'...")
                    target_playlist_id = spotifaj_functions.create_playlist(username, playlist_name)
        else:
            if spotifaj_functions.confirm(f"Create playlist '{playlist_name}' and add {total_tracks} tracks?"):
                logger.info(f"Creating playlist '{playlist_name}' for user '{username}'...")
                target_playlist_id = spotifaj_functions.create_playlist(username, playlist_name)

    if target_playlist_id:
        # Filter out duplicates if adding to existing playlist
        if is_existing:
            logger.info("Checking for duplicates (ID & Metadata)...")
            
            # 1. ID Check (Fast)
            existing_ids = spotifaj_functions.get_playlist_track_ids(username, target_playlist_id)
            found_tracks = [t for t in found_tracks if t['id'] not in existing_ids]
            
            # 2. Metadata Check (Slower but catches cross-album duplicates)
            if found_tracks:
                existing_signatures = spotifaj_functions.get_playlist_track_signatures(username, target_playlist_id)
                unique_tracks = []
                for t in found_tracks:
                    sig = spotifaj_functions.create_track_signature(t)
                    if sig not in existing_signatures:
                        unique_tracks.append(t)
                        # Add to signatures to prevent duplicates within the new batch itself
                        existing_signatures.add(sig) 
                
                found_tracks = unique_tracks

            track_ids = [t['id'] for t in found_tracks]
            
            skipped_count = total_tracks - len(track_ids)
            if skipped_count > 0:
                logger.info(f"Skipping {skipped_count} duplicates.")
        
        if track_ids:
            # Validate BEFORE adding
            if validate or spotifaj_functions.confirm("Validate new tracks before adding (check labels)?", default=True):
                validated_ids = spotifaj_functions.validate_tracks_list(sp, found_tracks, label)
                if validated_ids is None:
                    logger.info("Validation cancelled. Aborting add operation.")
                    return
                track_ids = validated_ids

            if track_ids:
                logger.info(f"Adding {len(track_ids)} tracks to playlist (ID: {target_playlist_id})...")
                spotifaj_functions.add_song_to_spotify_playlist(username, track_ids, target_playlist_id)
                logger.info("Done!")
                
                # Offer to add cover image (only for new playlists)
                if not is_existing:
                    if spotifaj_functions.confirm("\nWould you like to add a cover image?", default=False):
                        image_url = click.prompt("Enter image URL (JPEG)", type=str)
                        try:
                            upload_playlist_cover(sp, target_playlist_id, image_url)
                            console.print("[green]Cover image uploaded successfully![/green]")
                        except Exception as e:
                            console.print(f"[red]Failed to upload cover image: {e}[/red]")
            else:
                logger.info("No tracks left after validation.")
        else:
            logger.info("No new tracks to add.")
    else:
        logger.info("Operation cancelled or failed.")

@spotifaj.command()
@click.argument('label')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--playlist', help="Playlist name (defaults to '<label> - Discogs Verified').")
@click.option('--strictness', type=click.Choice(['loose', 'normal', 'strict']), default='normal', help="Verification strictness.")
@click.option('--force-update', is_flag=True, help="Force update cache.")
def discogs_label(label, username, playlist, strictness, force_update):
    """Search for tracks by label using Discogs as source of truth."""
    if not DiscogsLabelWorkflow:
        logger.error("Discogs modules not loaded. Check dependencies.")
        return

    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
        
    try:
        # Initialize workflow
        workflow = DiscogsLabelWorkflow(sp)
        
        # Find label
        discogs_label = workflow._find_label(label)
        if not discogs_label:
            logger.error(f"Could not find Discogs label: {label}")
            return
            
        logger.info(f"Found Discogs label: {discogs_label.name} (ID: {discogs_label.id})")
        
        # Run workflow
        track_ids = workflow.get_label_tracks(
            label=discogs_label,
            force_update=force_update,
            strictness=strictness
        )
        
        if track_ids:
            # Determine playlist name
            if playlist:
                playlist_name = playlist
            else:
                # Check for both naming patterns: just label name, and label + suffix
                playlist_name_simple = discogs_label.name
                playlist_name_verified = f"{discogs_label.name} - Discogs Verified"
                
                # Check both patterns
                existing_simple = spotifaj_functions.find_playlist_by_name(username, playlist_name_simple)
                existing_verified = spotifaj_functions.find_playlist_by_name(username, playlist_name_verified)
                
                if existing_simple and not existing_verified:
                    playlist_name = playlist_name_simple
                    existing_playlist_id = existing_simple
                elif existing_verified:
                    playlist_name = playlist_name_verified
                    existing_playlist_id = existing_verified
                else:
                    # Neither exists, use verified naming
                    playlist_name = playlist_name_verified
                    existing_playlist_id = None
            
            # If we haven't checked yet, do it now
            if not playlist:
                if 'existing_playlist_id' not in locals():
                    existing_playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_name)
            else:
                existing_playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_name)
            
            if existing_playlist_id:
                # Playlist exists - check for new tracks
                existing_track_ids = spotifaj_functions.get_playlist_track_ids(username, existing_playlist_id)
                new_tracks = [tid for tid in track_ids if tid not in existing_track_ids]
                
                if new_tracks:
                    if spotifaj_functions.confirm(f"Playlist '{playlist_name}' exists with {len(existing_track_ids)} tracks. Add {len(new_tracks)} new tracks?", default=False):
                        workflow.create_label_playlist(track_ids, discogs_label.name, playlist_name=playlist_name)
                    else:
                        logger.info("Skipped updating playlist")
                else:
                    logger.info(f"All {len(track_ids)} tracks already in playlist '{playlist_name}'. No new tracks to add.")
            else:
                # No existing playlist - ask to create
                if spotifaj_functions.confirm(f"Found {len(track_ids)} verified tracks. Create playlist '{playlist_name}'?", default=True):
                    workflow.create_label_playlist(track_ids, discogs_label.name, playlist_name=playlist_name)
                else:
                    logger.info("Skipped creating playlist")
        else:
            logger.info("No tracks found.")
            
    except Exception as e:
        logger.error(f"Error in Discogs workflow: {e}")

@spotifaj.command()
@click.argument('username')
@click.argument('playlist_name')
@click.option('--public', is_flag=True, help="Make the playlist public.")
def create_playlist(username, playlist_name, public):
    """Create a new Spotify playlist."""
    logger.info(f"Creating playlist '{playlist_name}' for user '{username}'...")
    
    playlist_id = spotifaj_functions.create_playlist(username, playlist_name, public=public)
    
    if playlist_id:
        logger.info(f"Successfully created playlist. ID: {playlist_id}")
    else:
        logger.error("Failed to create playlist.")

@spotifaj.command()
@click.argument('query')
@click.option('--type', default='track', help='Type of search (artist, track, playlist, album).')
def search(query, type):
    """Search Spotify for a query."""
    sp = spotifaj_functions.get_spotify_client()
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
        
    logger.info(f"Searching for: '{query}' (Type: {type})...")
    try:
        # Fetch more results to account for duplicates
        results = sp.search(q=query, type=type, limit=50)
        items = results[f'{type}s']['items']
        if not items:
            logger.info("No results found.")
            return
            
        # Sort by similarity to query, then popularity
        if type in ['track', 'artist', 'album']:
             def sort_key(item):
                 name = item['name']
                 # Calculate similarity
                 similarity = SequenceMatcher(None, query.lower().strip(), name.lower().strip()).ratio()
                 # Return tuple: (similarity, popularity)
                 # This ensures exact/close matches come first, and among those, the most popular ones.
                 return (similarity, item.get('popularity', 0))
             
             items.sort(key=sort_key, reverse=True)

        # If artist search, prioritize exact matches and reduce noise
        if type == 'artist':
            exact_matches = [item for item in items if item['name'].lower().strip() == query.lower().strip()]
            if exact_matches:
                items = exact_matches
            else:
                # If no exact match, limit to top 5 to avoid long list of bad matches
                items = items[:5]

        seen = set()
        unique_items = []
        
        for item in items:
            name = item['name']
            # Create a unique key for deduplication based on display attributes
            if type == 'track':
                # Use primary artist only for deduplication
                primary_artist = item['artists'][0]['name']
                # Normalize name: remove (...) and [...] and - ...
                norm_name = re.sub(r"(?i)\s*(\(|\[|-).*", "", name)
                key = f"{norm_name}:{primary_artist}"
            elif type == 'artist':
                key = name
            elif type == 'album':
                primary_artist = item['artists'][0]['name']
                norm_name = re.sub(r"(?i)\s*(\(|\[|-).*", "", name)
                key = f"{norm_name}:{primary_artist}"
            elif type == 'playlist':
                owner = item['owner']['display_name']
                key = f"{name}:{owner}"
            else:
                key = item['id']
            
            # Normalize key for case-insensitive comparison
            key = key.lower().strip()
            
            if key not in seen:
                seen.add(key)
                unique_items.append(item)
            
            if len(unique_items) >= 20:
                break

        for i, item in enumerate(unique_items):
            name = item['name']
            # Use Spotify URI (spotify:...) to open directly in app
            url = item.get('uri') or item.get('external_urls', {}).get('spotify', '')
            
            def fmt_link(text, target):
                return f"[link={target}]{escape(text)}[/link]" if target else escape(text)

            prefix = f"{i+1:>2}. " if len(unique_items) > 1 else ""

            # Handle different item types for display
            if type == 'track':
                artists = ", ".join([a['name'] for a in item['artists']])
                console.print(f"{prefix}{fmt_link(f'{name} - {artists}', url)}", highlight=False)
            elif type == 'artist':
                console.print(f"{prefix}{fmt_link(name, url)}", highlight=False)
            elif type == 'album':
                artists = ", ".join([a['name'] for a in item['artists']])
                console.print(f"{prefix}{fmt_link(f'{name} - {artists}', url)}", highlight=False)
            elif type == 'playlist':
                owner = item['owner']['display_name']
                console.print(f"{prefix}{fmt_link(f'{name} (by {owner})', url)}", highlight=False)
                
    except Exception as e:
        logger.error(f"Error during search: {e}")

@spotifaj.command()
@click.argument('username')
def list_playlists(username):
    """List a user's playlists."""
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
        
    try:
        all_playlists = []
        playlists = sp.user_playlists(username)
        while playlists:
            all_playlists.extend(playlists['items'])
            if playlists['next']:
                playlists = sp.next(playlists)
            else:
                break
        
        # Display in numbered format like search command
        from rich.markup import escape
        for i, playlist in enumerate(all_playlists, 1):
            name = playlist['name']
            uri = playlist.get('uri', '')
            
            def fmt_link(text, target):
                return f"[link={target}]{escape(text)}[/link]" if target else escape(text)
            
            console.print(f"{i:>2}. {fmt_link(name, uri)}", highlight=False)
            
    except Exception as e:
        logger.error(f"Error fetching playlists: {e}")

@spotifaj.command()
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--verbose', '-v', is_flag=True, help="Include track count and ownership info.")
def export_playlist_names(username, verbose):
    """Export all playlist names from your profile (one per line)."""
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
    
    try:
        all_playlists = spotifaj_functions.fetch_all_user_playlists(username)
        
        if not all_playlists:
            console.print("[yellow]No playlists found.[/yellow]")
            return
        
        # Output playlist names (one per line for easy export)
        for playlist in all_playlists:
            if verbose:
                owner = playlist['owner']['id']
                owned = "✓" if owner == username else f"(by {owner})"
                track_count = playlist['tracks']['total']
                print(f"{playlist['name']}\t{track_count} tracks\t{owned}")
            else:
                print(playlist['name'])
        
        # Show count to stderr so it doesn't interfere with piping
        import sys
        sys.stderr.write(f"\nTotal: {len(all_playlists)} playlists\n")
        
    except Exception as e:
        logger.error(f"Error fetching playlists: {e}")

@spotifaj.command()
@click.argument('query')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--playlist', help="Name of the playlist to create.")
@click.option('--artist', is_flag=True, help="Filter results by artist.")
@click.option('--album', is_flag=True, help="Filter results by album.")
@click.option('--track', is_flag=True, help="Filter results by track.")
@click.option('--limit', default=SPOTIFY_SEARCH_DEFAULT_LIMIT, help=f"Number of tracks to fetch (default {SPOTIFY_SEARCH_DEFAULT_LIMIT}).")
@click.option('--all', 'fetch_all', is_flag=True, help=f"Fetch all results (up to {SPOTIFY_SEARCH_RESULT_LIMIT}).")
def search_and_add(query, username, playlist, artist, album, track, limit, fetch_all):
    """Search for tracks by query and add to a new playlist."""
    playlist_name = playlist if playlist else f"Search: {query}"
    
    sp = spotifaj_functions.get_spotify_client()
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
        
    search_query = query
    if artist:
        search_query = f"artist:{query}"
    elif album:
        search_query = f"album:{query}"
    elif track:
        search_query = f"track:{query}"
    else:
        # Attempt to detect intent
        logger.info("Detecting search intent...")
        try:
            # Search for single result in each category
            intent_results = sp.search(q=query, type='artist,album,track', limit=1)
            
            best_type = 'track'
            best_score = 0.0
            best_match_name = query
            
            # Check Artist
            if intent_results.get('artists', {}).get('items'):
                artist_obj = intent_results['artists']['items'][0]
                score = SequenceMatcher(None, query.lower(), artist_obj['name'].lower()).ratio()
                # Boost artist score slightly as it's a common intent for "Name" queries
                if score > SIMILARITY_THRESHOLD_ARTIST_INTENT: 
                    best_score = score
                    best_type = 'artist'
                    best_match_name = artist_obj['name']
            
            # Check Album (only override artist if significantly better)
            if intent_results.get('albums', {}).get('items'):
                album_obj = intent_results['albums']['items'][0]
                score = SequenceMatcher(None, query.lower(), album_obj['name'].lower()).ratio()
                if score > best_score and score > SIMILARITY_THRESHOLD_ALBUM_INTENT:
                    best_score = score
                    best_type = 'album'
                    best_match_name = album_obj['name']
            
            # Check Track (only override if exact match or very high confidence and others are low)
            if intent_results.get('tracks', {}).get('items'):
                track_obj = intent_results['tracks']['items'][0]
                score = SequenceMatcher(None, query.lower(), track_obj['name'].lower()).ratio()
                # If track is exact match, it might be a track search, but "Thriller" is both.
                # Usually if user wants a playlist, Artist or Album is better source than single track.
                # So we only default to track if Artist/Album scores are low.
                pass 

            if best_type == 'artist' and best_score > SIMILARITY_THRESHOLD_ARTIST_INTENT:
                logger.info(f"Detected Artist intent: '{best_match_name}' (Confidence: {best_score:.2f})")
                # Use original query to avoid incorrect auto-correction (e.g. Igor Jadranin -> Igor Garanin)
                search_query = f"artist:{query}"
            elif best_type == 'album' and best_score > SIMILARITY_THRESHOLD_ALBUM_INTENT:
                logger.info(f"Detected Album intent: '{best_match_name}' (Confidence: {best_score:.2f})")
                search_query = f"album:{query}"
            else:
                logger.info(f"Using general search (Best guess: {best_type}, Score: {best_score:.2f})")
                
        except Exception as e:
            logger.warning(f"Intent detection failed: {e}")

    search_limit = limit
    if fetch_all:
        search_limit = None # Let search function handle it
        
    logger.info(f"Searching for: '{search_query}'...")
    
    # Using spotifaj_functions.search which returns a dict with 'tracks'
    results = spotifaj_functions.search(sp, search_query, limit=search_limit, fetch_all=fetch_all)
    tracks = results.get('tracks', [])
    total_tracks = len(tracks)
    
    logger.info(f"Found {total_tracks} tracks.")
    
    if total_tracks == 0:
        return

    # Display tracks (limit to DEFAULT_DISPLAY_LIMIT by default unless user asked for more via limit option explicitly?)
    # User requirement: "per default if no option provided - display only first DEFAULT_DISPLAY_LIMIT"
    display_limit = DEFAULT_DISPLAY_LIMIT
    
    for i, track in enumerate(tracks):
        if i >= display_limit:
            break
            
        name = track['name']
        artists = ", ".join([a['name'] for a in track['artists']])
        # Use Spotify URI (spotify:...) to open directly in app
        url = track.get('uri') or track.get('external_urls', {}).get('spotify', '')
        
        def fmt_link(text, target):
            return f"[link={target}]{escape(text)}[/link]" if target else escape(text)
            
        console.print(f"{i+1:>2}. {fmt_link(f'{name} - {artists}', url)}", highlight=False)

    if total_tracks > display_limit:
        console.print(f"... and {total_tracks - display_limit} more tracks (not displayed).")

    track_ids = [t['id'] for t in tracks]

    if spotifaj_functions.confirm(f"Create playlist '{playlist_name}' and add ALL {total_tracks} tracks?"):
        logger.info(f"Creating playlist '{playlist_name}' for user '{username}'...")
        playlist_id = spotifaj_functions.create_playlist(username, playlist_name)
        
        if playlist_id:
            logger.info(f"Adding {total_tracks} tracks to playlist (ID: {playlist_id})...")
            spotifaj_functions.add_song_to_spotify_playlist(username, track_ids, playlist_id)
            logger.info("Done!")
        else:
            logger.error("Failed to create playlist.")
    else:
        logger.info("Operation cancelled.")

@spotifaj.command()
@click.argument('playlist_input', required=False)
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--all', 'check_all', is_flag=True, help="Check ALL user playlists for duplicates.")
@click.option('--dry-run', is_flag=True, help="Show duplicates without removing them.")
@click.option('--keep-best', type=click.Choice(['popularity', 'explicit', 'clean', 'longest', 'shortest']), help="When removing duplicates, keep the best version based on criteria.")
def deduplicate(playlist_input, username, check_all, dry_run, keep_best):
    """
    Check for duplicate tracks in a playlist with smart keep-best logic.
    
    PLAYLIST_INPUT can be a Spotify Playlist URL or a Playlist Name.
    --keep-best options:
      popularity: Keep the most popular version
      explicit: Prefer explicit versions
      clean: Prefer clean (non-explicit) versions
      longest: Keep the longest version
      shortest: Keep the shortest version
    """
    if not playlist_input and not check_all:
        logger.error("Please provide a PLAYLIST_INPUT or use --all.")
        return

    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    playlists_to_check = []

    if check_all:
        logger.info("Fetching all user playlists...")
        all_playlists = spotifaj_functions.fetch_all_user_playlists(username)
        # Filter to only owned playlists
        playlists_to_check = [p for p in all_playlists if p['owner']['id'] == username]
        logger.info(f"Filtered to {len(playlists_to_check)} owned playlists (out of {len(all_playlists)} total).")
    elif playlist_input:
        # Check if input is a URL
        url_match = re.search(r"playlist/([a-zA-Z0-9]+)", playlist_input)
        if url_match:
            playlist_id = url_match.group(1)
            try:
                pl = sp.playlist(playlist_id)
                playlists_to_check = [pl]
            except Exception as e:
                logger.error(f"Could not find playlist with ID {playlist_id}: {e}")
                return
        else:
            # Treat as name
            playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_input)
            if playlist_id:
                pl = sp.playlist(playlist_id)
                playlists_to_check = [pl]
            else:
                logger.error(f"Could not find playlist named '{playlist_input}'.")
                return

    if not playlists_to_check:
        logger.info("No playlists found to check.")
        return

    # Stats
    total_duplicates = 0
    max_duplicates = 0
    max_dup_playlist = None
    playlists_with_dups = 0
    total_playlists = len(playlists_to_check)

    console.print(f"[bold cyan]Checking {total_playlists} playlists for duplicates...[/bold cyan]")

    with Progress(
        TimeElapsedColumn(),
        TextColumn("[cyan]Scanning[/cyan]"),
        TextColumn("[progress.description]{task.description}", table_column=Column(width=45, overflow="ellipsis", no_wrap=True)),
        TextColumn("{task.completed}/[bold]{task.total}[/bold]"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False
    ) as progress:
        task = progress.add_task("...", total=total_playlists)

        for pl_index, pl in enumerate(playlists_to_check):
            # Be polite to the API - increase delay to avoid rate limiting
            # Use longer delay when processing many playlists
            time.sleep(0.5 if total_playlists > 50 else SPOTIFY_DEFAULT_DELAY)
            
            name = pl['name']
            pid = pl['id']
            owner_id = pl['owner']['id']
            is_collaborative = pl['collaborative']
            
            # Check if we can modify this playlist
            can_modify = (owner_id == username) or is_collaborative
            
            progress.update(task, description=f"[cyan]'{name}'...[/cyan]")
            
            try:
                duplicates = spotifaj_functions.find_duplicates_in_playlist(username, pid)
            except Exception as e:
                if '429' in str(e):
                    console.print(f"\n[yellow]⚠ Rate limit reached. Processed {pl_index + 1}/{total_playlists} playlists.[/yellow]")
                    console.print(f"[yellow]Please wait before running again, or process fewer playlists at a time.[/yellow]")
                    break
                else:
                    console.print(f"\n[red]Error processing '{name}': {e}[/red]")
                    progress.advance(task)
                    continue
            
            if duplicates and keep_best:
                # Apply keep-best logic to determine which versions to keep
                # Group duplicates by signature
                from collections import defaultdict
                dup_groups = defaultdict(list)
                
                for d in duplicates:
                    # Create signature from original track
                    orig = d['original']
                    sig = spotifaj_functions.create_track_signature(orig)
                    dup_groups[sig].append(d)
                
                # For each group, select best version to keep
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
                        # Prefer explicit, then by popularity
                        explicit_versions = [v for v in versions if v.get('explicit', False)]
                        if explicit_versions:
                            best = max(explicit_versions, key=lambda t: t.get('popularity', 0))
                        else:
                            best = max(versions, key=lambda t: t.get('popularity', 0))
                    elif keep_best == 'clean':
                        # Prefer clean (non-explicit), then by popularity
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
                count = len(duplicates)
                
                if count == 0:
                    progress.advance(task)
                    continue
            
            if duplicates:
                count = len(duplicates)
                total_duplicates += count
                playlists_with_dups += 1
                if count > max_duplicates:
                    max_duplicates = count
                    max_dup_playlist = name
                
                progress.console.print(f"[bold yellow]Found {count} duplicates in '{name}'.[/bold yellow]")
                if not can_modify:
                    progress.console.print(f"[red]Note: Playlist '{name}' is owned by '{owner_id}' and is not collaborative. Cannot remove tracks.[/red]")
                
                # Create a Rich table to display duplicates
                table = Table(title=f"Duplicates in '{name}'")
                table.add_column("Pos", justify="right", style="cyan", no_wrap=True)
                table.add_column("Track Name", style="magenta")
                table.add_column("Artist", style="green")
                table.add_column("Album", style="yellow")
                table.add_column("Duration", justify="right")
                table.add_column("Match Type", style="red")

                for d in duplicates:
                    dup = d['duplicate']
                    orig = d['original']
                    pos = d['position']
                    
                    # Format duration
                    dur_ms = dup['duration_ms']
                    minutes, seconds = divmod(dur_ms // 1000, 60)
                    duration_str = f"{minutes}:{seconds:02d}"
                    
                    table.add_row(
                        str(pos + 1), # 1-based index for display
                        dup['name'],
                        dup['artists'][0]['name'],
                        dup['album']['name'],
                        duration_str,
                        "Duplicate"
                    )
                    # Optionally show the original it matched against?
                    if dup['uri'] != orig['uri']:
                         table.add_row(
                            "", 
                            f"↳ Matches: {orig['name']}", 
                            orig['artists'][0]['name'], 
                            orig['album']['name'], 
                            "", 
                            "Original"
                        )

                progress.console.print(table)
                
                if dry_run:
                    progress.console.print(f"[dim][Dry Run] Would remove {len(duplicates)} tracks from '{name}'.[/dim]")
                elif not can_modify:
                    progress.console.print(f"[dim]Skipping removal for read-only playlist '{name}'.[/dim]")
                else:
                    progress.stop() # Pause progress for input
                    if spotifaj_functions.confirm(f"Remove {len(duplicates)} duplicates from '{name}'?", default=False):
                        # Group by URI for removal
                        removal_map = {} # uri -> list of positions
                        for d in duplicates:
                            uri = d['duplicate']['uri']
                            pos = d['position']
                            if uri not in removal_map:
                                removal_map[uri] = []
                            removal_map[uri].append(pos)
                        
                        tracks_to_remove = [{'uri': uri, 'positions': positions} for uri, positions in removal_map.items()]
                        
                        with Progress(
                            TextColumn("[bold blue]Removing duplicates...[/bold blue]"),
                            BarColumn(),
                            TaskProgressColumn(),
                            TimeRemainingColumn(),
                            console=console,
                            transient=True
                        ) as remove_progress:
                            remove_task = remove_progress.add_task("Removing...", total=len(tracks_to_remove))
                            
                            def update_progress(count):
                                remove_progress.advance(remove_task, count)
                                
                            spotifaj_functions.remove_specific_occurrences(username, pid, tracks_to_remove, progress_callback=update_progress)
                        
                        console.print("[green]Cleanup complete.[/green]")
                    else:
                        console.print("[yellow]Skipping removal.[/yellow]")
                    progress.start() # Resume progress
            
            progress.advance(task)

    # Summary Statistics
    console.print("\n[bold]--- Deduplication Summary ---[/bold]")
    console.print(f"Total Playlists Scanned: [cyan]{total_playlists}[/cyan]")
    console.print(f"Playlists with Duplicates: [yellow]{playlists_with_dups}[/yellow]")
    console.print(f"Total Duplicates Found: [red]{total_duplicates}[/red]")
    if max_dup_playlist:
        console.print(f"Highest Duplicates: [red]{max_duplicates}[/red] in '[bold]{max_dup_playlist}[/bold]'")

@spotifaj.command()
@click.argument('playlist_input')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('-f', '--format', type=click.Choice(['txt', 'csv', 'json', 'm3u']), help="Export format (auto-detected from file extension if not specified).")
@click.option('--file', type=click.Path(), help="Output file path. If not specified, prints to stdout.")
def export_playlist(playlist_input, username, format, file):
    """
    Export a playlist in various formats.
    
    PLAYLIST_INPUT can be a Spotify Playlist URL, ID, or Name.
    Formats: txt (Artist - Track), csv (full metadata), json, m3u (playlist file)
    """
    # Auto-detect format from file extension if not specified
    if not format:
        if file:
            ext = file.lower().rsplit('.', 1)[-1] if '.' in file else None
            if ext in ['txt', 'csv', 'json', 'm3u']:
                format = ext
            else:
                format = 'txt'  # Default fallback
        else:
            format = 'txt'  # Default for stdout
    
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    playlist_id = None

    # 1. Try to parse as URL or ID
    match = re.search(r'playlist/([a-zA-Z0-9]+)', playlist_input)
    if match:
        playlist_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', playlist_input):
        playlist_id = playlist_input
    
    # 2. If not URL/ID, try to find by name
    if not playlist_id:
        logger.info(f"Searching for playlist with name: '{playlist_input}'...")
        playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_input)

    if not playlist_id:
        logger.error(f"Could not find playlist: {playlist_input}")
        sys.exit(1)

    logger.info(f"Exporting playlist ID: {playlist_id}...", extra={"markup": True})
    
    try:
        # Get playlist info
        playlist_info = sp.playlist(playlist_id)
        playlist_name = playlist_info['name']
        
        results = sp.playlist_items(playlist_id)
        tracks = results['items']
        
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        
        # Filter out None tracks
        valid_tracks = []
        for item in tracks:
            if item and 'track' in item and item['track']:
                valid_tracks.append(item)
        
        # Generate output based on format
        import json
        import csv
        from io import StringIO
        
        output_content = None
        
        if format == 'txt':
            lines = []
            for item in valid_tracks:
                track = item['track']
                artists = ", ".join([a['name'] for a in track['artists']])
                lines.append(f"{artists} - {track['name']}")
                # Include URI as comment only when exporting to file (for reliable re-import)
                if file:
                    lines.append(f"# {track['uri']}")
            output_content = "\n".join(lines)
            
        elif format == 'csv':
            output_buffer = StringIO()
            writer = csv.writer(output_buffer)
            writer.writerow(['Artist', 'Track', 'Album', 'Year', 'Duration (ms)', 'Popularity', 'ISRC', 'Spotify URI'])
            
            for item in valid_tracks:
                track = item['track']
                artists = ", ".join([a['name'] for a in track['artists']])
                album = track['album']['name']
                year = track['album'].get('release_date', '')[:4] if track['album'].get('release_date') else ''
                duration = track['duration_ms']
                popularity = track.get('popularity', 0)
                isrc = track.get('external_ids', {}).get('isrc', '')
                uri = track['uri']
                
                writer.writerow([artists, track['name'], album, year, duration, popularity, isrc, uri])
            
            output_content = output_buffer.getvalue()
            
        elif format == 'json':
            playlist_data = {
                'name': playlist_name,
                'id': playlist_id,
                'total_tracks': len(valid_tracks),
                'tracks': []
            }
            
            for item in valid_tracks:
                track = item['track']
                track_data = {
                    'name': track['name'],
                    'artists': [a['name'] for a in track['artists']],
                    'album': track['album']['name'],
                    'release_date': track['album'].get('release_date', ''),
                    'duration_ms': track['duration_ms'],
                    'popularity': track.get('popularity', 0),
                    'explicit': track.get('explicit', False),
                    'isrc': track.get('external_ids', {}).get('isrc', ''),
                    'uri': track['uri'],
                    'spotify_url': track['external_urls'].get('spotify', '')
                }
                playlist_data['tracks'].append(track_data)
            
            output_content = json.dumps(playlist_data, indent=2)
            
        elif format == 'm3u':
            lines = ['#EXTM3U']
            for item in valid_tracks:
                track = item['track']
                artists = ", ".join([a['name'] for a in track['artists']])
                duration_sec = track['duration_ms'] // 1000
                lines.append(f"#EXTINF:{duration_sec},{artists} - {track['name']}")
                lines.append(track['external_urls'].get('spotify', track['uri']))
            output_content = "\n".join(lines)
        
        # Output to file or stdout
        if file:
            with open(file, 'w', encoding='utf-8') as f:
                f.write(output_content)
            console.print(f"[green]Exported {len(valid_tracks)} tracks to {file}[/green]")
        else:
            print(output_content)
            
    except Exception as e:
        logger.error(f"Error exporting playlist: {e}")


def upload_playlist_cover(sp, playlist_id, image_url):
    """
    Download an image from URL and upload it as playlist cover.
    
    Args:
        sp: Spotify client
        playlist_id: Spotify playlist ID
        image_url: URL to JPEG image
    
    Raises:
        Exception: If download or upload fails
    """
    try:
        from PIL import Image
    except ImportError:
        raise Exception("Pillow is required for image processing. Install it with: pip install Pillow")
    
    try:
        # Download image
        logger.info(f"Downloading image from {image_url}...")
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Open image with PIL
        img = Image.open(BytesIO(response.content))
        
        # Convert to RGB if needed (for PNG with transparency, etc.)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Start with original dimensions
        width, height = img.size
        logger.debug(f"Original image size: {width}x{height}")
        
        # Spotify recommends max dimensions and requires < 256KB
        # Start with reasonable dimensions and aggressive compression
        max_dimension = 640  # Spotify shows at 300x300, so 640 is safe
        
        # Resize if too large
        if width > max_dimension or height > max_dimension:
            if width > height:
                new_width = max_dimension
                new_height = int(height * (max_dimension / width))
            else:
                new_height = max_dimension
                new_width = int(width * (max_dimension / height))
            
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            logger.debug(f"Resized to: {new_width}x{new_height}")
        
        # Compress to fit under 256KB (accounting for base64 encoding overhead)
        # Base64 adds exactly 4/3 overhead (33.33%), so aim for max 180KB raw data to be safe
        MAX_RAW_SIZE = 180 * 1024  # 180KB raw = 240KB encoded (safe margin under 256KB)
        
        quality = 90
        buffer = BytesIO()
        img.save(buffer, format='JPEG', quality=quality, optimize=True)
        image_data = buffer.getvalue()
        
        logger.debug(f"Initial compression at quality {quality}: {len(image_data)} bytes")
        
        # Reduce quality until it fits
        while len(image_data) > MAX_RAW_SIZE and quality > 30:
            quality -= 5
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=quality, optimize=True)
            image_data = buffer.getvalue()
            logger.debug(f"Compressed to quality {quality}: {len(image_data)} bytes")
        
        # If still too large, reduce dimensions further
        current_width, current_height = img.size
        while len(image_data) > MAX_RAW_SIZE and current_width > 300:
            # Reduce by 10% each iteration
            current_width = int(current_width * 0.9)
            current_height = int(current_height * 0.9)
            
            img = img.resize((current_width, current_height), Image.Resampling.LANCZOS)
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=quality, optimize=True)
            image_data = buffer.getvalue()
            logger.debug(f"Resized to {current_width}x{current_height}, {len(image_data)} bytes")
        
        if len(image_data) > MAX_RAW_SIZE:
            raise Exception(f"Image too large even after compression ({len(image_data)} bytes raw, ~{int(len(image_data) * 1.33)} bytes encoded, max 256KB encoded)")
        
        logger.info(f"Final image: {len(image_data)} bytes (quality={quality}, size={img.size})")
        
        # Encode to base64
        encoded_image = base64.b64encode(image_data).decode('utf-8')
        
        # Upload to Spotify
        sp.playlist_upload_cover_image(playlist_id, encoded_image)
        
    except requests.RequestException as e:
        raise Exception(f"Failed to download image: {e}")
    except Exception as e:
        raise Exception(f"Failed to process/upload image: {e}")


def normalize_text_for_matching(text):
    """
    Normalize text for fuzzy matching.
    
    - Normalizes all dash types and separators
    - Removes content after | pipe
    - Case insensitive
    - Collapses multiple spaces
    
    Args:
        text: Raw text string
        
    Returns:
        str: Normalized text (lowercase)
    """
    if not text:
        return ""
    
    # Remove content after pipe separator
    if '|' in text:
        text = text.split('|')[0].strip()
    
    # Normalize all dash types, separators, and "vs"/"v" to space
    # This makes "theorem vs. swayzak" match "theorem, swayzak"
    text = re.sub(r'\s+vs\.?\s+|\s+v\.?\s+', ' ', text, flags=re.IGNORECASE)  # "vs." or "v." -> space
    text = re.sub(r'[-–—―‐‑‒−/|,]', ' ', text)  # dashes, slashes, commas -> space
    
    # Collapse multiple spaces to single space
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text.lower()


def parse_track_input(line):
    """
    Parse a track input line to extract artist and track name.
    
    Supports various formats:
    - Artist - Track
    - Artist | Track
    - Artist – Track (em dash)
    - Artist: Track
    - Artist featuring Artist2 - Track
    
    Returns:
        tuple: (artist, track_name, all_artists) where all_artists is the full artist string
    """
    # Strip numbered prefixes like "01. ", "1. ", etc.
    line = re.sub(r'^\s*\d+\.\s*', '', line)
    
    # Don't normalize yet - we need separators to parse!
    # Just clean up the pipe content first
    if '|' in line:
        line = line.split('|')[0].strip()
    
    # Check for format: Artist "Track Name" (Label)
    quoted_match = re.match(r'^([^"]+?)\s+"([^"]+)"\s*(?:\([^)]+\))?', line)
    if quoted_match:
        artist_part = quoted_match.group(1).strip()
        track = quoted_match.group(2).strip()
        primary_artist, all_artists = normalize_artist_string(artist_part)
        return primary_artist, track, all_artists
    
    # Remove common metadata patterns in parentheses
    # Examples: (taken from Album, Year), (Album Version), (feat. Artist), etc.
    # Keep version info like (Remix), (Radio Edit) as those are important for matching
    line = re.sub(r'\s*\(taken from[^)]+\)', '', line, flags=re.IGNORECASE)
    line = re.sub(r'\s*\(from[^)]+\d{4}[^)]*\)', '', line, flags=re.IGNORECASE)  # (from Album, 2004)
    line = re.sub(r'\s*\(released (?:on|in)[^)]+\)', '', line, flags=re.IGNORECASE)  # (released on 12'' By BBE in 2003)
    
    # Common separators (including all dash types)
    separators = [' - ', ' – ', ' — ', ': ']
    
    for sep in separators:
        if sep in line:
            parts = line.split(sep, 1)
            if len(parts) == 2:
                artist_part = parts[0].strip()
                track = parts[1].strip()
                
                # Extract primary artist and normalize featuring/feat/ft patterns
                primary_artist, all_artists = normalize_artist_string(artist_part)
                
                return primary_artist, track, all_artists
    
    # If no separator found, return the whole line as track name
    return None, line.strip(), None


def normalize_artist_string(artist_str):
    """
    Normalize artist string to handle featuring/feat/ft patterns.
    
    Args:
        artist_str: Raw artist string (e.g., "Navy Blue featuring Billy Woods")
        
    Returns:
        tuple: (primary_artist, normalized_all_artists)
    """
    # Patterns for featuring
    featuring_patterns = [
        r'\s+featuring\s+',
        r'\s+feat\.?\s+',
        r'\s+ft\.?\s+',
        r'\s+with\s+',
        r'\s+&\s+',
    ]
    
    # Extract primary artist (before any featuring pattern)
    primary_artist = artist_str
    for pattern in featuring_patterns:
        match = re.split(pattern, artist_str, maxsplit=1, flags=re.IGNORECASE)
        if len(match) > 1:
            primary_artist = match[0].strip()
            break
    
    # Return both primary artist and the full string (for broader matching)
    return primary_artist, artist_str


def calculate_match_confidence(search_result, expected_artist, expected_track, expected_all_artists=None):
    """
    Calculate confidence score for a search result match.
    
    Args:
        search_result: Spotify track object
        expected_artist: Expected primary artist name
        expected_track: Expected track name
        expected_all_artists: Full artist string including features
        
    Returns:
        int: Confidence score (0-100)
    """
    if not search_result:
        return 0
    
    confidence = 0
    
    # Get actual values from search result
    actual_track = search_result.get('name', '')
    actual_artists = [artist['name'] for artist in search_result.get('artists', [])]
    actual_artist_str = ', '.join(actual_artists)
    
    # Normalize for comparison
    expected_track_norm = normalize_text_for_matching(expected_track).lower() if expected_track else ""
    actual_track_norm = normalize_text_for_matching(actual_track).lower()
    
    # Remove common suffixes from track names for better matching
    # (Radio Edit, Remix, Remaster, etc. shouldn't reduce confidence)
    def strip_track_suffixes(track):
        suffixes = [
            r'\s*radio\s+edit.*$',
            r'\s*single\s+edit.*$',
            r'\s*album\s+version.*$',
            r'\s*original\s+mix.*$',
            r'\s*remaster.*$',
            r'\s*remix.*$',
            r'\s*\d{4}\s+remaster.*$',
            r'\s+minus$',  # Trailing "minus" (version indicator)
            r'\s+plus$',   # Trailing "plus" (version indicator)
        ]
        for suffix in suffixes:
            track = re.sub(suffix, '', track, flags=re.IGNORECASE)
        return track.strip()
    
    expected_track_clean = strip_track_suffixes(expected_track_norm)
    actual_track_clean = strip_track_suffixes(actual_track_norm)
    
    # Track name matching (50 points max)
    if expected_track:
        track_similarity = SequenceMatcher(None, expected_track_clean, actual_track_clean).ratio()
        confidence += int(track_similarity * 50)
        
        # Bonus for original/radio versions over remixes
        # If the expected track doesn't mention a remix, prefer original versions
        if not re.search(r'\b(remix|mix|edit)\b', expected_track, re.IGNORECASE):
            # Check if actual track is a remix (but not radio/single edit)
            if re.search(r'\b(remix|mix)\b', actual_track, re.IGNORECASE):
                # Don't penalize Radio Edit or Single Edit
                if not re.search(r'\b(radio|single|original)\s+(edit|mix|version)\b', actual_track, re.IGNORECASE):
                    # Heavy penalize other remixes by 20 points
                    confidence = max(0, confidence - 20)
            # Boost Radio Edit / Original Mix / Single Edit by 10 points
            elif re.search(r'\b(radio|single|original)\s+(edit|mix|version)\b', actual_track, re.IGNORECASE):
                confidence = min(100, confidence + 10)
            # Also boost if it's just the plain track (no version info)
            elif not re.search(r'\b(version|edit|mix|live|acoustic|instrumental)\b', actual_track, re.IGNORECASE):
                confidence = min(100, confidence + 5)
    
    # Artist matching (50 points max)
    if expected_artist:
        # Normalize expected artist
        expected_artist_norm = normalize_text_for_matching(expected_artist).lower()
        # Check against all artists in the track
        artist_scores = []
        
        # 1. Check primary artist match (with normalization)
        for actual_artist in actual_artists:
            actual_artist_norm = normalize_text_for_matching(actual_artist).lower()
            score = SequenceMatcher(None, expected_artist_norm, actual_artist_norm).ratio()
            artist_scores.append(score)
        
        # 2. Also check if any words from expected_all_artists appear in actual artists
        if expected_all_artists:
            # Extract artist names from the full string
            # Split on featuring/feat/ft/with/& and also on commas (with optional spaces)
            expected_names = re.split(r'\s+(?:featuring|feat\.?|ft\.?|with|&)\s+|\s*,\s*', expected_all_artists, flags=re.IGNORECASE)
            for expected_name in expected_names:
                expected_name_norm = normalize_text_for_matching(expected_name.strip()).lower()
                for actual_artist in actual_artists:
                    actual_artist_norm = normalize_text_for_matching(actual_artist).lower()
                    score = SequenceMatcher(None, expected_name_norm, actual_artist_norm).ratio()
                    artist_scores.append(score)
            
            # Bonus: If we have multiple expected artists, check if actual track also has multiple
            # and if the artist count is similar (indicates multi-artist collaboration match)
            multi_artist_boost_applied = False
            if len(expected_names) > 1 and len(actual_artists) >= len(expected_names):
                # Count how many expected artists have good matches (>= 0.8 similarity)
                good_matches = sum(1 for score in artist_scores if score >= 0.8)
                match_ratio = good_matches / len(expected_names)
                
                # If most/all artists match well, boost confidence significantly
                if match_ratio >= 0.75:
                    # This is likely a multi-artist exact match
                    confidence = min(100, confidence + 20)
                    multi_artist_boost_applied = True
        else:
            multi_artist_boost_applied = False
        
        # Use best artist match
        if artist_scores:
            best_artist_match = max(artist_scores)
            confidence += int(best_artist_match * 50)
            
            # Only apply strict penalties if this is NOT a multi-artist match
            # (multi-artist matches already verified all artists individually)
            if not multi_artist_boost_applied:
                # Strict penalty if artist match is poor
                # Even "similar" artist names like Anushka vs Anouk should be penalized
                if best_artist_match < 0.3:
                    # Extremely poor match - basically reject it
                    confidence = int(confidence * 0.2)  # Reduce by 80%
                elif best_artist_match < 0.5:
                    # Poor match
                    confidence = int(confidence * 0.4)  # Reduce by 60%
                elif best_artist_match < 0.75:
                    # Moderate match - similar but not exact (Anushka vs Anouk)
                    confidence = int(confidence * 0.6)  # Reduce by 40%
    else:
        # No artist specified, only use track matching (less reliable)
        confidence = int(confidence * 0.7)  # Reduce confidence when no artist
    
    return min(confidence, 100)


def fetch_1001tracklists(url):
    """
    Fetch tracklist from 1001tracklists.com URL.
    
    Args:
        url: 1001tracklists.com URL
        
    Returns:
        list: List of track strings in "Artist - Track" format
    """
    logger.info(f"Fetching tracklist from {url}...")
    
    # Extract tracklist ID from URL
    match = re.search(r'/tracklist/([a-zA-Z0-9]+)/', url)
    if not match:
        logger.error("Could not extract tracklist ID from URL")
        return None
    
    tracklist_id = match.group(1)
    
    # The 1001tracklists export API requires authentication
    logger.error("1001tracklists.com requires authentication for programmatic access.")
    logger.error("Please export the tracklist manually:")
    console.print("\n[yellow]Manual Export Instructions:[/yellow]")
    console.print("1. Open the tracklist in your browser")
    console.print("2. Look for the 'Export' or 'Download' button on the page")
    console.print("3. Export as Text format")
    console.print("4. Save to a file (e.g., tracklist.txt)")
    console.print("5. Import with: [cyan]./spotifaj import-playlist tracklist.txt -n \"Playlist Name\"[/cyan]")
    console.print("\nOr copy tracks directly from the page and paste when prompted:\n")
    console.print("   [cyan]./spotifaj import-playlist -n \"Playlist Name\"[/cyan]")
    console.print("   [dim](then paste tracks and press Ctrl+D)[/dim]\n")
    
    return None


@spotifaj.command()
@click.argument('input_file', type=click.File('r'), required=False)
@click.option('--name', '-n', required=True, help="Name of the new playlist.")
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--yes', '-y', is_flag=True, help="Skip confirmation prompts.")
@click.option('--url', help="Import from a 1001tracklists.com URL.")
def import_playlist(input_file, name, username, yes, url):
    """
    Create a playlist from a text list of tracks or a 1001tracklists URL.
    
    Reads from INPUT_FILE, stdin, or --url if provided.
    Expected format: "Artist - Track Name" per line.
    """
    # Handle input source
    if url:
        # Check if it's a 1001tracklists URL
        if '1001tracklists.com' in url:
            lines = fetch_1001tracklists(url)
            if not lines:
                logger.error("Failed to fetch tracklist from URL.")
                return
        else:
            logger.error("URL must be from 1001tracklists.com")
            return
    elif input_file:
        lines = input_file.readlines()
    else:
        # Check if data is being piped
        if not sys.stdin.isatty():
            lines = sys.stdin.readlines()
        else:
            console.print("[yellow]Enter tracks (Artist - Song Name), one per line.[/yellow]")
            console.print("[yellow]Press Ctrl+D (Linux/Mac) or Ctrl+Z (Windows) on a new line to finish:[/yellow]")
            lines = sys.stdin.readlines()

    if not lines:
        logger.warning("No input provided.")
        return

    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    # Helper function to handle API calls with automatic token refresh
    def safe_search(query, limit=50, max_retries=3):
        """Perform a search with automatic token refresh on 401 errors."""
        for attempt in range(max_retries):
            try:
                return sp.search(q=query, limit=limit, type='track')
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 401 and attempt < max_retries - 1:
                    # Token expired - the auth_manager should auto-refresh on next call
                    logger.warning(f"Token expired during search, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(1)
                    continue
                else:
                    raise
            except Exception as e:
                raise
        return None

    track_uris = []
    not_found = []
    low_confidence_matches = []
    
    logger.info(f"Processing {len(lines)} lines...")
    
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Searching tracks...", total=len(lines))
        
        line_num = 0
        while line_num < len(lines):
            line = lines[line_num].strip()
            
            if not line:
                progress.advance(task)
                line_num += 1
                continue
            
            # Skip log lines if piping from export command
            if (line.startswith("[") and "INFO" in line) or line.startswith("Exporting playlist"):
                progress.advance(task)
                line_num += 1
                continue
            
            # Check if this is a direct URI input (without comment prefix)
            if line.startswith("spotify:track:"):
                uri = line.strip()
                track_uris.append({
                    'uri': uri,
                    'input': uri,
                    'found': f'{uri} (direct URI)',
                    'confidence': 100,
                    'line_num': line_num
                })
                progress.advance(task)
                line_num += 1
                continue
            
            # Skip URI comment lines - they should have been processed with their track line
            if line.startswith("# spotify:track:"):
                progress.advance(task)
                line_num += 1
                continue
            
            # Check if next line is a URI comment (from our TXT export)
            has_uri_next = (line_num + 1 < len(lines) and 
                           lines[line_num + 1].strip().startswith("# spotify:track:"))
            
            if has_uri_next:
                # Use the URI directly without searching
                uri = lines[line_num + 1].strip()[2:].strip()  # Remove "# " prefix
                track_uris.append({
                    'uri': uri,
                    'input': line,
                    'found': f'{line} (from URI)',
                    'confidence': 100,
                    'line_num': line_num
                })
                progress.advance(task)
                progress.advance(task)  # Skip the URI line too
                line_num += 2  # Skip both lines
                continue

            try:
                # Parse the input to extract artist and track
                artist, track, all_artists = parse_track_input(line)
                
                # Normalize search terms by removing special characters for better matching
                def normalize_for_search(text):
                    if not text:
                        return ""
                    # Remove apostrophes (Yesterday's -> Yesterdays)
                    text = text.replace("'", "").replace("'", "")
                    # Remove dashes, pipes, and other separators
                    text = re.sub(r'[-–—|/]', ' ', text)
                    # Collapse multiple spaces
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text
                
                # Add fuzzy search variant for common typos
                def create_fuzzy_variants(text):
                    """Create search variants for common typos."""
                    variants = [text]
                    
                    # Common misspellings: y <-> i (Rythym vs Rhythm)
                    if 'y' in text.lower():
                        variants.append(re.sub(r'y', 'i', text, flags=re.IGNORECASE))
                    if 'i' in text.lower():
                        variants.append(re.sub(r'i', 'y', text, flags=re.IGNORECASE))
                    
                    return list(set(variants))  # Remove duplicates
                
                # Try multiple search strategies for better results
                search_queries = []
                
                if artist and track:
                    # Normalize both artist and track for search
                    artist_norm = normalize_for_search(artist)
                    track_norm = normalize_for_search(track)
                    
                    # Strategy 1: Simple artist + track (flexible, works for most cases)
                    search_queries.append(f"{artist_norm} {track_norm}")
                    
                    # Strategy 2: Add fuzzy variants for track name to catch typos
                    track_variants = create_fuzzy_variants(track_norm)
                    for variant in track_variants:
                        if variant != track_norm:  # Don't duplicate the original
                            search_queries.append(f"{artist_norm} {variant}")
                    
                    # Strategy 3: Track name only (fallback for rare/misspelled artists)
                    search_queries.append(f"{track_norm}")
                else:
                    # Fallback: search the whole line (normalized)
                    search_queries.append(normalize_for_search(track or line))
                
                # Collect results from all search strategies
                all_items = []
                seen_uris = set()
                
                for search_query in search_queries:
                    try:
                        # Increase limit to 50 to catch more variations/remixes
                        # Use safe_search to handle token expiration
                        results = safe_search(search_query, limit=50)
                        if results:
                            items = results['tracks']['items']
                            # Deduplicate by URI
                            for item in items:
                                if item['uri'] not in seen_uris:
                                    all_items.append(item)
                                    seen_uris.add(item['uri'])
                    except Exception as search_err:
                        logger.debug(f"Search query '{search_query}' failed: {search_err}")
                        continue
                
                if all_items:
                    # Calculate confidence for each result
                    best_match = None
                    best_confidence = 0
                    
                    for item in all_items:
                        confidence = calculate_match_confidence(item, artist, track, all_artists)
                        if confidence > best_confidence:
                            best_confidence = confidence
                            best_match = item
                        
                        # Debug: log all candidates with confidence > 30% for manual review
                        if confidence >= 30:
                            logger.debug(f"Candidate: {', '.join([a['name'] for a in item['artists']])} - {item['name']} ({confidence}%)")
                    
                    # Accept match based on confidence
                    # Minimum threshold: don't suggest garbage matches
                    MIN_SUGGESTION_CONFIDENCE = 40
                    
                    if best_match and best_confidence >= MIN_SUGGESTION_CONFIDENCE:
                        track_info = {
                            'uri': best_match['uri'],
                            'input': line,
                            'found': f"{', '.join([a['name'] for a in best_match['artists']])} - {best_match['name']}",
                            'confidence': best_confidence,
                            'line_num': line_num  # Preserve original order
                        }
                        
                        if best_confidence >= CONFIDENCE_THRESHOLD_AUTO_ACCEPT:
                            track_uris.append(track_info)
                        else:
                            # Low confidence - save for manual review
                            low_confidence_matches.append(track_info)
                    else:
                        # No match above minimum threshold
                        not_found.append(line)
                else:
                    not_found.append(line)
            except Exception as e:
                logger.error(f"Error searching for '{line}': {e}")
                not_found.append(line)
            
            progress.advance(task)
            line_num += 1

    # Report results
    console.print(f"\n[bold]Found {len(track_uris)} high-confidence matches.[/bold]")
    
    # Handle low-confidence matches
    if low_confidence_matches:
        console.print(f"\n[yellow]Found {len(low_confidence_matches)} low-confidence matches:[/yellow]")
        for match in low_confidence_matches:
            console.print(f"  Input: [cyan]{match['input']}[/cyan]")
            console.print(f"  Found: [magenta]{match['found']}[/magenta] (confidence: {match['confidence']}%)")
            if yes or spotifaj_functions.confirm("  Include this track?", default=False):
                track_uris.append(match)
            console.print()
    
    # Sort all tracks by original line number to preserve input order
    track_uris.sort(key=lambda x: x['line_num'])
    
    # Extract just the URIs in the correct order
    final_track_uris = [t['uri'] for t in track_uris]
    
    if not_found:
        console.print(f"[red]Could not find {len(not_found)} tracks:[/red]")
        for nf in not_found[:10]:
            console.print(f"  - {nf}")
        if len(not_found) > 10:
            console.print(f"  ... and {len(not_found) - 10} more.")

    if not final_track_uris:
        logger.warning("No tracks found to add.")
        return

    # Create playlist
    if yes or spotifaj_functions.confirm(f"Create playlist '{name}' with {len(final_track_uris)} tracks?", default=True):
        playlist_id = spotifaj_functions.create_playlist(username, name, sp=sp)
        if playlist_id:
            spotifaj_functions.add_song_to_spotify_playlist(username, final_track_uris, playlist_id, sp=sp)
            console.print(f"[bold green]Successfully created playlist '{name}'![/bold green]")
            
            # Offer to add cover image
            if spotifaj_functions.confirm("\nWould you like to add a cover image?", default=False):
                image_url = click.prompt("Enter image URL (JPEG/PNG)", type=str)
                try:
                    upload_playlist_cover(sp, playlist_id, image_url)
                    console.print("[green]Cover image uploaded successfully![/green]")
                except Exception as e:
                    console.print(f"[red]Failed to upload cover image: {e}[/red]")
        else:
            logger.error("Failed to create playlist.")

@spotifaj.command()
@click.argument('playlist_input')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
def analytics(playlist_input, username):
    """
    Analyze playlist statistics and metadata.
    
    Shows duration, genre distribution, decade analysis, artist frequency, and more.
    PLAYLIST_INPUT can be a Spotify Playlist URL, ID, or Name.
    """
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    # Parse playlist input
    playlist_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', playlist_input)
    if match:
        playlist_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', playlist_input):
        playlist_id = playlist_input
    
    if not playlist_id:
        logger.info(f"Searching for playlist with name: '{playlist_input}'...")
        playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_input)

    if not playlist_id:
        logger.error(f"Could not find playlist: {playlist_input}")
        sys.exit(1)

    try:
        # Get playlist info
        playlist_info = sp.playlist(playlist_id)
        playlist_name = playlist_info['name']
        
        console.print(f"\n[bold cyan]Analyzing playlist: {playlist_name}[/bold cyan]\n")
        
        # Fetch all tracks
        results = sp.playlist_items(playlist_id)
        tracks = results['items']
        
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        
        # Filter valid tracks
        valid_tracks = [item for item in tracks if item and 'track' in item and item['track']]
        total_tracks = len(valid_tracks)
        
        if total_tracks == 0:
            console.print("[yellow]No tracks found in playlist.[/yellow]")
            return
        
        # === Duration Statistics ===
        durations = [t['track']['duration_ms'] for t in valid_tracks]
        total_duration_ms = sum(durations)
        avg_duration_ms = total_duration_ms / total_tracks
        min_duration_ms = min(durations)
        max_duration_ms = max(durations)
        
        def format_duration(ms):
            hours, remainder = divmod(ms // 1000, 3600)
            minutes, seconds = divmod(remainder, 60)
            if hours > 0:
                return f"{hours}h {minutes}m {seconds}s"
            return f"{minutes}m {seconds}s"
        
        console.print("[bold]Duration Statistics:[/bold]")
        console.print(f"  Total Duration: [cyan]{format_duration(total_duration_ms)}[/cyan]")
        console.print(f"  Average Track: [cyan]{format_duration(avg_duration_ms)}[/cyan]")
        console.print(f"  Shortest: [cyan]{format_duration(min_duration_ms)}[/cyan]")
        console.print(f"  Longest: [cyan]{format_duration(max_duration_ms)}[/cyan]")
        console.print()
        
        # === Decade Distribution ===
        decades = {}
        unknown_year = 0
        
        for item in valid_tracks:
            track = item['track']
            release_date = track['album'].get('release_date', '')
            if release_date and len(release_date) >= 4:
                try:
                    year = int(release_date[:4])
                    decade = (year // 10) * 10
                    decades[decade] = decades.get(decade, 0) + 1
                except ValueError:
                    unknown_year += 1
            else:
                unknown_year += 1
        
        console.print("[bold]Decade Distribution:[/bold]")
        for decade in sorted(decades.keys()):
            count = decades[decade]
            percentage = (count / total_tracks) * 100
            bar = '█' * int(percentage / 2)
            console.print(f"  {decade}s: [green]{bar}[/green] {count} ({percentage:.1f}%)")
        if unknown_year > 0:
            percentage = (unknown_year / total_tracks) * 100
            console.print(f"  Unknown: {unknown_year} ({percentage:.1f}%)")
        console.print()
        
        # === Artist Frequency ===
        artist_counts = {}
        for item in valid_tracks:
            track = item['track']
            for artist in track['artists']:
                name = artist['name']
                artist_counts[name] = artist_counts.get(name, 0) + 1
        
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        console.print("[bold]Top 10 Artists:[/bold]")
        for i, (artist, count) in enumerate(top_artists, 1):
            percentage = (count / total_tracks) * 100
            console.print(f"  {i:>2}. [magenta]{artist}[/magenta]: {count} tracks ({percentage:.1f}%)")
        console.print()
        
        # === Popularity Statistics ===
        popularities = [t['track'].get('popularity', 0) for t in valid_tracks]
        avg_popularity = sum(popularities) / len(popularities) if popularities else 0
        
        console.print("[bold]Popularity:[/bold]")
        console.print(f"  Average Popularity: [cyan]{avg_popularity:.1f}/100[/cyan]")
        console.print()
        
        # === Explicit Content ===
        explicit_count = sum(1 for item in valid_tracks if item['track'].get('explicit', False))
        explicit_pct = (explicit_count / total_tracks) * 100
        
        console.print("[bold]Content:[/bold]")
        console.print(f"  Explicit Tracks: [yellow]{explicit_count}[/yellow] ({explicit_pct:.1f}%)")
        console.print(f"  Clean Tracks: [green]{total_tracks - explicit_count}[/green] ({100 - explicit_pct:.1f}%)")
        console.print()
        
        # === Album Diversity ===
        unique_albums = set()
        for item in valid_tracks:
            track = item['track']
            unique_albums.add(track['album']['id'])
        
        console.print("[bold]Diversity:[/bold]")
        console.print(f"  Unique Artists: [cyan]{len(artist_counts)}[/cyan]")
        console.print(f"  Unique Albums: [cyan]{len(unique_albums)}[/cyan]")
        console.print(f"  Tracks per Artist (avg): [cyan]{total_tracks / len(artist_counts):.1f}[/cyan]")
        console.print()
        
    except Exception as e:
        logger.error(f"Error analyzing playlist: {e}")

@spotifaj.command()
@click.argument('playlists', nargs=-1, required=False)
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--batch', is_flag=True, help="Auto-update multiple playlists (provide playlist names as arguments).")
@click.option('--file', '-f', 'playlist_file', type=click.Path(exists=True), help="Read playlist names from file (one per line).")
@click.option('--dry-run', is_flag=True, help="Show what would be added without actually updating.")
def auto_update(playlists, username, batch, playlist_file, dry_run):
    """
    Auto-update label playlists with new releases since last run.
    
    Tracks the last update time and only adds tracks released after that.
    Perfect for keeping label playlists current with new releases.
    
    Examples:
      # Single playlist
      spotifaj auto-update "Warp Records"
      
      # Batch update multiple playlists
      spotifaj auto-update --batch "Warp Records" "Ninja Tune" "Kompakt"
      
      # Batch update from file
      spotifaj auto-update --batch --file playlists.txt
    """
    try:
        from utils.auto_update_tracker import AutoUpdateTracker
    except ImportError:
        logger.error("Auto-update tracker module not found.")
        return

    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    tracker = AutoUpdateTracker()
    
    # Load playlist names from file if provided
    playlist_list = list(playlists) if playlists else []
    if playlist_file:
        try:
            with open(playlist_file, 'r') as f:
                file_playlists = []
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Handle tab-separated format (from export-playlist-names --verbose)
                    # Take only the first column (playlist name)
                    playlist_name = line.split('\t')[0].strip()
                    if playlist_name:
                        file_playlists.append(playlist_name)
                playlist_list.extend(file_playlists)
        except Exception as e:
            console.print(f"[red]Error reading file '{playlist_file}': {e}[/red]")
            return
    
    # Batch mode: update multiple playlists by exact name
    if batch:
        if not playlist_list or len(playlist_list) == 0:
            console.print("[red]Error: --batch requires playlist names or --file option.[/red]")
            console.print("[dim]Usage: spotifaj auto-update --batch \"Playlist 1\" \"Playlist 2\"[/dim]")
            console.print("[dim]       spotifaj auto-update --batch --file playlists.txt[/dim]")
            return
        
        console.print(f"[bold cyan]Batch updating {len(playlist_list)} playlists...[/bold cyan]")
        console.print(f"[dim]Fetching all playlists...[/dim]")
        
        # Fetch all playlists once to avoid rate limiting
        all_playlists = spotifaj_functions.fetch_all_user_playlists(username)
        playlist_name_to_id = {p['name']: p['id'] for p in all_playlists}
        
        console.print(f"[dim]Checking {len(playlist_list)} playlists against tracker...[/dim]")
        
        # Get tracker metadata for all playlists
        tracked_playlists = tracker.get_all_tracked()
        
        # Build list of playlists to update with their metadata
        playlists_to_update = []
        # Track skipped playlists for summary
        not_found = []
        not_tracked = []
        recently_updated = []  # Updated within last 24 hours
        
        # Get current time for 24-hour check
        from datetime import datetime, timedelta
        now = datetime.now()
        cutoff_time = now - timedelta(hours=24)
        
        for playlist_name in playlist_list:
            # Look up playlist ID in memory (no API call)
            playlist_id = playlist_name_to_id.get(playlist_name)
            
            if not playlist_id:
                not_found.append(playlist_name)
                continue
            
            # Check if tracked and has label metadata
            if playlist_id not in tracked_playlists:
                not_tracked.append(playlist_name)
                continue
            
            # Check if updated within last 24 hours
            last_update_str = tracker.get_last_update(playlist_id)
            if last_update_str:
                try:
                    last_update = datetime.fromisoformat(last_update_str)
                    if last_update > cutoff_time:
                        recently_updated.append(playlist_name)
                        continue
                except (ValueError, TypeError):
                    # Invalid timestamp, continue with update
                    pass
            
            metadata = tracked_playlists[playlist_id].get('metadata', {})
            stored_label = metadata.get('label')
            
            if not stored_label:
                console.print(f"[yellow]⚠ Playlist '{playlist_name}' has no label metadata. Skipping.[/yellow]")
                continue
            
            playlists_to_update.append({
                'id': playlist_id,
                'name': playlist_name,
                'label': stored_label
            })
        
        # Show summary of skipped playlists
        if recently_updated:
            console.print(f"\n[dim]⏭ {len(recently_updated)} playlist(s) updated within last 24 hours (skipping):[/dim]")
            for name in recently_updated:
                console.print(f"[dim]  • {name}[/dim]")
        
        if not_tracked:
            console.print(f"\n[yellow]⚠ {len(not_tracked)} playlist(s) exist but not tracked (run auto-update first to initialize):[/yellow]")
            for name in not_tracked:
                console.print(f"[dim]  • {name}[/dim]")
        
        if not_found:
            console.print(f"\n[dim]Skipped {len(not_found)} playlist(s) not found (create them first)[/dim]")
        
        if not playlists_to_update:
            console.print("[red]No valid playlists to update.[/red]")
            return
        
        console.print(f"[green]Found {len(playlists_to_update)} playlists to update:[/green]")
        for p in playlists_to_update:
            console.print(f"  • {p['name']} (label: {p['label']})")
        
        if not dry_run:
            if not spotifaj_functions.confirm(f"\nUpdate all {len(playlists_to_update)} playlists?", default=True):
                console.print("[yellow]Batch update cancelled.[/yellow]")
                return
        
        # Update each playlist with spinner progress
        successful = 0
        failed = 0
        skipped = 0
        
        from rich.progress import Progress, SpinnerColumn, TextColumn
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=False
        ) as progress:
            task = progress.add_task("[cyan]Starting batch update...", total=len(playlists_to_update))
            
            for i, p in enumerate(playlists_to_update, 1):
                progress.update(task, description=f"[cyan]Processing [{i}/{len(playlists_to_update)}]: {p['name']}")
                
                try:
                    result = _auto_update_single(
                        sp, tracker, p['label'], username, 
                        p['name'], p['id'], dry_run
                    )
                    if result == 'success':
                        successful += 1
                    elif result == 'skipped':
                        skipped += 1
                    else:
                        failed += 1
                except Exception as e:
                    console.print(f"[red]Error updating {p['name']}: {e}[/red]")
                    failed += 1
                
                progress.advance(task)
        
        # Summary
        console.print(f"\n[bold]--- Batch Update Summary ---[/bold]")
        console.print(f"Total: {len(playlists_to_update)} | [green]Updated: {successful}[/green] | [yellow]Skipped: {skipped}[/yellow] | [red]Failed: {failed}[/red]")
        
        # Show recently updated playlists
        if recently_updated:
            console.print(f"\n[dim]Recently Updated ({len(recently_updated)}):[/dim]")
            console.print("[dim]These playlists were updated within the last 24 hours.[/dim]\n")
            for name in recently_updated:
                console.print(f"[dim]  • {name}[/dim]")
        
        # Show untracked playlists at end for easy reference
        if not_tracked:
            console.print(f"\n[bold yellow]Untracked Playlists ({len(not_tracked)}):[/bold yellow]")
            console.print("[dim]These playlists exist but haven't been initialized for auto-update.[/dim]")
            console.print("[dim]Initialize them by running: spotifaj auto-update \"Playlist Name\"[/dim]\n")
            for name in not_tracked:
                console.print(f"  • {name}")
        
        return
    
    # Single playlist mode
    if not playlist_list or len(playlist_list) != 1:
        console.print("[red]Error: Provide exactly one playlist name, or use --batch for multiple.[/red]")
        console.print("[dim]Usage: spotifaj auto-update \"Playlist Name\"[/dim]")
        console.print("[dim]       spotifaj auto-update --batch \"Playlist 1\" \"Playlist 2\"[/dim]")
        console.print("[dim]       spotifaj auto-update --batch --file playlists.txt[/dim]")
        return
    
    playlist_name = playlist_list[0]
    
    # Find or create playlist
    playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_name)
    
    if not playlist_id:
        if dry_run:
            console.print(f"[yellow]Playlist '{playlist_name}' does not exist. Would create it in live mode.[/yellow]")
            # Exit in dry-run mode
            return
        else:
            if spotifaj_functions.confirm(f"Playlist '{playlist_name}' not found. Create it?", default=True):
                logger.info(f"Creating playlist '{playlist_name}'...")
                playlist_id = spotifaj_functions.create_playlist(username, playlist_name)
                if not playlist_id:
                    logger.error("Failed to create playlist.")
                    return
            else:
                logger.info("Operation cancelled.")
                return
    
    # Get label from tracker metadata or use playlist name
    label = tracker.get_metadata(playlist_id, 'label') if playlist_id else None
    if not label:
        label = playlist_name
        console.print(f"[dim]No label metadata found. Using playlist name '{label}' as label.[/dim]")
    
    # Use the helper function for single playlist update
    try:
        result = _auto_update_single(sp, tracker, label, username, playlist_name, playlist_id, dry_run)
        if result == 'success':
            from datetime import datetime
            console.print(f"[dim]Tracker updated. Next run will only fetch tracks after {datetime.now().isoformat()}[/dim]")
        elif result == 'failed':
            logger.error("Auto-update failed.")
    except Exception as e:
        logger.error(f"Error during auto-update: {e}")

def _auto_update_single(sp, tracker, label, username, playlist_name, playlist_id=None, dry_run=False):
    """
    Helper function to auto-update a single playlist.
    Returns 'success', 'skipped', or 'failed'.
    """
    from datetime import datetime, timedelta
    
    # Find or verify playlist
    if not playlist_id:
        playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_name)
    
    if not playlist_id:
        if dry_run:
            console.print(f"[yellow]Playlist '{playlist_name}' does not exist. Would create it in live mode.[/yellow]")
        else:
            console.print(f"[yellow]Playlist '{playlist_name}' not found. Skipping.[/yellow]")
            return 'skipped'
    
    # Get last update time
    last_update = tracker.get_last_update(playlist_id) if playlist_id else None
    
    if last_update:
        console.print(f"[dim]Last update: {last_update}[/dim]")
        last_date = datetime.fromisoformat(last_update).strftime('%Y-%m-%d')
    else:
        console.print(f"[dim]First time update. Fetching recent releases.[/dim]")
        last_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Search for new releases
    console.print(f"[dim]Searching for releases from {label} since {last_date}...[/dim]")
    
    try:
        query = f'label:"{label}"'
        candidate_tracks = []
        offset = 0
        limit = 50
        label_lower = label.lower()
        
        # First pass: collect candidate tracks with recent release dates
        while True:
            results = sp.search(q=query, type='track', limit=limit, offset=offset, market='US')
            items = results['tracks']['items']
            
            if not items:
                break
            
            for track in items:
                release_date = track['album'].get('release_date', '')
                if release_date >= last_date:
                    candidate_tracks.append(track)
            
            if len(items) < limit:
                break
            
            offset += limit
            if offset >= 1000:
                break
        
        # Second pass: verify label field by fetching full album details
        found_tracks = []
        if candidate_tracks:
            # Get unique album IDs
            album_ids = list(set(track['album']['id'] for track in candidate_tracks))
            
            # Fetch album details in batches of 20 (Spotify API limit)
            album_labels = {}
            for i in range(0, len(album_ids), 20):
                batch = album_ids[i:i+20]
                albums_data = sp.albums(batch)
                if albums_data and 'albums' in albums_data:
                    for album in albums_data['albums']:
                        if album:
                            album_labels[album['id']] = album.get('label', '').lower()
            
            # Filter tracks by label field
            for track in candidate_tracks:
                album_id = track['album']['id']
                album_label = album_labels.get(album_id, '').lower()
                
                # Only accept if label field actually matches
                if (album_label == label_lower or 
                    album_label.startswith(label_lower + ' ') or
                    album_label.startswith(label_lower + '-') or
                    label_lower in album_label):
                    found_tracks.append(track)
        
        found_tracks.sort(key=lambda t: t['album'].get('release_date', ''), reverse=True)
        
        if not found_tracks:
            console.print("[dim]No new tracks found.[/dim]")
            if playlist_id:
                tracker.set_last_update(playlist_id)
                tracker.set_metadata(playlist_id, 'label', label)
                if not last_update:
                    console.print(f"[green]✓ Initialized tracking for '{playlist_name}'[/green]")
                    console.print(f"[dim]Next update will only fetch tracks after {datetime.now().strftime('%Y-%m-%d')}[/dim]")
            return 'skipped'
        
        # Filter duplicates if playlist exists
        if playlist_id:
            # 1. ID Check (Fast)
            existing_ids = spotifaj_functions.get_playlist_track_ids(username, playlist_id)
            new_tracks = [t for t in found_tracks if t['id'] not in existing_ids]
            
            id_dupes = len(found_tracks) - len(new_tracks)
            
            # 2. Metadata Check (Slower but catches cross-album duplicates)
            if new_tracks:
                existing_signatures = spotifaj_functions.get_playlist_track_signatures(username, playlist_id)
                unique_tracks = []
                for t in new_tracks:
                    sig = spotifaj_functions.create_track_signature(t)
                    if sig not in existing_signatures:
                        unique_tracks.append(t)
                        # Add to signatures to prevent duplicates within the new batch itself
                        existing_signatures.add(sig)
                
                metadata_dupes = len(new_tracks) - len(unique_tracks)
                new_tracks = unique_tracks
            else:
                metadata_dupes = 0
            
            total_dupes = id_dupes + metadata_dupes
            if total_dupes > 0:
                console.print(f"[dim]Filtered {total_dupes} duplicates ({id_dupes} by ID, {metadata_dupes} by metadata)[/dim]")
            
            found_tracks = new_tracks
        
        if not found_tracks:
            console.print("[dim]All tracks already in playlist.[/dim]")
            if playlist_id:
                tracker.set_last_update(playlist_id)
                tracker.set_metadata(playlist_id, 'label', label)
                if not last_update:
                    console.print(f"[green]✓ Initialized tracking for '{playlist_name}'[/green]")
                    console.print(f"[dim]Next update will only fetch tracks after {datetime.now().strftime('%Y-%m-%d')}[/dim]")
            return 'skipped'
        
        console.print(f"[green]Found {len(found_tracks)} new tracks[/green]")
        
        # Show sample
        if len(found_tracks) <= 5:
            for track in found_tracks:
                artists = ", ".join([a['name'] for a in track['artists']])
                console.print(f"  • {artists} - {track['name']}")
        else:
            for track in found_tracks[:3]:
                artists = ", ".join([a['name'] for a in track['artists']])
                console.print(f"  • {artists} - {track['name']}")
            console.print(f"  ... and {len(found_tracks) - 3} more")
        
        if dry_run:
            console.print(f"[yellow]Dry run: Would add {len(found_tracks)} tracks[/yellow]")
            return 'skipped'
        
        # Validate tracks before adding (silently filter out non-matching labels)
        console.print(f"[dim]Validating tracks against label '{label}'...[/dim]")
        validated_ids = spotifaj_functions.validate_tracks_list(sp, found_tracks, label, auto_mode=True)
        
        if not validated_ids:
            console.print("[yellow]No valid tracks after label validation.[/yellow]")
            # Still update tracker to avoid re-checking same tracks
            tracker.set_last_update(playlist_id)
            tracker.set_metadata(playlist_id, 'label', label)
            return 'skipped'
        
        if len(validated_ids) < len(found_tracks):
            filtered_count = len(found_tracks) - len(validated_ids)
            console.print(f"[dim]Filtered {filtered_count} tracks with non-matching labels[/dim]")
        
        # Add tracks
        spotifaj_functions.add_song_to_spotify_playlist(username, validated_ids, playlist_id)
        console.print(f"[bold green]✓ Added {len(validated_ids)} tracks[/bold green]")
        
        # Update tracker
        tracker.set_last_update(playlist_id)
        tracker.set_metadata(playlist_id, 'label', label)
        
        return 'success'
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception(f"Failed to update {playlist_name}")
        return 'failed'

@spotifaj.command()
@click.argument('playlist_input')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--limit', default=50, help="Number of recommendations to generate.")
@click.option('-n', '--name', 'target_name', default=None, help="Custom name for the recommendations playlist.")
def recommend(playlist_input, username, limit, target_name):
    """
    Generate track recommendations based on a playlist's artists and genres.
    
    Analyzes the playlist's top artists and their genres to find similar tracks.
    Automatically creates a new playlist with recommendations (min 10 tracks).
    
    PLAYLIST_INPUT can be a Spotify Playlist URL, ID, or Name.
    """
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    # Parse playlist input
    playlist_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', playlist_input)
    if match:
        playlist_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', playlist_input):
        playlist_id = playlist_input
    
    if not playlist_id:
        logger.info(f"Searching for playlist with name: '{playlist_input}'...")
        playlist_id = spotifaj_functions.find_playlist_by_name(username, playlist_input)

    if not playlist_id:
        logger.error(f"Could not find playlist: {playlist_input}")
        sys.exit(1)

    try:
        # Get playlist info
        playlist_info = sp.playlist(playlist_id)
        playlist_name = playlist_info['name']
        
        console.print(f"\n[bold cyan]Analyzing playlist: {playlist_name}[/bold cyan]")
        
        # Fetch all tracks
        results = sp.playlist_items(playlist_id)
        tracks = results['items']
        
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        
        # Filter valid tracks
        valid_tracks = [item['track'] for item in tracks if item and 'track' in item and item['track']]
        
        if len(valid_tracks) == 0:
            console.print("[yellow]No tracks found in playlist.[/yellow]")
            return
        
        # Scale analysis based on playlist size
        # Small playlists (<= 50): analyze ALL tracks
        # Large playlists: analyze up to 80%
        if len(valid_tracks) <= 50:
            analysis_count = len(valid_tracks)
        else:
            analysis_count = min(len(valid_tracks), int(len(valid_tracks) * 0.8))
        
        track_ids = [t['id'] for t in valid_tracks[:analysis_count]]
        
        console.print(f"[cyan]Analyzing {len(track_ids)} of {len(valid_tracks)} tracks...[/cyan]")
        console.print("[dim]Note: Audio features unavailable in Development Mode (requires Extended Quota)[/dim]\n")
        
        # Get top artists from playlist (analyze at least 50%, all for small playlists)
        if len(valid_tracks) <= 50:
            artist_analysis_count = len(valid_tracks)
        else:
            artist_analysis_count = min(len(valid_tracks), max(50, int(len(valid_tracks) * 0.5)))
        
        artist_counts = {}
        for track in valid_tracks[:artist_analysis_count]:
            for artist in track['artists']:
                artist_id = artist['id']
                artist_counts[artist_id] = artist_counts.get(artist_id, 0) + 1
        
        # Get top 25 artists, then randomly select 2-4 from them for variety
        import random
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:25]
        num_seeds = random.randint(2, min(4, len(top_artists)))
        artist_seeds = random.sample([a[0] for a in top_artists], num_seeds)
        
        console.print(f"[cyan]Finding similar tracks based on playlist artists and genres...[/cyan]")
        
        # Use search-based recommendations (Spotify Recommendations API blocked in Development Mode)
        rec_tracks = []
        try:
            search_artists = []
            all_genres = []
            
            # Get artist names and genres
            for artist_id in artist_seeds:
                artist_info = sp.artist(artist_id)
                search_artists.append(artist_info['name'])
                all_genres.extend(artist_info.get('genres', []))
            
            # Randomly select which genres to search (for variety between runs)
            unique_genres = list(set(all_genres))
            random.shuffle(unique_genres)
            genres_to_search = unique_genres[:random.randint(2, min(5, len(unique_genres)))]
            
            # Randomly shuffle artists to search in different order each time
            random.shuffle(search_artists)
            
            # Search using artist names and genres
            all_results = []
            for artist_name in search_artists[:3]:  # Use up to 3 random artists
                query = f'artist:"{artist_name}"'
                # Vary the limit randomly for more variety
                search_limit = random.randint(15, 30)
                results = sp.search(q=query, type='track', limit=search_limit)
                if results and 'tracks' in results and 'items' in results['tracks']:
                    all_results.extend(results['tracks']['items'])
            
            # Also search by genre if available
            for genre in genres_to_search:
                query = f'genre:"{genre}"'
                try:
                    # Vary search limit for different results each time
                    genre_limit = random.randint(10, 25)
                    results = sp.search(q=query, type='track', limit=genre_limit)
                    if results and 'tracks' in results and 'items' in results['tracks']:
                        all_results.extend(results['tracks']['items'])
                except:
                    pass
            
            # Deduplicate by track ID and track name (same song, different albums)
            seen_ids = set()
            seen_names = set()
            unique_results = []
            for track in all_results:
                track_name_normalized = track['name'].lower().strip()
                if track['id'] not in seen_ids and track_name_normalized not in seen_names:
                    seen_ids.add(track['id'])
                    seen_names.add(track_name_normalized)
                    unique_results.append(track)
            
            # Apply diversity filtering: limit tracks per album and artist
            album_counts = {}
            artist_counts = {}  # Track all artists in a song, not just primary
            diverse_results = []
            
            random.shuffle(unique_results)  # Randomize before filtering
            
            for track in unique_results:
                album_id = track['album']['id']
                
                # Get all artist IDs from the track (including featured artists)
                all_artist_ids = [a['id'] for a in track['artists'] if a and 'id' in a]
                
                # Check if album limit reached (max 1 per album)
                if album_counts.get(album_id, 0) >= 1:
                    continue
                
                # Check artist limits - be strict about variety
                artist_limit_reached = False
                for artist_id in all_artist_ids:
                    current_count = artist_counts.get(artist_id, 0)
                    # Most artists get only 1 track (strict diversity)
                    # Only 20% chance to allow a 2nd track from same artist
                    if current_count >= 1:
                        if current_count >= 2 or random.random() > 0.2:
                            artist_limit_reached = True
                            break
                
                if artist_limit_reached:
                    continue
                
                # Add track and update counts for ALL artists
                diverse_results.append(track)
                album_counts[album_id] = album_counts.get(album_id, 0) + 1
                for artist_id in all_artist_ids:
                    artist_counts[artist_id] = artist_counts.get(artist_id, 0) + 1
                
                # Stop when we have enough
                if len(diverse_results) >= limit * 2:  # Get 2x to account for playlist filtering
                    break
            
            rec_tracks = diverse_results[:limit]
            
        except Exception as search_error:
            logger.error(f"Search-based recommendations failed: {search_error}")
            console.print(f"[red]Could not generate recommendations: {search_error}[/red]")
            return
        
        if not rec_tracks:
            console.print("[yellow]No recommendations found.[/yellow]")
            return
        
        # Filter out tracks already in playlist
        existing_uris = set(t['uri'] for t in valid_tracks)
        new_recs = [t for t in rec_tracks if t['uri'] not in existing_uris]
        
        console.print(f"\n[bold green]Found {len(new_recs)} new recommendations:[/bold green]")
        
        # Display recommendations with album info for diversity verification
        for i, track in enumerate(new_recs[:20], 1):
            artists = ", ".join([a['name'] for a in track['artists']])
            album = track['album']['name']
            console.print(f"  {i:>2}. [magenta]{track['name']}[/magenta] - [green]{artists}[/green]")
            console.print(f"      [dim]from {album}[/dim]")
        
        if len(new_recs) > 20:
            console.print(f"  ... and {len(new_recs) - 20} more")
        
        # Create playlist if we have enough quality recommendations (with confirmation)
        if len(new_recs) >= 10:
            playlist_name_safe = playlist_name[:50]  # Limit length
            rec_playlist_name = target_name if target_name else f"{playlist_name_safe} — Recommendations"
            
            if spotifaj_functions.confirm(f"\nCreate playlist '{rec_playlist_name}' with {len(new_recs)} tracks?", default=True):
                rec_playlist_id = spotifaj_functions.create_playlist(username, rec_playlist_name)
                if rec_playlist_id:
                    rec_track_ids = [t['id'] for t in new_recs]
                    spotifaj_functions.add_song_to_spotify_playlist(username, rec_track_ids, rec_playlist_id)
                    console.print(f"[bold green]✓ Created playlist '{rec_playlist_name}' with {len(rec_track_ids)} tracks[/bold green]")
                else:
                    console.print("[red]✗ Failed to create playlist[/red]")
            else:
                console.print("[yellow]Playlist creation cancelled[/yellow]")
        else:
            console.print(f"\n[yellow]Not enough recommendations ({len(new_recs)}) to create playlist (min 10 required)[/yellow]")
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")

@spotifaj.command()
@click.option('--shell', type=click.Choice(['bash', 'zsh', 'fish']), default='zsh', help="Target shell.")
def install_completion(shell):
    """
    Install shell completion for spotifaj.py.
    """
    import os
    from pathlib import Path
    
    script_path = os.path.abspath(sys.argv[0])
    prog_name = os.path.basename(script_path)
    # Click uses the script name, uppercased, with dots replaced by underscores
    env_var = f"_{prog_name.replace('.', '_').replace('-', '_').upper()}_COMPLETE"
    
    if shell == 'zsh':
        rc_file = Path.home() / '.zshrc'
        cmd = f'eval "$({env_var}=zsh_source {script_path})"'
    elif shell == 'bash':
        rc_file = Path.home() / '.bashrc'
        cmd = f'eval "$({env_var}=bash_source {script_path})"'
    elif shell == 'fish':
        rc_file = Path.home() / '.config/fish/completions/spotifaj.fish'
        cmd = f'eval ({env_var}=fish_source {script_path})'
        
    console.print(f"Detected script path: [cyan]{script_path}[/cyan]")
    
    if shell in ['bash', 'zsh']:
        console.print(f"To enable completion, run this command (or add it to {rc_file}):")
        console.print(f"\n    [green]{cmd}[/green]\n")
        
        # Add wrapper hint
        wrapper_path = os.path.join(os.path.dirname(script_path), 'spotifaj')
        if os.path.exists(wrapper_path):
             console.print("[yellow]Note: Since you are using the './spotifaj' wrapper, use this instead to fix the command name:[/yellow]")
             wrapper_cmd = f'_SPOTIFAJ_PY_COMPLETE=zsh_source "{wrapper_path}" | sed "s/spotifaj\\.py/spotifaj/g" > ~/.spotifaj-complete.zsh && source ~/.spotifaj-complete.zsh'
             console.print(f"    [green]{wrapper_cmd}[/green]\n")

        if click.confirm(f"Append this to {rc_file}?"):
            try:
                with open(rc_file, 'a') as f:
                    f.write(f"\n# spotifaj completion\n{cmd}\n")
                console.print(f"[bold green]Added to {rc_file}. Restart your shell or run 'source {rc_file}' to apply.[/bold green]")
            except Exception as e:
                console.print(f"[red]Failed to write to {rc_file}: {e}[/red]")
    else:
        console.print(f"For fish, run:\n    {cmd}")

@spotifaj.command()
@click.argument('source_playlist')
@click.argument('target_playlist')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--remove-extra', is_flag=True, help="Remove tracks from target that aren't in source.")
@click.option('--preserve-order', is_flag=True, help="Reorder target to match source order (only if tracks are identical).")
@click.option('--dry-run', is_flag=True, help="Show what would be done without making changes.")
def sync_playlist(source_playlist, target_playlist, username, remove_extra, preserve_order, dry_run):
    """
    Sync target playlist with source playlist.
    
    Makes the target playlist match the source playlist by adding missing tracks.
    Optionally removes extra tracks and preserves order.
    
    Examples:
      # Add missing tracks from source to target
      spotifaj sync-playlist "Source Playlist" "Target Playlist"
      
      # Make target exactly match source (add + remove)
      spotifaj sync-playlist "Master" "Backup" --remove-extra
      
      # Preview changes without making them
      spotifaj sync-playlist "Source" "Target" --dry-run
      
      # Match both content and order
      spotifaj sync-playlist "Source" "Target" --remove-extra --preserve-order
    """
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
    
    # Resolve source playlist ID
    source_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', source_playlist)
    if match:
        source_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', source_playlist):
        source_id = source_playlist
    else:
        # Try exact match first, then fuzzy match
        source_id = spotifaj_functions.find_playlist_by_name_fuzzy(username, source_playlist)
    
    if not source_id:
        logger.error(f"Could not find source playlist: {source_playlist}")
        console.print("\n[red]Error:[/red] Could not find source playlist: {source_playlist}")
        console.print("\n[yellow]Note:[/yellow] If this is a Spotify-curated playlist (Discover Weekly, Release Radar, etc.):")
        console.print("       These are [bold]completely inaccessible[/bold] to apps without 'extended quota mode'.")
        console.print("       See SPOTIFY_PLAYLIST_RESTRICTIONS.md for details and workarounds.")
        sys.exit(1)
    
    # Resolve target playlist ID
    target_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', target_playlist)
    if match:
        target_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', target_playlist):
        target_id = target_playlist
    else:
        # Try exact match first, then fuzzy match
        target_id = spotifaj_functions.find_playlist_by_name_fuzzy(username, target_playlist)
    
    if not target_id:
        logger.error(f"Could not find target playlist: {target_playlist}")
        console.print("\n[red]Error:[/red] Could not find target playlist: {target_playlist}")
        console.print("\n[yellow]Note:[/yellow] If this is a Spotify-curated playlist (Discover Weekly, Release Radar, etc.):")
        console.print("       These are [bold]completely inaccessible[/bold] to apps without 'extended quota mode'.")
        console.print("       See SPOTIFY_PLAYLIST_RESTRICTIONS.md for details and workarounds.")
        sys.exit(1)
    
    # Get playlist names for display
    try:
        source_info = sp.playlist(source_id)
        target_info = sp.playlist(target_id)
        source_name = source_info['name']
        target_name = target_info['name']
    except Exception as e:
        logger.error(f"Error fetching playlist info: {e}")
        if "404" in str(e) or "not found" in str(e).lower():
            console.print("\n[red]Error:[/red] Spotify returned 404 (Not Found) for one of the playlists.")
            console.print("\n[yellow]This usually means:[/yellow]")
            console.print("  • The playlist is Spotify-curated (Discover Weekly, Release Radar, etc.)")
            console.print("  • Your app lacks 'extended quota mode' to access it")
            console.print("  • See [cyan]SPOTIFY_PLAYLIST_RESTRICTIONS.md[/cyan] for solutions")
        sys.exit(1)
    
    console.print(f"\n[bold cyan]Syncing playlists:[/bold cyan]")
    console.print(f"  Source: [green]{source_name}[/green]")
    console.print(f"  Target: [yellow]{target_name}[/yellow]")
    console.print()
    
    # Perform sync
    result = spotifaj_functions.sync_playlists(
        username=username,
        source_playlist_id=source_id,
        target_playlist_id=target_id,
        remove_extra=remove_extra,
        preserve_order=preserve_order,
        dry_run=True  # Always dry run first to show changes
    )
    
    if 'error' in result:
        logger.error(result['error'])
        sys.exit(1)
    
    # Display summary
    console.print(f"[bold]Summary:[/bold]")
    console.print(f"  Source tracks: [cyan]{result['source_count']}[/cyan]")
    console.print(f"  Target tracks: [cyan]{result['target_count']}[/cyan]")
    console.print()
    
    to_add = result['to_add']
    to_remove = result['to_remove']
    
    if to_add:
        console.print(f"[green]Tracks to add ({len(to_add)}):[/green]")
        for track in result['to_add_tracks'][:10]:  # Show first 10
            artists = ", ".join([a['name'] for a in track['artists']])
            console.print(f"  + {artists} - {track['name']}")
        if len(to_add) > 10:
            console.print(f"  ... and {len(to_add) - 10} more")
        console.print()
    
    if to_remove:
        if remove_extra:
            console.print(f"[red]Tracks to remove ({len(to_remove)}):[/red]")
            for track in result['to_remove_tracks'][:10]:  # Show first 10
                artists = ", ".join([a['name'] for a in track['artists']])
                console.print(f"  - {artists} - {track['name']}")
            if len(to_remove) > 10:
                console.print(f"  ... and {len(to_remove) - 10} more")
            console.print()
        else:
            console.print(f"[yellow]Target has {len(to_remove)} extra tracks (use --remove-extra to remove them)[/yellow]")
            console.print()
    
    if not to_add and not to_remove:
        console.print("[bold green]✓ Playlists are already in sync![/bold green]")
        if preserve_order:
            console.print("[dim]Order synchronization not checked in dry-run mode.[/dim]")
        return
    
    if dry_run:
        console.print("[bold yellow]Dry run complete. Use without --dry-run to apply changes.[/bold yellow]")
        return
    
    # Confirm and apply changes
    action_desc = "add missing tracks"
    if remove_extra and to_remove:
        action_desc = f"add {len(to_add)} tracks and remove {len(to_remove)} tracks"
    elif to_add:
        action_desc = f"add {len(to_add)} tracks"
    
    if spotifaj_functions.confirm(f"\nProceed to {action_desc}?", default=True):
        # Actually perform the sync
        result = spotifaj_functions.sync_playlists(
            username=username,
            source_playlist_id=source_id,
            target_playlist_id=target_id,
            remove_extra=remove_extra,
            preserve_order=preserve_order,
            dry_run=False
        )
        
        console.print(f"\n[bold green]✓ Successfully synced '{target_name}' with '{source_name}'![/bold green]")
        
        if to_add:
            console.print(f"  Added: [green]{len(to_add)}[/green] tracks")
        if remove_extra and to_remove:
            console.print(f"  Removed: [red]{len(to_remove)}[/red] tracks")
    else:
        console.print("[yellow]Sync cancelled.[/yellow]")

@spotifaj.command()
@click.argument('source_playlist')
@click.argument('target_playlist')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--keep-best', type=click.Choice(['popularity', 'explicit', 'clean', 'longest', 'shortest']), 
              help="When removing duplicates, keep the best version based on criteria.")
@click.option('--dry-run', is_flag=True, help="Show what would be done without making changes.")
def merge_playlists(source_playlist, target_playlist, username, keep_best, dry_run):
    """
    Copy missing tracks from source to target playlist and deduplicate.
    
    This is a one-way merge operation that:
    1. Adds tracks from source that aren't in target (by ID and metadata)
    2. Removes any duplicates from the target playlist
    3. Never removes tracks that were already in target
    
    Examples:
      # Simple merge
      spotifaj merge-playlists "Source" "Target"
      
      # Merge and keep most popular versions when deduplicating
      spotifaj merge-playlists "Source" "Target" --keep-best popularity
      
      # Preview changes
      spotifaj merge-playlists "Source" "Target" --dry-run
    
    Keep-best options:
      popularity: Keep the most popular version
      explicit: Prefer explicit versions
      clean: Prefer clean (non-explicit) versions
      longest: Keep the longest version
      shortest: Keep the shortest version
    """
    sp = spotifaj_functions.get_spotify_client(username=username)
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
    
    # Resolve source playlist ID
    source_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', source_playlist)
    if match:
        source_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', source_playlist):
        source_id = source_playlist
    else:
        # Try exact match first, then fuzzy match
        source_id = spotifaj_functions.find_playlist_by_name_fuzzy(username, source_playlist)
    
    if not source_id:
        console.print(f"[red]Could not find source playlist: {source_playlist}[/red]")
        console.print(f"\n[yellow]Note:[/yellow] If this is a Spotify-curated playlist (Discover Weekly, etc.):")
        console.print(f"[yellow]       These are [bold]completely inaccessible[/bold] without 'extended quota mode'.[/yellow]")
        console.print(f"[dim]       See SPOTIFY_PLAYLIST_RESTRICTIONS.md for solutions.[/dim]")
        # Show similar playlists
        all_playlists = spotifaj_functions.fetch_all_user_playlists(username)
        if all_playlists:
            search_lower = source_playlist.lower()
            similar = [p['name'] for p in all_playlists if search_lower in p['name'].lower()][:5]
            if similar:
                console.print(f"\n[yellow]Or did you mean one of these?[/yellow]")
                for name in similar:
                    console.print(f"  - {name}")
        sys.exit(1)
    
    # Resolve target playlist ID
    target_id = None
    match = re.search(r'playlist/([a-zA-Z0-9]+)', target_playlist)
    if match:
        target_id = match.group(1)
    elif re.match(r'^[a-zA-Z0-9]{22}$', target_playlist):
        target_id = target_playlist
    else:
        # Try exact match first, then fuzzy match
        target_id = spotifaj_functions.find_playlist_by_name_fuzzy(username, target_playlist)
    
    if not target_id:
        console.print(f"[red]Could not find target playlist: {target_playlist}[/red]")
        console.print(f"\n[yellow]Note:[/yellow] If this is a Spotify-curated playlist (Discover Weekly, etc.):")
        console.print(f"[yellow]       These are [bold]completely inaccessible[/bold] without 'extended quota mode'.[/yellow]")
        console.print(f"[dim]       See SPOTIFY_PLAYLIST_RESTRICTIONS.md for solutions.[/dim]")
        # Show similar playlists
        all_playlists = spotifaj_functions.fetch_all_user_playlists(username)
        if all_playlists:
            search_lower = target_playlist.lower()
            similar = [p['name'] for p in all_playlists if search_lower in p['name'].lower()][:5]
            if similar:
                console.print(f"\n[yellow]Or did you mean one of these?[/yellow]")
                for name in similar:
                    console.print(f"  - {name}")
        sys.exit(1)
    
    # Get playlist names for display
    try:
        source_info = sp.playlist(source_id)
        target_info = sp.playlist(target_id)
        source_name = source_info['name']
        target_name = target_info['name']
    except Exception as e:
        logger.error(f"Error fetching playlist info: {e}")
        if "404" in str(e) or "not found" in str(e).lower():
            console.print("\n[red]Error:[/red] Spotify returned 404 (Not Found) for one of the playlists.")
            console.print("\n[yellow]This usually means:[/yellow]")
            console.print("  • The playlist is Spotify-curated (Discover Weekly, Release Radar, etc.)")
            console.print("  • Your app lacks 'extended quota mode' to access it")
            console.print("  • See [cyan]SPOTIFY_PLAYLIST_RESTRICTIONS.md[/cyan] for solutions")
        sys.exit(1)
    
    console.print(f"\n[bold cyan]Merging playlists:[/bold cyan]")
    console.print(f"  Source: [green]{source_name}[/green]")
    console.print(f"  Target: [yellow]{target_name}[/yellow]")
    if keep_best:
        console.print(f"  Dedup Strategy: [magenta]{keep_best}[/magenta]")
    console.print()
    
    # Perform merge with deduplication
    result = spotifaj_functions.copy_missing_tracks_with_dedup(
        username=username,
        source_playlist_id=source_id,
        target_playlist_id=target_id,
        keep_best=keep_best,
        dry_run=dry_run
    )
    
    if 'error' in result:
        logger.error(result['error'])
        sys.exit(1)
    
    # Display summary
    console.print(f"[bold]Summary:[/bold]")
    console.print(f"  Source tracks: [cyan]{result['source_count']}[/cyan]")
    console.print(f"  Target tracks (before): [cyan]{result['target_count_before']}[/cyan]")
    console.print()
    
    to_add = result['to_add']
    
    if to_add:
        console.print(f"[green]Tracks to add from source ({len(to_add)}):[/green]")
        for track in result['to_add_tracks'][:10]:  # Show first 10
            artists = ", ".join([a['name'] for a in track['artists']])
            console.print(f"  + {artists} - {track['name']}")
        if len(to_add) > 10:
            console.print(f"  ... and {len(to_add) - 10} more")
        console.print()
    else:
        console.print("[dim]No new tracks to add (all source tracks already in target)[/dim]")
        console.print()
    
    if dry_run:
        console.print("[yellow]Note: Deduplication scan will run after adding tracks (not shown in dry-run)[/yellow]")
        console.print()
        console.print(f"[bold]After merge:[/bold]")
        console.print(f"  Target tracks (estimated): [cyan]{result['target_count_after']}[/cyan]")
        console.print()
        console.print("[bold yellow]Dry run complete. Use without --dry-run to apply changes.[/bold yellow]")
        return
    
    if not to_add:
        console.print("[bold green]✓ Target already contains all tracks from source![/bold green]")
        console.print("[yellow]Checking for duplicates...[/yellow]")
    
    # Confirm and apply changes
    if to_add:
        action_desc = f"add {len(to_add)} tracks and deduplicate"
        if not spotifaj_functions.confirm(f"\nProceed to {action_desc}?", default=True):
            console.print("[yellow]Merge cancelled.[/yellow]")
            return
    
    # Actually perform the merge
    result = spotifaj_functions.copy_missing_tracks_with_dedup(
        username=username,
        source_playlist_id=source_id,
        target_playlist_id=target_id,
        keep_best=keep_best,
        dry_run=False
    )
    
    console.print(f"\n[bold green]✓ Successfully merged '{source_name}' into '{target_name}'![/bold green]")
    
    if to_add:
        console.print(f"  Added: [green]{len(to_add)}[/green] tracks")
    
    if result['duplicates_removed'] > 0:
        console.print(f"  Removed: [red]{result['duplicates_removed']}[/red] duplicates")
    else:
        console.print("  [dim]No duplicates found[/dim]")
    
    console.print(f"\n[bold]Final count:[/bold]")
    console.print(f"  Target tracks: [cyan]{result['target_count_after']}[/cyan]")

@spotifaj.command(hidden=True)
@click.option('--version', help="Version number for the release (e.g. 1.0.0). Defaults to next patch version.")
@click.option('--dry-run', is_flag=True, help="Print to stdout instead of writing to file.")
def generate_changelog(version, dry_run):
    """
    Generate changelog from git commits.
    """
    try:
        from utils.changelog_generator import generate_changelog as gen_log
    except ImportError:
        logger.error("Changelog generator module not found.")
        return

    # Determine version if not provided
    if not version:
        # Simple increment logic or default
        version = "0.0.2" # Placeholder, ideally read current and increment
        # Try to read current version from __version__
        try:
            current = globals().get('__version__', '0.0.1')
            parts = [int(x) for x in current.split('.')]
            parts[-1] += 1
            version = ".".join(map(str, parts))
        except:
            pass

    logger.info(f"Generating changelog for version {version}...")
    
    try:
        content = gen_log(version)
        
        if dry_run:
            console.print(content)
        else:
            changelog_path = "CHANGELOG.md"
            
            # Read existing content
            existing = ""
            if os.path.exists(changelog_path):
                with open(changelog_path, 'r') as f:
                    existing = f.read()
                    
                # Remove title if present to avoid duplication when prepending
                if existing.startswith("# Changelog"):
                    existing = existing.replace("# Changelog\n\nAll notable changes to this project will be documented in this file.\n\n", "")
            
            # Write new content
            with open(changelog_path, 'w') as f:
                f.write("# Changelog\n\nAll notable changes to this project will be documented in this file.\n\n")
                f.write(content)
                f.write("\n\n")
                f.write(existing)
                
            console.print(f"[bold green]Updated {changelog_path}[/bold green]")
            
    except Exception as e:
        logger.error(f"Failed to generate changelog: {e}")

if __name__ == '__main__':
    spotifaj()
