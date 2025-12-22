#!/usr/bin/env python3
"""
Spotify CLI - Unified entry point for Spotify tools.
"""

import click
import sys
import os
import re
import time
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
import spotifaj_functions

# Import new modules
try:
    from workflows.discogs_workflow import DiscogsLabelWorkflow
    from workflows.spotify_workflow import SpotifyLabelWorkflow
    from clients.discogs_client import get_discogs_client
    from utils.cache_manager import CacheManager
    from utils.track_deduplicator import deduplicate_tracks, generate_track_signature
except ImportError as e:
    logger.warning(f"Could not import advanced modules: {e}")
    DiscogsLabelWorkflow = None
    SpotifyLabelWorkflow = None

console = Console()
__version__ = "0.0.1"

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
def search_label(label, username, playlist, exhaustive, year, validate):
    """Search for tracks by label and optionally add to a playlist."""
    
    if year and str(year).lower() == 'all':
        exhaustive = True
        year = None

    playlist_name = playlist if playlist else label
    if year:
        playlist_name = f"{playlist_name} ({year})"
    
    sp = spotifaj_functions.get_spotify_client()
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    if year:
        if '-' in str(year):
            try:
                start_str, end_str = year.split('-')
                start_year = int(start_str)
                end_year = int(end_str)
                
                logger.info(f"Searching for label: '{label}' in range {start_year}-{end_year}...")
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
                logger.info(f"Searching for label: '{label}' in year {year_int}...")
                found_tracks = spotifaj_functions.search_tracks_by_year(sp, label, year_int)
            except ValueError:
                logger.error(f"Invalid year: {year}. Use a 4-digit year, range YYYY-YYYY, or 'all'.")
                sys.exit(1)
    elif exhaustive:
        found_tracks = spotifaj_functions.search_tracks_exhaustive(sp, label)
    else:
        logger.info(f"Searching for label: '{label}' (Standard Search)...")
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
            else:
                logger.info("No tracks left after validation.")
        else:
            logger.info("No new tracks to add.")
    else:
        logger.info("Operation cancelled or failed.")

@spotifaj.command()
@click.argument('label')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--strictness', type=click.Choice(['loose', 'normal', 'strict']), default='normal', help="Verification strictness.")
@click.option('--force-update', is_flag=True, help="Force update cache.")
def discogs_label(label, username, strictness, force_update):
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
            if spotifaj_functions.confirm(f"Found {len(track_ids)} verified tracks. Create playlist?"):
                workflow.create_label_playlist(track_ids, discogs_label.name)
        else:
            logger.info("No tracks found.")
            
    except Exception as e:
        logger.error(f"Error in Discogs workflow: {e}")

@spotifaj.command()
@click.argument('label')
@click.option('--year', help="Year(s) to search. 'all', 'YYYY', or 'YYYY-YYYY'. Default: current year.")
@click.option('--playlist', help="Name of playlist to create.")
@click.option('--no-cache', is_flag=True, help="Disable caching.")
def spotify_label(label, year, playlist, no_cache):
    """
    Advanced Spotify label search with confidence scoring.
    
    Uses multiple search strategies (High/Medium/Low confidence) to find tracks
    associated with a label, verifies them against metadata, and deduplicates results.
    """
    if not SpotifyLabelWorkflow:
        console.print("[red]Error: Advanced modules not available.[/red]")
        return

    sp = spotifaj_functions.get_spotify_client()
    if not sp:
        return

    workflow = SpotifyLabelWorkflow(sp)
    
    # Parse year input for the workflow
    from datetime import datetime
    year_input = '1' if str(year).lower() == 'all' else (year if year else str(datetime.now().year))
    
    console.print(f"[bold green]Starting advanced search for label: {label}[/bold green]")
    if year:
        console.print(f"Year range: {year}")

    try:
        track_ids = workflow.get_label_tracks(
            label, 
            year_input=year_input,
            use_cache=not no_cache
        )
        
        if track_ids:
            console.print(f"[green]Found {len(track_ids)} verified tracks.[/green]")
            if click.confirm(f"Create playlist with {len(track_ids)} tracks?", default=True):
                pl_id = workflow.create_label_playlist(label, track_ids, playlist_name=playlist)
                if pl_id:
                    console.print(f"[bold green]Playlist created successfully![/bold green]")
        else:
            console.print("[yellow]No tracks found matching criteria.[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error running workflow: {e}[/red]")
        logger.exception("Workflow failed")

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
    sp = spotifaj_functions.get_spotify_client(username=username, scope="playlist-read-private")
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)
        
    try:
        playlists = sp.user_playlists(username)
        while playlists:
            for playlist in playlists['items']:
                print(f"{playlist['name']} (ID: {playlist['id']}, Tracks: {playlist['tracks']['total']})")
            
            if playlists['next']:
                playlists = sp.next(playlists)
            else:
                playlists = None
    except Exception as e:
        logger.error(f"Error fetching playlists: {e}")

@spotifaj.command()
@click.argument('query')
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
@click.option('--playlist', help="Name of the playlist to create.")
@click.option('--artist', is_flag=True, help="Filter results by artist.")
@click.option('--album', is_flag=True, help="Filter results by album.")
@click.option('--track', is_flag=True, help="Filter results by track.")
@click.option('--limit', default=50, help="Number of tracks to fetch (default 50).")
@click.option('--all', 'fetch_all', is_flag=True, help="Fetch all results (up to 1000).")
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
                if score > 0.8: 
                    best_score = score
                    best_type = 'artist'
                    best_match_name = artist_obj['name']
            
            # Check Album (only override artist if significantly better)
            if intent_results.get('albums', {}).get('items'):
                album_obj = intent_results['albums']['items'][0]
                score = SequenceMatcher(None, query.lower(), album_obj['name'].lower()).ratio()
                if score > best_score and score > 0.9:
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

            if best_type == 'artist' and best_score > 0.8:
                logger.info(f"Detected Artist intent: '{best_match_name}' (Confidence: {best_score:.2f})")
                # Use original query to avoid incorrect auto-correction (e.g. Igor Jadranin -> Igor Garanin)
                search_query = f"artist:{query}"
            elif best_type == 'album' and best_score > 0.9:
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

    # Display tracks (limit to 20 by default unless user asked for more via limit option explicitly?)
    # User requirement: "per default if no option provided - display only first 20"
    display_limit = 20
    
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
def deduplicate(playlist_input, username, check_all, dry_run):
    """
    Check for duplicate tracks in a playlist.
    
    PLAYLIST_INPUT can be a Spotify Playlist URL or a Playlist Name.
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

        for pl in playlists_to_check:
            # Be polite to the API
            time.sleep(0.2)
            
            name = pl['name']
            pid = pl['id']
            owner_id = pl['owner']['id']
            is_collaborative = pl['collaborative']
            
            # Check if we can modify this playlist
            can_modify = (owner_id == username) or is_collaborative
            
            progress.update(task, description=f"[cyan]'{name}'...[/cyan]")
            
            duplicates = spotifaj_functions.find_duplicates_in_playlist(username, pid)
            
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
def export_playlist(playlist_input, username):
    """
    Export a playlist to a text list (Artist - Track).
    
    PLAYLIST_INPUT can be a Spotify Playlist URL, ID, or Name.
    Output is printed to stdout, so it can be redirected to a file.
    """
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
        results = sp.playlist_items(playlist_id)
        tracks = results['items']
        
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
            
        # Print to stdout for redirection
        for item in tracks:
            if not item or 'track' not in item or not item['track']:
                continue
                
            track = item['track']
            name = track['name']
            artists = ", ".join([artist['name'] for artist in track['artists']])
            
            # Print clean "Artist - Title" format
            print(f"{artists} - {name}")
            
    except Exception as e:
        logger.error(f"Error exporting playlist: {e}")

@spotifaj.command()
@click.argument('input_file', type=click.File('r'), required=False)
@click.option('--name', '-n', required=True, help="Name of the new playlist.")
@click.option('--username', default=settings.spotipy_username, help="Spotify username.")
def import_playlist(input_file, name, username):
    """
    Create a playlist from a text list of tracks.
    
    Reads from INPUT_FILE or stdin if not provided.
    Expected format: "Artist - Track Name" per line.
    """
    # Handle input source
    if input_file:
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

    sp = spotifaj_functions.get_spotify_client(username=username, scope="playlist-modify-public playlist-modify-private")
    if not sp:
        logger.error("Failed to initialize Spotify client.")
        sys.exit(1)

    track_uris = []
    not_found = []
    
    logger.info(f"Processing {len(lines)} lines...")
    
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Searching tracks...", total=len(lines))
        
        for line in lines:
            line = line.strip()
            if not line:
                progress.advance(task)
                continue
            
            # Skip log lines if piping from export command
            if (line.startswith("[") and "INFO" in line) or line.startswith("Exporting playlist"):
                progress.advance(task)
                continue

            try:
                # Search for the track
                results = sp.search(q=line, limit=1, type='track')
                items = results['tracks']['items']
                
                if items:
                    track_uris.append(items[0]['uri'])
                else:
                    not_found.append(line)
            except Exception as e:
                logger.error(f"Error searching for '{line}': {e}")
                not_found.append(line)
            
            progress.advance(task)

    # Report results
    console.print(f"\n[bold]Found {len(track_uris)} tracks.[/bold]")
    if not_found:
        console.print(f"[red]Could not find {len(not_found)} tracks:[/red]")
        for nf in not_found[:10]:
            console.print(f"  - {nf}")
        if len(not_found) > 10:
            console.print(f"  ... and {len(not_found) - 10} more.")

    if not track_uris:
        logger.warning("No tracks found to add.")
        return

    # Create playlist
    if spotifaj_functions.confirm(f"Create playlist '{name}' with {len(track_uris)} tracks?", default=True):
        playlist_id = spotifaj_functions.create_playlist(username, name)
        if playlist_id:
            spotifaj_functions.add_song_to_spotify_playlist(username, track_uris, playlist_id)
            console.print(f"[bold green]Successfully created playlist '{name}'![/bold green]")
        else:
            logger.error("Failed to create playlist.")

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
