"""
Track Deduplicator
-----------------
Handles deduplication of tracks using intelligent comparison.
"""
import logging
import re

logger = logging.getLogger('track_deduplicator')

def generate_track_signature(track):
    """
    Create a robust signature for track deduplication.
    
    Handles various edge cases like featured artists, remixes, etc.
    """
    if not track:
        return None
        
    name = track['name'].lower()
    
    # Clean up the track name
    # Remove remix/version indicators for better matching
    base_name = name.split(' - ')[0].strip()
    
    # Get all artists and sort them for consistency
    artists = []
    for artist in track['artists']:
        artist_name = artist['name'].lower()
        # Handle "feat." variations 
        if 'feat.' in artist_name or 'ft.' in artist_name or 'featuring' in artist_name:
            parts = re.split(r'feat\.|ft\.|featuring', artist_name, maxsplit=1)
            artists.append(parts[0].strip())
            if len(parts) > 1 and parts[1].strip():
                artists.append(parts[1].strip())
        else:
            artists.append(artist_name)
            
    # Sort alphabetically for consistent signatures
    artist_key = "+".join(sorted(artists))
    
    # Add track duration (rounded to nearest 5 seconds) to distinguish different versions
    duration_bucket = (track['duration_ms'] // 5000) * 5  # Round to nearest 5 seconds
    
    return f"{base_name}|{artist_key}|{duration_bucket}"

def deduplicate_tracks(spotify_client, track_ids, display_progress=False, progress_bar=None):
    """
    Deduplicate tracks without relying on audio features to avoid API errors.
    """
    if not track_ids:
        return []
        
    # Create progress bar if needed
    pbar = progress_bar
    if display_progress and pbar is None:
        # We'll use rich progress if available, or just skip
        pass
    
    # Use a simple set-based deduplication approach first
    unique_ids = list(dict.fromkeys(track_ids))  # This preserves order
    
    # For more advanced deduplication, use track metadata instead of audio features
    try:
        if len(unique_ids) > 50:  # Only do advanced deduplication for larger sets
            # Get tracks in batches
            batch_size = 50
            tracks_info = []
            
            for i in range(0, len(unique_ids), batch_size):
                batch = unique_ids[i:i+batch_size]
                try:
                    batch_info = spotify_client.tracks(batch)
                    if batch_info and 'tracks' in batch_info:
                        tracks_info.extend(batch_info['tracks'])
                except Exception as e:
                    logger.warning(f"Error getting track info: {e}")
            
            # Group tracks by artist+album to find potential duplicates
            grouped_tracks = {}
            for track in tracks_info:
                if not track:
                    continue
                    
                # Create key based on artist and album
                artists = "+".join(sorted([a['name'] for a in track.get('artists', [])]))
                album = track.get('album', {}).get('name', '')
                key = f"{artists}|{album}"
                
                if key not in grouped_tracks:
                    grouped_tracks[key] = []
                grouped_tracks[key].append(track['id'])
            
            # Keep only one track per artist+album if there are multiple
            # This is a simplified approach but avoids 403 errors
            final_ids = []
            for group, ids in grouped_tracks.items():
                if len(ids) > 1:
                    # Keep the first one
                    final_ids.append(ids[0])
                else:
                    final_ids.extend(ids)
            
            return final_ids
    except Exception as e:
        logger.error(f"Error in advanced deduplication: {e}")
    
    # Fall back to simple deduplication if anything fails
    return unique_ids
