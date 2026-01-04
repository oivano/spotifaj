# Spotifaj CLI

<p align="center">
  <img src="assets/avatar.png" alt="Spotifaj Logo" width="200"/>
</p>

A powerful, feature-rich command-line interface for managing Spotify playlists, discovering music via Discogs, and performing advanced library operations.

## Features

*   **Discogs Integration**: Create Spotify playlists from Discogs labels with high accuracy.
*   **Advanced Search**: Search for tracks by label, year, or exhaustive year-by-year scanning.
*   **Playlist Portability**: Export playlists in multiple formats (TXT, CSV, JSON, M3U) and import them back.
*   **Smart Deduplication**: Find and remove duplicate tracks with intelligent keep-best logic (popularity, explicit/clean, duration).
*   **Playlist Analytics**: Analyze playlists with duration stats, decade distribution, artist frequency, and diversity metrics.
*   **Auto-Update**: Smart playlists that automatically add new label releases since last run.
*   **Recommendations**: Generate similar tracks based on playlist artists and genres using search-based discovery.
*   **Cover Image Upload**: Add custom playlist covers from URLs with automatic compression.
*   **Smart Caching**: SQLite-backed caching minimizes API calls and speeds up operations 10-100x.
*   **Rich UI**: Beautiful terminal output with progress bars, colors, and tables.

## Installation

### Prerequisites
*   Python 3.8+
*   Spotify Developer Account (Client ID & Secret)
*   Discogs Developer Account (User Token) - *Optional, for Discogs features*

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://gitlab.com/oivan/spotifaj.git
    cd spotifaj
    ```

2.  **Configure Environment:**
    Copy the example environment file and fill in your credentials.
    ```bash
    cp .env.example .env
    vi .env
    ```
    
    **Required `.env` variables:**
    ```ini
    SPOTIPY_CLIENT_ID="your_spotify_client_id"
    SPOTIPY_CLIENT_SECRET="your_spotify_client_secret"
    SPOTIPY_REDIRECT_URI="http://127.0.0.1:8888/callback"
    SPOTIPY_USERNAME="your_spotify_username"
    DISCOGS_USER_TOKEN="your_discogs_token" # Optional
    ```

3.  **Run the Wrapper:**
    The included wrapper script handles virtual environment creation and dependency installation automatically.
    ```bash
    ./spotifaj --version
    ```

4.  **Install Shell Completion (Optional):**
    Enable tab completion for Zsh, Bash, or Fish.
    ```bash
    ./spotifaj install-completion
    ```

## Usage

### 1. Search & Create from Label (Spotify Source)
Search for tracks released by a specific label on Spotify.

```bash
# Search for "Warp Records" releases in 2023
./spotifaj spotify-label "Warp Records" --year 2023

# Exhaustive search (year-by-year) for all time
./spotifaj spotify-label "Warp Records" --year all --playlist "Warp History"
```

### 2. Search & Create from Label (Discogs Source)
Use Discogs as the source of truth for a label's discography, then find those tracks on Spotify. This is often more accurate for niche labels.

```bash
./spotifaj discogs-label "Basic Channel"
```

### 3. Playlist Portability
Export a playlist in various formats or import from text.

```bash
# Export as text (default)
./spotifaj export-playlist "Discover Weekly" > discover_backup.txt

# Export as CSV with full metadata
./spotifaj export-playlist "My Playlist" --format csv -o playlist.csv

# Export as JSON
./spotifaj export-playlist "My Playlist" --format json -o playlist.json

# Export as M3U playlist file
./spotifaj export-playlist "My Playlist" --format m3u -o playlist.m3u

# Import a playlist
./spotifaj import-playlist discover_backup.txt --name "Discover Weekly Backup"

# Clone a playlist in one line
./spotifaj export-playlist "Source Playlist" | ./spotifaj import-playlist --name "Cloned Playlist"
```

### 4. Playlist Analytics
Analyze your playlists with detailed statistics.

```bash
# Analyze a playlist
./spotifaj analytics "My Favorites"

# Shows: duration stats, decade distribution, top artists, popularity, 
# explicit content %, and diversity metrics
```

### 5. Auto-Update Smart Playlists
Keep label playlists current with new releases.

```bash
# First run: adds releases from last 30 days
./spotifaj auto-update "Warp Records"

# Subsequent runs: only adds new tracks since last update
./spotifaj auto-update "Warp Records"

# Dry run to preview what would be added
./spotifaj auto-update "Ninja Tune" --dry-run

# Batch update multiple playlists by exact name
./spotifaj auto-update --batch "Warp Records" "Ninja Tune" "Kompakt"

# Batch update from file (one playlist name per line)
./spotifaj auto-update --batch --file playlists.txt

# Dry run for batch update
./spotifaj auto-update --batch --dry-run "Label 1" "Label 2"
```

**Batch Mode:**
- Provide exact playlist names as arguments with `--batch`
- Use `--file` option to read playlist names from a file (one per line)
- Playlists must be auto-updated at least once to be tracked
- Shows summary of updated/skipped/failed playlists

### 6. Deduplication
Clean up your playlists by removing duplicate tracks.

```bash
# Check a specific playlist
./spotifaj deduplicate "My Messy Playlist"

# Check ALL your playlists (dry run)
./spotifaj deduplicate --all --dry-run

# Smart deduplication: keep most popular version
./spotifaj deduplicate "My Playlist" --keep-best popularity

# Keep explicit or clean versions
./spotifaj deduplicate "My Playlist" --keep-best explicit
./spotifaj deduplicate "My Playlist" --keep-best clean

# Keep longest or shortest versions
./spotifaj deduplicate "My Playlist" --keep-best longest
```

### 7. Recommendations
Generate similar tracks based on a playlist's artists and genres.

```bash
# Generates recommendations and prompts to create playlist
./spotifaj recommend "My Playlist"

# Generate more recommendations
./spotifaj recommend "Chill Vibes" --limit 100
```

**Behavior:**
- Finds similar tracks from playlist artists and genres
- Strict diversity: max 1 track per album, mostly 1 per artist (20% chance for 2nd)
- Prompts to create playlist "[Source Name] — Recommendations" if ≥10 tracks found

**Note:** Uses search-based discovery. Audio features analysis requires Extended Quota Mode (organizations only, 250k+ MAU).

### 8. Playlist Cover Images
Add custom cover art to your playlists.

```bash
# The import-playlist command will prompt for cover images
./spotifaj import-playlist tracks.txt --name "My Mix"
# (Interactive prompt: "Would you like to add a cover image?")
# Enter URL: https://example.com/image.jpg
# Auto-compresses to fit Spotify's 256KB limit
```

### 9. General Search
Quickly search for tracks, artists, or albums.

```bash
./spotifaj search "Aphex Twin" --type artist
```

## Development

### Release Management
Generate a changelog based on git commits.

```bash
./spotifaj generate-changelog --version 1.1.0
```

## License

This project is licensed under the **GNU General Public License v3.0 (GPLv3)**.

You are free to copy, distribute, and modify the software as long as you track changes/dates in source files and keep modifications under GPLv3. You can distribute your application using a GPL library commercially, but you must also provide the source code.

See [LICENSE](LICENSE) for the full text.
