# Spotifaj CLI

<p align="center">
  <img src="assets/avatar.png" alt="Spotifaj Logo" width="200"/>
</p>

A powerful, feature-rich command-line interface for managing Spotify playlists, discovering music via Discogs, and performing advanced library operations.

## Features

*   **Discogs Integration**: Create Spotify playlists from Discogs labels with high accuracy.
*   **Advanced Search**: Search for tracks by label, year, or exhaustive year-by-year scanning.
*   **Playlist Portability**: Export playlists to text files and import them back (great for backups or cloning).
*   **Deduplication**: Find and remove duplicate tracks from your playlists based on ID or metadata (fuzzy matching).
*   **Smart Caching**: Minimizes API calls to Spotify and Discogs to avoid rate limits and speed up repeated searches.
*   **Rich UI**: Beautiful terminal output with progress bars, colors, and tables.

## Installation

### Prerequisites
*   Python 3.8+
*   Spotify Developer Account (Client ID & Secret)
*   Discogs Developer Account (User Token) - *Optional, for Discogs features*

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/spotifaj.git
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
Export a playlist to a text file (Artist - Track) or import one.

```bash
# Export a playlist
./spotifaj export-playlist "Discover Weekly" > discover_backup.txt

# Import a playlist
./spotifaj import-playlist discover_backup.txt --name "Discover Weekly Backup"

# Clone a playlist in one line
./spotifaj export-playlist "Source Playlist" | ./spotifaj import-playlist --name "Cloned Playlist"
```

### 4. Deduplication
Clean up your playlists by removing duplicate tracks.

```bash
# Check a specific playlist
./spotifaj deduplicate "My Messy Playlist"

# Check ALL your playlists (dry run)
./spotifaj deduplicate --all --dry-run
```

### 5. General Search
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
