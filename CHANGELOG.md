# Changelog

## [0.0.3] - 2026-01-04

### Added
- **Multiple export formats**: Export playlists in TXT, CSV, JSON, or M3U formats with full metadata
- **Playlist analytics**: New `analytics` command showing duration stats, decade distribution, artist frequency, popularity metrics, and diversity scores
- **Auto-update smart playlists**: New `auto-update` command that tracks last update time and automatically adds new label releases
- **Advanced deduplication**: Enhanced `deduplicate` command with `--keep-best` option (popularity, explicit/clean, longest/shortest)
- **Recommendation engine**: New `recommend` command using search-based discovery to find similar tracks from playlist artists/genres
- **Playlist cover upload**: Interactive cover image upload with URL input, automatic compression, and format conversion
- **Improved track matching**: Strip metadata patterns from imports (taken from, released on, etc.) and apostrophe normalization

### Changed
- `export-playlist` now supports `--format` option (txt, csv, json, m3u) and `--output` for file writing
- `deduplicate` applies smart keep-best logic when removing duplicates based on quality criteria
- `import-playlist` preserves original track order (manual reviews no longer appended to end)
- `recommend` uses search-based method (audio_features/recommendations APIs require Extended Quota unavailable to individuals)
- Image upload properly accounts for base64 encoding overhead (180KB raw = 240KB encoded)
- Minimum confidence threshold (40%) prevents garbage match suggestions
- OAuth scopes optimized to prevent double authentication (passes `sp` client to helper functions)

### Fixed
- Track order preservation in import-playlist (manual corrections maintain position)
- Playlist cover image compression with aggressive quality/dimension reduction
- Search normalization removes apostrophes for better matching ("Yesterday's" ↔ "Yesterdays")
- Metadata pattern removal in track names for cleaner matching
- Auth scope issue causing re-authentication during playlist creation

## [0.0.2] - 2026-01-03

### Added
- YAML configuration system (config.yaml) for runtime customization of all constants
- SQLite cache backend for 10-100x performance improvement over JSON
- Performance profiling system with @profile decorator and hot path tracking
- Comprehensive type hints (Python 3.10+) across core modules
- Configurable logging levels via YAML configuration
- Improved text normalization for playlist import/export (handles em dashes, pipes, extra whitespace)

### Changed
- Constants now load from config.yaml with automatic fallback to defaults
- Cache system supports both SQLite and JSON backends (configurable)
- Import playlist matching improved with aggressive normalization (handles "R O G I" vs "ROGI")
- Search queries normalized to remove special characters for better matching

### Fixed
- Dead code and unused imports removed
- Requirements.txt now includes pyyaml dependency

## [0.0.1] - 2025-12-22

### Added
- Initial release of Spotifaj CLI.
- Unified entry point `spotifaj.py`.
- Discogs integration for label search.
- Spotify label search with year filtering.
- Playlist export and import functionality.
- Shell completion support.
- Self-bootstrapping wrapper script.
