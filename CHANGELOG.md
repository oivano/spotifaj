# Changelog

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
