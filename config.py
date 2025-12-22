import logging
import sys
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    spotipy_client_id: str = Field(..., description="Spotify Client ID")
    spotipy_client_secret: str = Field(..., description="Spotify Client Secret")
    spotipy_redirect_uri: str = Field("http://localhost:8888/callback", description="Spotify Redirect URI")
    spotipy_username: str = Field(..., description="Spotify Username")
    
    # App Settings
    log_level: str = Field("INFO", description="Logging level")

settings = Settings()

# Configure Logging
try:
    from rich.logging import RichHandler
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)]
    )
except ImportError:
    # Fallback if rich is not installed
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

logger = logging.getLogger("spotifaj")
