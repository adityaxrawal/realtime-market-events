# backend/core/config.py
# Configuration management using Pydantic BaseSettings

import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from functools import lru_cache

# Load environment variables from .env file if it exists
# This is especially useful for local development.
load_dotenv()

class Settings(BaseSettings):
    """
    Defines application settings, loaded from environment variables.
    """
    # Application settings
    APP_NAME: str = "Financial Analytics Dashboard API"
    API_V1_STR: str = "/api/v1" # Define API prefix version

    # Dhan API Credentials (Loaded from .env file)
    # IMPORTANT: Add your actual DhanHQ Access Token to your .env file
    # Example .env entry: DHAN_ACCESS_TOKEN=your_actual_token_here
    DHAN_CLIENT_ID: str = os.getenv("DHAN_CLIENT_ID", "DEFAULT_CLIENT_ID_IF_NEEDED") # Replace if needed
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN_HERE") # Default if not set

    # Database settings (Example - adjust if needed later)
    # Example .env entry: DATABASE_URL=postgresql+psycopg2://user:password@host:port/db
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:password@timescaledb:5432/postgres")

    # CORS Origins (Example - adjust for your frontend URL)
    # Can be a comma-separated string in .env or defined directly
    BACKEND_CORS_ORIGINS: list[str] = [
        "http://localhost",
        "http://localhost:3000", # Default create-react-app
        "http://localhost:5173", # Default Vite
        # Add your deployed frontend URL here
    ]

    # WebSocket settings
    WEBSOCKET_MAX_CONNECTIONS: int = 100 # Example limit

    class Config:
        # Makes BaseSettings case-insensitive for environment variables
        case_sensitive = False
        # Specifies the .env file encoding
        env_file_encoding = 'utf-8'

# Use lru_cache to cache the settings object, so .env is read only once
@lru_cache()
def get_settings() -> Settings:
    """Returns the cached settings instance."""
    return Settings()

# Instantiate settings to be easily imported
settings = get_settings()

# Example usage:
# from core.config import settings
# print(settings.DHAN_ACCESS_TOKEN)
