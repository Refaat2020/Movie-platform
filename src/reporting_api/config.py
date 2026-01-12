"""
FastAPI application configuration.

Uses pydantic-settings for environment-based configuration.
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application
    APP_NAME: str = "Movie Reporting API"
    DEBUG: bool = False
    
    # MongoDB
    MONGODB_URI: str = "mongodb://admin:admin@localhost:27017/movies_reporting?authSource=admin"
    MONGODB_DATABASE: str = "movies_reporting"
    
    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost:8000",
        "http://localhost:3000",
    ]
    
    # API Configuration
    API_PREFIX: str = "/api"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
