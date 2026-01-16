"""Application settings"""

import os
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Database settings
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")  # Use 127.0.0.1 to force IPv4
    db_port: int = int(os.getenv("DB_PORT", "5433"))
    db_name: str = os.getenv("DB_NAME", "nba_prop_variance")
    db_user: str = os.getenv("DB_USER", "nba_user")
    db_password: str = os.getenv("DB_PASSWORD", "nba_password")

    # API settings
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def database_url(self) -> str:
        """Construct database URL from settings"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
