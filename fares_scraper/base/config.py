from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class ScraperSettings(BaseSettings):
    """
    Centralized configuration for the scraper framework using Pydantic Settings.
    This allows loading from environment variables automatically.
    """
    model_config = SettingsConfigDict(env_prefix="SCRAPER_", env_file=".env", extra="ignore")

    timeout: int = Field(default=15, description="Timeout for network requests in seconds")
    pool_size: int = Field(default=10, description="Max concurrent requests allowed")
    max_retries: int = Field(default=5, description="Number of retries for failed requests")
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        description="Default User-Agent header"
    )
    proxies: List[str] = Field(default_factory=list, description="List of proxy URLs")
    
    # Example: SCRAPER_PROXIES="http://p1,http://p2" will be parsed automatically

# Global settings instance
settings = ScraperSettings()
