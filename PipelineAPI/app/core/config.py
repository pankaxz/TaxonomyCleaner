from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    PROJECT_NAME: str = "Pipeline API"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Comma-separated in env is also supported by pydantic-settings for list types.
    CORS_ORIGINS: list[str] = []


@lru_cache
def get_settings() -> Settings:
    return Settings()
