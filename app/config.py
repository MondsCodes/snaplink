from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    base_url: str = "http://localhost:8000"
    app_name: str = "SnapLink"
    database_url: str = "postgresql+asyncpg://snaplink:snaplink@localhost:5432/snaplink"


settings = Settings()
