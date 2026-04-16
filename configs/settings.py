from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    env: str = Field(default="dev", alias="ENV")
    db_path: str = Field(default="data/app.db", alias="DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    telegram_allowed_chat_ids: str = Field(alias="TELEGRAM_ALLOWED_CHAT_IDS")

    google_sheet_id: str = Field(alias="GOOGLE_SHEET_ID")
    google_service_account_file: str = Field(alias="GOOGLE_SERVICE_ACCOUNT_FILE")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @property
    def telegram_allowed_chat_id_set(self) -> set[int]:
        raw = self.telegram_allowed_chat_ids.strip()
        if not raw:
            return set()
        return {
            int(part.strip())
            for part in raw.split(",")
            if part.strip()
        }

    @property
    def db_path_obj(self) -> Path:
        return Path(self.db_path)

    @property
    def google_service_account_path(self) -> Path:
        return Path(self.google_service_account_file)


@lru_cache
def get_settings() -> Settings:
    return Settings()
