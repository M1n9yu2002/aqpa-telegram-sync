from __future__ import annotations

from telegram.ext import CommandHandler

from adapters.telegram_client import build_application
from bots.handlers import positions_command, sync_command, watchlist_command
from configs.settings import get_settings
from storage.repositories import Database


def main() -> None:
    settings = get_settings()

    database = Database(settings.db_path)
    database.initialize()

    application = build_application(settings.telegram_bot_token)
    application.add_handler(CommandHandler("sync", sync_command))
    application.add_handler(CommandHandler("positions", positions_command))
    application.add_handler(CommandHandler("watchlist", watchlist_command))
    application.run_polling()


if __name__ == "__main__":
    main()
