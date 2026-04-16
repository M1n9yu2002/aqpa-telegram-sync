from __future__ import annotations

from telegram.ext import Application, ApplicationBuilder


def build_application(bot_token: str) -> Application:
    return ApplicationBuilder().token(bot_token).build()
