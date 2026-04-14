import asyncio
import logging
import os
import signal
import time
from collections import defaultdict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Rate limiting: 10 requests per minute per (bot, user)
RATE_LIMIT = 10
RATE_WINDOW = 60  # seconds
rate_limits: dict[tuple[int, int], list[float]] = defaultdict(list)


def check_rate_limit(bot_id: int, user_id: int) -> bool:
    key = (bot_id, user_id)
    now = time.time()
    timestamps = rate_limits[key]
    rate_limits[key] = [t for t in timestamps if now - t < RATE_WINDOW]
    if len(rate_limits[key]) >= RATE_LIMIT:
        return False
    rate_limits[key].append(now)
    return True


MESSAGE_TEXT = (
    "Переходи скорее!\n"
    "\n"
    "🚀 VPN с обходом РКН замедлений прямо в Telegram!\n"
    "• Обход любых блокировок\n"
    "• Работа при ограничении мобильного интернета белые списки\n"
    "• Выгодная Цена\n"
    "\n"
    "Работает в любой точке мира! 👇"
)


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    bot_id = context.bot.id
    user_id = update.effective_user.id

    if not check_rate_limit(bot_id, user_id):
        return

    referral_link = context.bot_data.get(
        "referral_link", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
    )

    keyboard = [[InlineKeyboardButton("🚀 Подключить VPN", url=referral_link)]]
    await update.message.reply_text(
        MESSAGE_TEXT, reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def ignore_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass


def build_app(token: str, referral_link: str) -> Application:
    app = Application.builder().token(token).build()
    app.bot_data["referral_link"] = referral_link
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, ignore_handler))
    return app


async def run() -> None:
    default_link = os.getenv(
        "DEFAULT_REFERRAL_LINK", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
    )

    apps: list[Application] = []
    for i in range(1, 9):
        token = os.getenv(f"BOT_TOKEN_{i}")
        if not token:
            continue
        referral_link = os.getenv(f"REFERRAL_LINK_{i}", default_link)
        app = build_app(token, referral_link)
        apps.append(app)
        logger.info("Bot %d configured (token ...%s)", i, token[-6:])

    if not apps:
        logger.error("No bot tokens found. Set BOT_TOKEN_1 .. BOT_TOKEN_8 env vars.")
        return

    # Initialize and start all bots
    for app in apps:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            allowed_updates=[Update.MESSAGE],
            drop_pending_updates=True,
        )

    logger.info("Started %d bot(s). Waiting for updates...", len(apps))

    # Wait for shutdown signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await stop_event.wait()

    # Graceful shutdown
    logger.info("Shutting down...")
    for app in apps:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(run())
