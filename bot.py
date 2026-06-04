import asyncio
import hashlib
import logging
import os
import signal
import time
from collections import defaultdict

from aiohttp import web
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
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


# Description shown when user opens the bot chat for the first time (up to 512 chars)
BOT_DESCRIPTION = (
    "🔐 Быстрый и надёжный VPN прямо в Telegram!\n"
    "\n"
    "✅ Обход любых блокировок и замедлений РКН\n"
    "✅ Работает при ограничении мобильного интернета\n"
    "✅ Белые списки — без потери скорости\n"
    "✅ Выгодная цена\n"
    "✅ Работает в любой точке мира\n"
    "\n"
    "Нажми /start чтобы подключиться 🚀"
)

# Short description shown in search results and bot profile (up to 120 chars)
BOT_SHORT_DESCRIPTION = (
    "🔐 VPN в Telegram — обход блокировок РКН, белые списки, работает по всему миру. Нажми Start!"
)

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


def build_app(token: str, referral_link: str, bot_desc: str, bot_short_desc: str) -> Application:
    app = Application.builder().token(token).updater(None).build()
    app.bot_data["referral_link"] = referral_link
    app.bot_data["description"] = bot_desc
    app.bot_data["short_description"] = bot_short_desc
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, ignore_handler))
    return app


def webhook_path(token: str) -> str:
    return "/webhook/" + hashlib.sha256(token.encode()).hexdigest()[:16]


async def run() -> None:
    port = int(os.getenv("PORT", "8080"))
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("WEBHOOK_DOMAIN")

    if not domain:
        logger.error(
            "Set RAILWAY_PUBLIC_DOMAIN or WEBHOOK_DOMAIN env var "
            "(e.g. myapp.up.railway.app)"
        )
        return

    default_link = os.getenv(
        "DEFAULT_REFERRAL_LINK", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
    )

    # path -> Application
    apps: dict[str, Application] = {}

    for i in range(1, 47):
        token = os.getenv(f"BOT_TOKEN_{i}")
        if not token:
            continue
        referral_link = os.getenv(f"REFERRAL_LINK_{i}", default_link)
        bot_desc = os.getenv(f"BOT_DESCRIPTION_{i}", BOT_DESCRIPTION)
        bot_short_desc = os.getenv(f"BOT_SHORT_DESCRIPTION_{i}", BOT_SHORT_DESCRIPTION)

        path = webhook_path(token)
        app = build_app(token, referral_link, bot_desc, bot_short_desc)
        apps[path] = app
        logger.info("Bot %d configured -> %s (token ...%s)", i, path, token[-6:])

    if not apps:
        logger.error("No bot tokens found. Set BOT_TOKEN_1 .. BOT_TOKEN_8 env vars.")
        return

    # Initialize bots and register webhooks
    failed_paths: list[str] = []
    for path, app in apps.items():
        try:
            await app.initialize()

            await app.bot.set_my_description(app.bot_data["description"])
            await app.bot.set_my_short_description(app.bot_data["short_description"])
            await app.bot.set_my_commands([
                BotCommand("start", "🚀 Подключить VPN"),
            ])

            webhook_url = f"https://{domain}{path}"
            await app.bot.set_webhook(
                url=webhook_url,
                allowed_updates=[Update.MESSAGE],
                drop_pending_updates=True,
            )
            logger.info("Webhook set: %s", webhook_url)

            await app.start()
        except Exception as exc:
            logger.error("Bot %s failed to start, skipping: %s", path, exc)
            failed_paths.append(path)

    for path in failed_paths:
        del apps[path]

    # --- aiohttp web server ---
    async def handle_webhook(request: web.Request) -> web.Response:
        path = request.path
        app = apps.get(path)
        if not app:
            return web.Response(status=404)
        data = await request.json()
        update = Update.de_json(data, app.bot)
        await app.process_update(update)
        return web.Response(status=200)

    async def handle_health(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    http_app = web.Application()
    http_app.router.add_get("/", handle_health)
    for path in apps:
        http_app.router.add_post(path, handle_webhook)

    runner = web.AppRunner(http_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info("Started %d bot(s) on port %d. Listening for webhooks...", len(apps), port)

    # Wait for shutdown signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await stop_event.wait()

    # Graceful shutdown
    logger.info("Shutting down...")
    for app in apps.values():
        await app.stop()
        await app.shutdown()
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run())
