import asyncio
import hashlib
import logging
import os
import signal
import time
from collections import defaultdict

import asyncpg
from aiohttp import web
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Global registry: webhook_path -> Application
all_apps: dict[str, Application] = {}
# bot_id -> sequential number (1, 2, 3...)
bot_numbers: dict[int, int] = {}
# bot_id -> @username from Telegram
bot_usernames: dict[int, str] = {}
# PostgreSQL connection pool
db_pool: asyncpg.Pool | None = None

RATE_LIMIT = 10
RATE_WINDOW = 60
rate_limits: dict[tuple[int, int], list[float]] = defaultdict(list)

MENU, BROADCAST_TEXT, BROADCAST_CONFIRM = range(3)

# ── Database ──────────────────────────────────────────────────────────

async def init_db() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                bot_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                first_seen DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (bot_id, user_id)
            )
        """)
    logger.info("Database connected and initialized")


async def close_db() -> None:
    if db_pool:
        await db_pool.close()


async def save_user(bot_id: int, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (bot_id, user_id, first_seen) "
            "VALUES ($1, $2, $3) "
            "ON CONFLICT (bot_id, user_id) DO NOTHING",
            bot_id, user_id, time.time(),
        )


async def get_users_for_bot(bot_id: int) -> list[int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM users WHERE bot_id = $1", bot_id
        )
        return [r["user_id"] for r in rows]


async def get_stats() -> dict[int, int]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bot_id, COUNT(*) AS cnt FROM users GROUP BY bot_id"
        )
        return {r["bot_id"]: r["cnt"] for r in rows}


async def get_total_unique_users() -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(DISTINCT user_id) AS cnt FROM users"
        )
        return row["cnt"]


# ── Rate limiting ─────────────────────────────────────────────────────

def check_rate_limit(bot_id: int, user_id: int) -> bool:
    key = (bot_id, user_id)
    now = time.time()
    rate_limits[key] = [t for t in rate_limits[key] if now - t < RATE_WINDOW]
    if len(rate_limits[key]) >= RATE_LIMIT:
        return False
    rate_limits[key].append(now)
    return True


# ── Bot profile texts ─────────────────────────────────────────────────

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

AUTO_BROADCAST_TEXT = (
    "🚀 Переезжаем в новый бот\n"
    "\n"
    "В новом боте:\n"
    "\n"
    "⚡️ Быстрее подключение\n"
    "🌍 Больше локаций\n"
    "🔒 Стабильная работа VPN\n"
    "\n"
    "👇 Нажмите кнопку и перейдите в новый бот прямо сейчас"
)

AUTO_BROADCAST_INTERVAL = int(os.getenv("AUTO_BROADCAST_DAYS", "7")) * 86400


# ── Handlers ──────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    bot_id = context.bot.id
    user_id = update.effective_user.id

    if not check_rate_limit(bot_id, user_id):
        return

    await save_user(bot_id, user_id)

    referral_link = context.bot_data.get(
        "referral_link", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
    )
    keyboard = [[InlineKeyboardButton("🚀 Подключить VPN", url=referral_link)]]
    await update.message.reply_text(
        MESSAGE_TEXT, reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def ignore_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass


# ── Admin handlers ────────────────────────────────────────────────────

def is_admin(update: Update) -> bool:
    return (
        ADMIN_ID != 0
        and update.effective_user is not None
        and update.effective_user.id == ADMIN_ID
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ConversationHandler.END

    total = await get_total_unique_users()
    active_bots = len(all_apps)

    keyboard = [
        [InlineKeyboardButton("📨 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="close")],
    ]
    await update.message.reply_text(
        f"🔐 Админ-панель\n\n"
        f"🤖 Ботов активно: {active_bots}\n"
        f"👥 Пользователей всего: {total}",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return MENU


async def menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "broadcast":
        await query.edit_message_text(
            "📨 Отправьте текст рассылки:\n\n"
            "Каждый бот отправит это сообщение своим пользователям "
            "с кнопкой «Подключить VPN».\n\n"
            "/cancel — отмена"
        )
        return BROADCAST_TEXT

    if query.data == "stats":
        stats = await get_stats()
        total = await get_total_unique_users()
        lines = []
        for bot_id, count in sorted(stats.items(), key=lambda x: bot_numbers.get(x[0], 0)):
            num = bot_numbers.get(bot_id, "?")
            name = bot_usernames.get(bot_id, "—")
            lines.append(f"  #{num} {name}: {count} чел.")
        stats_text = "\n".join(lines) if lines else "  Пока нет данных"
        await query.edit_message_text(
            f"📊 Статистика\n\n"
            f"{stats_text}\n\n"
            f"👥 Уникальных пользователей: {total}\n"
            f"🤖 Активных ботов: {len(all_apps)}"
        )
        return ConversationHandler.END

    await query.edit_message_text("Админ-панель закрыта.")
    return ConversationHandler.END


async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["broadcast_text"] = update.message.text

    keyboard = [
        [InlineKeyboardButton("✅ Отправить всем", callback_data="confirm")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
    ]
    await update.message.reply_text(
        f"📨 Предпросмотр:\n\n"
        f"{update.message.text}\n\n"
        f"[+ кнопка «🚀 Подключить VPN»]\n\n"
        f"Отправить всем пользователям всех ботов?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return BROADCAST_CONFIRM


async def broadcast_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "cancel":
        await query.edit_message_text("❌ Рассылка отменена.")
        return ConversationHandler.END

    text = context.user_data.get("broadcast_text", "")
    if not text:
        await query.edit_message_text("❌ Текст рассылки не найден.")
        return ConversationHandler.END

    await query.edit_message_text("⏳ Рассылка запущена...")

    total_sent = 0
    total_failed = 0
    bots_used = 0

    for _path, app in all_apps.items():
        bot_id = app.bot.id
        referral_link = app.bot_data.get(
            "referral_link", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
        )
        users = await get_users_for_bot(bot_id)
        if not users:
            continue

        bots_used += 1
        keyboard = [[InlineKeyboardButton("🚀 Подключить VPN", url=referral_link)]]
        markup = InlineKeyboardMarkup(keyboard)

        for user_id in users:
            if user_id == ADMIN_ID:
                continue
            try:
                await app.bot.send_message(
                    chat_id=user_id, text=text, reply_markup=markup
                )
                total_sent += 1
            except Exception:
                total_failed += 1
            await asyncio.sleep(0.04)

    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=(
            f"✅ Рассылка завершена!\n\n"
            f"📨 Отправлено: {total_sent}\n"
            f"❌ Не доставлено: {total_failed}\n"
            f"🤖 Ботов задействовано: {bots_used}"
        ),
    )
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ── Auto broadcast ────────────────────────────────────────────────────

async def auto_broadcast_loop() -> None:
    await asyncio.sleep(10)
    while True:
        try:
            await run_auto_broadcast()
        except Exception as exc:
            logger.error("Auto broadcast error: %s", exc)
        await asyncio.sleep(AUTO_BROADCAST_INTERVAL)


async def run_auto_broadcast() -> None:
    if not all_apps:
        return

    total_sent = 0
    total_failed = 0

    for _path, app in all_apps.items():
        bot_id = app.bot.id
        referral_link = app.bot_data.get(
            "referral_link", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
        )
        users = await get_users_for_bot(bot_id)
        if not users:
            continue

        keyboard = [[InlineKeyboardButton("🚀 Подключиться", url=referral_link)]]
        markup = InlineKeyboardMarkup(keyboard)

        for user_id in users:
            try:
                await app.bot.send_message(
                    chat_id=user_id, text=AUTO_BROADCAST_TEXT, reply_markup=markup
                )
                total_sent += 1
            except Exception:
                total_failed += 1
            await asyncio.sleep(0.04)

    logger.info(
        "Auto broadcast done: sent=%d, failed=%d", total_sent, total_failed
    )

    if ADMIN_ID:
        first_app = next(iter(all_apps.values()), None)
        if first_app:
            try:
                await first_app.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=(
                        f"🔄 Авто-рассылка завершена\n\n"
                        f"📨 Отправлено: {total_sent}\n"
                        f"❌ Не доставлено: {total_failed}"
                    ),
                )
            except Exception:
                pass


# ── App builder ───────────────────────────────────────────────────────

def build_app(token: str, referral_link: str, bot_desc: str, bot_short_desc: str) -> Application:
    app = Application.builder().token(token).updater(None).build()
    app.bot_data["referral_link"] = referral_link
    app.bot_data["description"] = bot_desc
    app.bot_data["short_description"] = bot_short_desc

    admin_conv = ConversationHandler(
        entry_points=[CommandHandler("admin", admin_cmd)],
        states={
            MENU: [CallbackQueryHandler(menu_cb)],
            BROADCAST_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_text),
            ],
            BROADCAST_CONFIRM: [CallbackQueryHandler(broadcast_confirm_cb)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
    )

    app.add_handler(admin_conv)
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.ALL, ignore_handler))
    return app


def webhook_path(token: str) -> str:
    return "/webhook/" + hashlib.sha256(token.encode()).hexdigest()[:16]


# ── Main ──────────────────────────────────────────────────────────────

async def run() -> None:
    port = int(os.getenv("PORT", "8080"))
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("WEBHOOK_DOMAIN")

    if not domain:
        logger.error(
            "Set RAILWAY_PUBLIC_DOMAIN or WEBHOOK_DOMAIN env var "
            "(e.g. myapp.up.railway.app)"
        )
        return

    if not DATABASE_URL:
        logger.error("Set DATABASE_URL env var (e.g. postgresql://user:pass@host:5432/db)")
        return

    await init_db()

    default_link = os.getenv(
        "DEFAULT_REFERRAL_LINK", "https://t.me/atlassecure_bot?start=ref_UEGJ3A"
    )

    apps: dict[str, Application] = {}

    for i in range(1, 301):
        token = os.getenv(f"BOT_TOKEN_{i}")
        if not token:
            continue
        referral_link = os.getenv(f"REFERRAL_LINK_{i}", default_link)
        bot_desc = os.getenv(f"BOT_DESCRIPTION_{i}", BOT_DESCRIPTION)
        bot_short_desc = os.getenv(f"BOT_SHORT_DESCRIPTION_{i}", BOT_SHORT_DESCRIPTION)

        path = webhook_path(token)
        app = build_app(token, referral_link, bot_desc, bot_short_desc)
        app.bot_data["bot_number"] = i
        apps[path] = app
        logger.info("Bot %d configured -> %s (token ...%s)", i, path, token[-6:])

    if not apps:
        logger.error("No bot tokens found. Set BOT_TOKEN_1 .. BOT_TOKEN_300 env vars.")
        return

    # Initialize bots and register webhooks (parallel, batches of 10)
    INIT_BATCH = 10

    async def init_bot(path: str, app: Application) -> str | None:
        try:
            await app.initialize()

            await asyncio.gather(
                app.bot.set_my_description(app.bot_data["description"]),
                app.bot.set_my_short_description(app.bot_data["short_description"]),
                app.bot.set_my_commands([
                    BotCommand("start", "🚀 Подключить VPN"),
                ]),
            )

            webhook_url = f"https://{domain}{path}"
            await app.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True,
            )
            logger.info("Webhook set: %s", webhook_url)

            bot_numbers[app.bot.id] = app.bot_data["bot_number"]
            bot_usernames[app.bot.id] = f"@{app.bot.username}" if app.bot.username else "—"
            await app.start()
            return None
        except Exception as exc:
            logger.error("Bot %s failed to start, skipping: %s", path, exc)
            return path

    items = list(apps.items())
    for batch_start in range(0, len(items), INIT_BATCH):
        batch = items[batch_start : batch_start + INIT_BATCH]
        results = await asyncio.gather(
            *(init_bot(path, app) for path, app in batch)
        )
        for path in results:
            if path is not None:
                del apps[path]

    all_apps.update(apps)

    # ── aiohttp web server ──

    async def handle_webhook(request: web.Request) -> web.Response:
        path = request.path
        app = all_apps.get(path)
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
    for path in all_apps:
        http_app.router.add_post(path, handle_webhook)

    runner = web.AppRunner(http_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(
        "Started %d bot(s) on port %d. Admin ID: %s",
        len(all_apps), port, ADMIN_ID or "not set",
    )

    broadcast_task = asyncio.create_task(auto_broadcast_loop())
    logger.info("Auto broadcast scheduled every %d days", AUTO_BROADCAST_INTERVAL // 86400)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await stop_event.wait()

    logger.info("Shutting down...")
    broadcast_task.cancel()
    for app in all_apps.values():
        await app.stop()
        await app.shutdown()
    await close_db()
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run())
