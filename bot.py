import asyncio
import datetime
import hashlib
import logging
import os
import signal
import time
from collections import defaultdict

import asyncpg
from aiohttp import web
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
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


def parse_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_IDS", "") or os.getenv("ADMIN_ID", "")
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


ADMIN_IDS: set[int] = parse_admin_ids()

# Maximum number of bots served by this process (env BOT_TOKEN_1..BOT_TOKEN_400)
MAX_BOTS = int(os.getenv("MAX_BOTS", "400"))
# How many bots are initialized (webhook + profile) in parallel on boot
INIT_BATCH = int(os.getenv("INIT_BATCH", "20"))
# How many bots broadcast in parallel (each bot has its own Telegram rate limit)
BROADCAST_CONCURRENCY = int(os.getenv("BROADCAST_CONCURRENCY", "25"))
# Pause between two messages sent by the same bot (~25 msg/s per bot)
SEND_DELAY = float(os.getenv("SEND_DELAY", "0.04"))

all_apps: dict[str, Application] = {}
bot_numbers: dict[int, int] = {}
bot_usernames: dict[int, str] = {}
bot_links: dict[int, str] = {}
db_pool: asyncpg.Pool | None = None
webhook_domain: str = ""
next_bot_number: int = 1

# Guards against two broadcasts (manual or automatic) running at once
broadcast_running: bool = False
background_tasks: set[asyncio.Task] = set()


def spawn_task(coro) -> asyncio.Task:
    """Run a coroutine in the background and keep a strong reference to it."""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


RATE_LIMIT = 10
RATE_WINDOW = 60
rate_limits: dict[tuple[int, int], list[float]] = defaultdict(list)

(
    MENU,
    BC_TEXT,
    BC_PHOTO,
    BC_BTN_TEXT,
    BC_BTN_URL,
    BC_CONFIRM,
    ADD_BOT_TOKEN,
    AUTO_BC_CONFIRM,
) = range(8)

# ── Database ──────────────────────────────────────────────────────────

async def init_db() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=int(os.getenv("DB_POOL_MIN", "5")),
        max_size=int(os.getenv("DB_POOL_MAX", "40")),
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                bot_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                first_seen DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (bot_id, user_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auto_broadcast_log (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_sent DOUBLE PRECISION NOT NULL,
                CHECK (id = 1)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_tokens (
                token TEXT PRIMARY KEY,
                referral_link TEXT,
                added_at DOUBLE PRECISION NOT NULL
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_first_seen "
            "ON users (bot_id, first_seen)"
        )
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


async def get_detailed_stats() -> dict[int, dict]:
    now = time.time()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bot_id, "
            "  COUNT(*) AS total, "
            "  COUNT(*) FILTER (WHERE first_seen >= $1) AS today, "
            "  COUNT(*) FILTER (WHERE first_seen >= $2) AS week, "
            "  COUNT(*) FILTER (WHERE first_seen >= $3) AS month "
            "FROM users GROUP BY bot_id",
            now - 86400, now - 604800, now - 2592000,
        )
        return {
            r["bot_id"]: {
                "total": r["total"],
                "today": r["today"],
                "week": r["week"],
                "month": r["month"],
            }
            for r in rows
        }


async def get_total_dynamics() -> dict:
    now = time.time()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT "
            "  COUNT(DISTINCT user_id) AS total, "
            "  COUNT(DISTINCT user_id) FILTER (WHERE first_seen >= $1) AS today, "
            "  COUNT(DISTINCT user_id) FILTER (WHERE first_seen >= $2) AS week, "
            "  COUNT(DISTINCT user_id) FILTER (WHERE first_seen >= $3) AS month "
            "FROM users",
            now - 86400, now - 604800, now - 2592000,
        )
        return dict(row)


async def get_last_auto_broadcast() -> float | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_sent FROM auto_broadcast_log WHERE id = 1"
        )
        return row["last_sent"] if row else None


async def set_last_auto_broadcast(ts: float) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO auto_broadcast_log (id, last_sent) VALUES (1, $1) "
            "ON CONFLICT (id) DO UPDATE SET last_sent = $1",
            ts,
        )


async def save_bot_token(token: str, referral_link: str | None) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bot_tokens (token, referral_link, added_at) "
            "VALUES ($1, $2, $3) ON CONFLICT (token) DO NOTHING",
            token, referral_link, time.time(),
        )


async def get_db_bot_tokens() -> list[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT token, referral_link FROM bot_tokens")
        return [dict(r) for r in rows]


async def delete_bot_token(token: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM bot_tokens WHERE token = $1", token)


# ── Rate limiting ─────────────────────────────────────────────────────

def prune_rate_limits(now: float) -> None:
    stale = [k for k, v in rate_limits.items() if not v or now - v[-1] > RATE_WINDOW]
    for k in stale:
        del rate_limits[k]


def check_rate_limit(bot_id: int, user_id: int) -> bool:
    key = (bot_id, user_id)
    now = time.time()
    if len(rate_limits) > 50_000:
        prune_rate_limits(now)
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


# ── Links ─────────────────────────────────────────────────────────────

FALLBACK_REFERRAL_LINK = "https://t.me/atlassecure_bot?start=ref_UEGJ3A"

DEFAULT_BUTTON_TEXT = "🚀 Подключить VPN"

# Button shown under the "we are moving to a new bot" broadcast
AUTO_BROADCAST_BUTTON_TEXT = os.getenv(
    "AUTO_BROADCAST_BUTTON_TEXT", "🚀 Перейти в новый бот"
)


def normalize_url(url: str | None) -> str | None:
    """Return a URL Telegram accepts in an inline button, or None if unusable."""
    url = (url or "").strip()
    if not url or " " in url or "\n" in url:
        return None
    low = url.lower()
    if low.startswith(("https://", "http://", "tg://")):
        return url
    if low.startswith("@") and len(url) > 1:
        return "https://t.me/" + url[1:]
    if low.startswith(("t.me/", "telegram.me/", "www.")):
        return "https://" + url
    if "." in url and "/" not in url.split(".")[0]:
        return "https://" + url
    return None


def default_referral_link() -> str:
    """Referral link used when a bot has no personal one."""
    return (
        normalize_url(os.getenv("DEFAULT_REFERRAL_LINK"))
        or FALLBACK_REFERRAL_LINK
    )


def referral_link_of(bot_data: dict) -> str:
    """Always returns a valid URL, so the inline button never breaks a send."""
    return normalize_url(bot_data.get("referral_link")) or default_referral_link()


def link_markup(text: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(text, url=url)]])


# ── Handlers ──────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    bot_id = context.bot.id
    user_id = update.effective_user.id
    if not check_rate_limit(bot_id, user_id):
        return
    try:
        await save_user(bot_id, user_id)
    except Exception:
        logger.exception("DB error saving user %d for bot %d", user_id, bot_id)
    referral_link = referral_link_of(context.bot_data)
    await update.message.reply_text(
        MESSAGE_TEXT,
        reply_markup=link_markup(DEFAULT_BUTTON_TEXT, referral_link),
    )


async def ignore_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass


# ── Admin ─────────────────────────────────────────────────────────────

def is_admin(update: Update) -> bool:
    return (
        bool(ADMIN_IDS)
        and update.effective_user is not None
        and update.effective_user.id in ADMIN_IDS
    )


def clear_bc(context: ContextTypes.DEFAULT_TYPE) -> None:
    for k in ("bc_text", "bc_photo", "bc_btn_text", "bc_btn_url"):
        context.user_data.pop(k, None)


async def format_auto_broadcast_info() -> str:
    last = await get_last_auto_broadcast()
    days = AUTO_BROADCAST_INTERVAL // 86400
    if last is None:
        return f"🔄 Авто-рассылка: каждые {days}д — ещё не отправлялась"
    last_dt = datetime.datetime.fromtimestamp(last, tz=datetime.timezone.utc)
    next_ts = last + AUTO_BROADCAST_INTERVAL
    now = time.time()
    if next_ts > now:
        remaining_h = (next_ts - now) / 3600
        remaining_str = (
            f"{remaining_h / 24:.1f}д" if remaining_h >= 24 else f"{remaining_h:.1f}ч"
        )
        return (
            f"🔄 Авто-рассылка: каждые {days}д\n"
            f"   Последняя: {last_dt:%d.%m.%Y %H:%M} UTC\n"
            f"   Следующая через: {remaining_str}"
        )
    return (
        f"🔄 Авто-рассылка: каждые {days}д\n"
        f"   Последняя: {last_dt:%d.%m.%Y %H:%M} UTC\n"
        f"   Следующая: скоро"
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ConversationHandler.END
    clear_bc(context)
    dyn = await get_total_dynamics()
    auto_info = await format_auto_broadcast_info()
    keyboard = [
        [InlineKeyboardButton("📨 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton("🚀 Сделать рассылку «Переезд»", callback_data="auto_bc")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("➕ Добавить бота", callback_data="add_bot")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="close")],
    ]
    await update.message.reply_text(
        f"🔐 Админ-панель\n\n"
        f"🤖 Ботов: {len(all_apps)} / {MAX_BOTS}\n"
        f"👥 Всего: {dyn['total']}  |  "
        f"📅 +{dyn['today']}  |  "
        f"📆 +{dyn['week']}  |  "
        f"🗓 +{dyn['month']}\n\n"
        f"{auto_info}",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return MENU


async def menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "broadcast":
        clear_bc(context)
        await query.edit_message_text(
            "📨 Шаг 1/4 — Текст\n\n"
            "Отправьте текст рассылки.\n"
            "/cancel — отмена"
        )
        return BC_TEXT

    if query.data == "auto_bc":
        if broadcast_running:
            await query.edit_message_text(
                "⏳ Рассылка уже выполняется. Дождитесь отчёта и попробуйте снова."
            )
            return ConversationHandler.END
        referral_link = referral_link_of(context.bot_data)
        keyboard = [
            # Real button — exactly what users will receive, tappable for a check
            [InlineKeyboardButton(AUTO_BROADCAST_BUTTON_TEXT, url=referral_link)],
            [InlineKeyboardButton("✅ Отправить всем", callback_data="auto_bc_go")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
        ]
        await query.edit_message_text(
            f"🚀 Рассылка «Переезжаем в новый бот»\n"
            f"{'─' * 30}\n"
            f"{AUTO_BROADCAST_TEXT}\n"
            f"{'─' * 30}\n"
            f"🔘 Кнопка: «{AUTO_BROADCAST_BUTTON_TEXT}»\n"
            f"🔗 Ссылка: реф-ссылка каждого бота\n"
            f"   (в этом боте: {referral_link})\n"
            f"🤖 Ботов: {len(all_apps)}\n\n"
            f"После отправки отсчёт авто-рассылки начнётся заново.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True,
        )
        return AUTO_BC_CONFIRM

    if query.data == "add_bot":
        await query.edit_message_text(
            "➕ Добавить бота\n\n"
            "Отправьте токен бота (получить в @BotFather).\n"
            "Формат: 123456789:ABCDEF...\n\n"
            "/cancel — отмена"
        )
        return ADD_BOT_TOKEN

    if query.data == "stats":
        stats = await get_detailed_stats()
        dyn = await get_total_dynamics()
        header = (
            f"📊 Статистика по ботам\n\n"
            f"👥 Уникальных: {dyn['total']}  |  "
            f"📅 +{dyn['today']}  |  "
            f"📆 +{dyn['week']}  |  "
            f"🗓 +{dyn['month']}\n"
            f"🤖 Активных ботов: {len(all_apps)}\n"
            f"{'─' * 30}\n"
        )
        lines: list[str] = []
        for bot_id, d in sorted(
            stats.items(), key=lambda x: bot_numbers.get(x[0], 0)
        ):
            num = bot_numbers.get(bot_id, "?")
            name = bot_usernames.get(bot_id, "—")
            link = bot_links.get(bot_id, "")
            growth = ""
            if d["today"]:
                growth += f" 📅+{d['today']}"
            if d["week"]:
                growth += f" 📆+{d['week']}"
            lines.append(
                f"#{num} {name}\n"
                f"   👥 {d['total']}{growth}\n"
                f"   🔗 {link}"
            )
        if not lines:
            lines.append("Пока нет данных")
        chunks: list[str] = []
        current = header
        for line in lines:
            entry = line + "\n"
            if len(current) + len(entry) > 4000:
                chunks.append(current)
                current = ""
            current += entry
        if current:
            chunks.append(current)
        await query.edit_message_text(chunks[0], disable_web_page_preview=True)
        for chunk in chunks[1:]:
            await query.message.reply_text(chunk, disable_web_page_preview=True)
            await asyncio.sleep(0.1)
        return ConversationHandler.END

    await query.edit_message_text("Админ-панель закрыта.")
    return ConversationHandler.END


# ── Move broadcast: manual trigger from the admin panel ──

async def auto_bc_confirm_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    await query.answer()

    if query.data != "auto_bc_go":
        await query.edit_message_text("❌ Рассылка отменена.")
        return ConversationHandler.END

    if broadcast_running:
        await query.edit_message_text("⏳ Рассылка уже выполняется.")
        return ConversationHandler.END

    await query.edit_message_text(
        f"⏳ Рассылка «Переезд» запущена по {len(all_apps)} ботам.\n"
        f"Отчёт придёт сюда по завершении."
    )
    # In the background: a webhook update must not wait for the whole broadcast
    spawn_task(
        run_auto_broadcast(manual_by=query.from_user.id, notify_bot=context.bot)
    )
    return ConversationHandler.END


# ── ADD_BOT_TOKEN ──

async def add_bot_token_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    global next_bot_number
    token = update.message.text.strip()

    if ":" not in token:
        await update.message.reply_text(
            "❌ Неверный формат токена. Попробуйте ещё раз или /cancel"
        )
        return ADD_BOT_TOKEN

    path = webhook_path(token)
    if path in all_apps:
        await update.message.reply_text("⚠️ Этот бот уже добавлен и активен.")
        return ConversationHandler.END

    if len(all_apps) >= MAX_BOTS:
        await update.message.reply_text(
            f"❌ Достигнут лимит в {MAX_BOTS} ботов.\n"
            f"Увеличьте переменную MAX_BOTS, чтобы добавить больше."
        )
        return ConversationHandler.END

    await update.message.reply_text("⏳ Проверяю токен...")

    default_link = default_referral_link()

    try:
        app = build_app(token, default_link, BOT_DESCRIPTION, BOT_SHORT_DESCRIPTION)
        await app.initialize()

        await asyncio.gather(
            app.bot.set_my_description(BOT_DESCRIPTION),
            app.bot.set_my_short_description(BOT_SHORT_DESCRIPTION),
            app.bot.set_my_commands([
                BotCommand("start", "🚀 Подключить VPN"),
            ]),
        )

        wh_url = f"https://{webhook_domain}{path}"
        await app.bot.set_webhook(
            url=wh_url,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )

        num = next_bot_number
        next_bot_number += 1
        app.bot_data["bot_number"] = num

        bot_numbers[app.bot.id] = num
        username = app.bot.username or ""
        bot_usernames[app.bot.id] = f"@{username}" if username else "—"
        bot_links[app.bot.id] = f"https://t.me/{username}" if username else "—"

        await app.start()
        all_apps[path] = app

        await save_bot_token(token, default_link)

        logger.info("Bot %d hot-added: @%s", num, username)

        await update.message.reply_text(
            f"✅ Бот добавлен и запущен!\n\n"
            f"🤖 #{num} @{username}\n"
            f"🔗 https://t.me/{username}\n\n"
            f"Бот сохранён в БД — переживёт редеплой."
        )
    except Exception as exc:
        logger.error("Failed to add bot: %s", exc)
        await update.message.reply_text(
            f"❌ Не удалось запустить бота:\n{exc}\n\n"
            f"Проверьте токен и попробуйте ещё раз или /cancel"
        )
        return ADD_BOT_TOKEN

    return ConversationHandler.END


# ── Broadcast steps ──────────────────────────────────────────────────

async def bc_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["bc_text"] = update.message.text
    keyboard = [[InlineKeyboardButton("⏭ Пропустить", callback_data="skip_photo")]]
    await update.message.reply_text(
        "📨 Шаг 2/4 — Фото\n\n"
        "Отправьте фото или нажмите «Пропустить».",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return BC_PHOTO


async def bc_photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["bc_photo"] = update.message.photo[-1].file_id
    keyboard = [[InlineKeyboardButton("⏭ Пропустить", callback_data="skip_btn")]]
    await update.message.reply_text(
        "📨 Шаг 3/4 — Кнопка\n\n"
        "Введите текст кнопки (например: Забрать скидку, Подключиться, Перейти)\n"
        "или нажмите «Пропустить» для кнопки по умолчанию.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return BC_BTN_TEXT


async def bc_photo_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["bc_photo"] = None
    keyboard = [[InlineKeyboardButton("⏭ Пропустить", callback_data="skip_btn")]]
    await query.edit_message_text(
        "📨 Шаг 3/4 — Кнопка\n\n"
        "Введите текст кнопки (например: Забрать скидку, Подключиться, Перейти)\n"
        "или нажмите «Пропустить» для кнопки по умолчанию.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return BC_BTN_TEXT


async def bc_btn_text_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data["bc_btn_text"] = update.message.text
    await update.message.reply_text(
        "📨 Шаг 4/4 — URL кнопки\n\n"
        "Отправьте ссылку для кнопки.\n"
        "Или отправьте «реф» — будет использована реф-ссылка бота."
    )
    return BC_BTN_URL


async def bc_btn_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["bc_btn_text"] = None
    context.user_data["bc_btn_url"] = None
    return await show_preview(query.message.chat_id, context)


async def bc_btn_url_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    raw = update.message.text.strip()
    if raw.lower() == "реф":
        context.user_data["bc_btn_url"] = None
        return await show_preview(update.message.chat_id, context)
    url = normalize_url(raw)
    if not url:
        await update.message.reply_text(
            "❌ Неверная ссылка. Пример: https://t.me/mybot?start=ref_ABC\n"
            "Отправьте ссылку ещё раз, «реф» — реф-ссылка бота, /cancel — отмена."
        )
        return BC_BTN_URL
    context.user_data["bc_btn_url"] = url
    return await show_preview(update.message.chat_id, context)


async def show_preview(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = context.user_data
    text = ud.get("bc_text", "")
    photo = ud.get("bc_photo")
    btn_text = ud.get("bc_btn_text")
    btn_url = ud.get("bc_btn_url")

    preview_parts = ["📨 Предпросмотр рассылки:\n"]
    preview_parts.append(f"📝 Текст: {text[:200]}{'…' if len(text) > 200 else ''}")
    preview_parts.append(f"🖼 Фото: {'Да' if photo else 'Нет'}")
    if btn_text:
        url_display = btn_url or "реф-ссылка бота"
        preview_parts.append(f"🔘 Кнопка: «{btn_text}» → {url_display}")
    else:
        preview_parts.append("🔘 Кнопка: «🚀 Подключить VPN» → реф-ссылка бота")

    preview_url = btn_url or referral_link_of(context.bot_data)
    keyboard = [
        # Real button — the admin can tap it to verify the link works
        [InlineKeyboardButton(btn_text or DEFAULT_BUTTON_TEXT, url=preview_url)],
        [InlineKeyboardButton("✅ Отправить всем", callback_data="confirm")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")],
    ]
    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(preview_parts),
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True,
    )
    return BC_CONFIRM


async def bc_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    global broadcast_running
    query = update.callback_query
    await query.answer()

    if query.data == "cancel":
        await query.edit_message_text("❌ Рассылка отменена.")
        clear_bc(context)
        return ConversationHandler.END

    ud = context.user_data
    text = ud.get("bc_text", "")
    photo = ud.get("bc_photo")
    btn_text = ud.get("bc_btn_text")
    btn_url = ud.get("bc_btn_url")

    if not text:
        await query.edit_message_text("❌ Текст рассылки не найден.")
        return ConversationHandler.END

    if broadcast_running:
        await query.edit_message_text(
            "⏳ Другая рассылка уже выполняется. Дождитесь отчёта."
        )
        return ConversationHandler.END

    await query.edit_message_text(
        f"⏳ Рассылка запущена по {len(all_apps)} ботам.\n"
        f"Отчёт придёт сюда по завершении."
    )

    admin_id = query.from_user.id
    bot = context.bot

    async def worker() -> None:
        global broadcast_running
        broadcast_running = True
        started = time.time()
        try:
            totals = await broadcast_to_all(
                text=text,
                photo=photo,
                btn_text=btn_text,
                btn_url=btn_url,
                skip_admins=True,
            )
        except Exception:
            logger.exception("Broadcast failed")
            totals = {"sent": 0, "failed": 0, "bots": 0}
        finally:
            broadcast_running = False
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=(
                    f"✅ Рассылка завершена!\n\n"
                    f"📨 Отправлено: {totals['sent']}\n"
                    f"❌ Не доставлено: {totals['failed']}\n"
                    f"🤖 Ботов: {totals['bots']}\n"
                    f"⏱ Заняло: {(time.time() - started) / 60:.1f} мин"
                ),
            )
        except Exception:
            logger.exception("Cannot deliver broadcast report to admin %d", admin_id)

    # Background task: the webhook request must return immediately, otherwise
    # Telegram retries the callback and the broadcast would start twice.
    spawn_task(worker())
    clear_bc(context)
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    clear_bc(context)
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ── Broadcast engine ──────────────────────────────────────────────────

async def send_one(
    bot, user_id: int, text: str, photo: str | None, markup: InlineKeyboardMarkup
) -> bool:
    """Send a single broadcast message, honouring Telegram flood control."""
    for _attempt in range(3):
        try:
            if photo:
                await bot.send_photo(
                    chat_id=user_id, photo=photo, caption=text, reply_markup=markup
                )
            else:
                await bot.send_message(
                    chat_id=user_id, text=text, reply_markup=markup
                )
            return True
        except RetryAfter as exc:
            wait = min(float(getattr(exc, "retry_after", 5)) + 1, 60)
            await asyncio.sleep(wait)
        except (Forbidden, BadRequest):
            return False  # blocked bot / deleted account — no point retrying
        except TelegramError:
            await asyncio.sleep(1)
        except Exception:
            return False
    return False


async def broadcast_to_all(
    text: str,
    photo: str | None = None,
    btn_text: str | None = None,
    btn_url: str | None = None,
    skip_admins: bool = False,
) -> dict:
    """Send a message from every running bot to all of its users.

    Bots work in parallel (each one has its own Telegram rate limit), which
    keeps a 400-bot broadcast within a reasonable time window.
    When btn_url is empty, each bot uses its own referral link.
    """
    apps = list(all_apps.values())
    if not apps:
        return {"sent": 0, "failed": 0, "bots": 0}

    final_btn_text = (btn_text or DEFAULT_BUTTON_TEXT).strip() or DEFAULT_BUTTON_TEXT
    fixed_url = normalize_url(btn_url)
    semaphore = asyncio.Semaphore(max(1, BROADCAST_CONCURRENCY))
    totals = {"sent": 0, "failed": 0, "bots": 0}

    async def run_for_bot(app: Application) -> None:
        async with semaphore:
            bot_id = app.bot.id
            try:
                users = await get_users_for_bot(bot_id)
            except Exception:
                logger.exception("Broadcast: cannot load users for bot %d", bot_id)
                return
            if not users:
                return
            # Per-bot referral link unless the admin supplied an explicit URL
            url = fixed_url or referral_link_of(app.bot_data)
            markup = link_markup(final_btn_text, url)
            sent = failed = 0
            for user_id in users:
                if skip_admins and user_id in ADMIN_IDS:
                    continue
                if await send_one(app.bot, user_id, text, photo, markup):
                    sent += 1
                else:
                    failed += 1
                await asyncio.sleep(SEND_DELAY)
            totals["sent"] += sent
            totals["failed"] += failed
            totals["bots"] += 1
            logger.info(
                "Broadcast via bot #%s: sent=%d failed=%d",
                bot_numbers.get(bot_id, "?"), sent, failed,
            )

    await asyncio.gather(*(run_for_bot(app) for app in apps))
    return totals


# ── Auto broadcast ────────────────────────────────────────────────────

async def auto_broadcast_loop() -> None:
    await asyncio.sleep(10)
    while True:
        try:
            last = await get_last_auto_broadcast()
            now = time.time()
            if last is not None:
                remaining = AUTO_BROADCAST_INTERVAL - (now - last)
                if remaining > 0:
                    logger.info(
                        "Auto broadcast: next in %.1f hours", remaining / 3600
                    )
                    await asyncio.sleep(remaining)
                    continue
            result = await run_auto_broadcast()
            if result.get("busy"):
                # a manual broadcast is in progress — retry a bit later
                await asyncio.sleep(300)
            else:
                # safety pause so an empty run can never spin the loop
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("Auto broadcast error: %s", exc)
            await asyncio.sleep(3600)


async def run_auto_broadcast(
    manual_by: int | None = None, notify_bot=None
) -> dict:
    """Send the "we are moving to a new bot" broadcast to every user.

    Runs on a timer and can also be launched from the admin panel
    (manual_by = admin user id). Either way the 7-day timer is reset, so
    users never receive the same message twice in a row.
    """
    global broadcast_running

    if not all_apps:
        return {"sent": 0, "failed": 0, "bots": 0}
    if broadcast_running:
        logger.warning("Auto broadcast skipped: another broadcast is running")
        return {"sent": 0, "failed": 0, "bots": 0, "busy": True}

    broadcast_running = True
    started = time.time()
    try:
        totals = await broadcast_to_all(
            text=AUTO_BROADCAST_TEXT,
            btn_text=AUTO_BROADCAST_BUTTON_TEXT,
            btn_url=None,  # each bot links to its own referral link
        )
    finally:
        broadcast_running = False

    now = time.time()
    await set_last_auto_broadcast(now)
    logger.info(
        "Auto broadcast done (%s): sent=%d, failed=%d, bots=%d, %.1fs",
        "manual" if manual_by else "scheduled",
        totals["sent"], totals["failed"], totals["bots"], now - started,
    )

    next_dt = datetime.datetime.fromtimestamp(
        now + AUTO_BROADCAST_INTERVAL, tz=datetime.timezone.utc
    )
    report = (
        f"{'🚀 Рассылка «Переезд» отправлена' if manual_by else '🔄 Авто-рассылка завершена'}\n\n"
        f"📨 Отправлено: {totals['sent']}\n"
        f"❌ Не доставлено: {totals['failed']}\n"
        f"🤖 Ботов: {totals['bots']}\n"
        f"⏱ Заняло: {(now - started) / 60:.1f} мин\n\n"
        f"⏭ Следующая авто-рассылка: {next_dt:%d.%m.%Y %H:%M} UTC"
    )
    sender = notify_bot or next(
        (app.bot for app in all_apps.values()), None
    )
    if sender:
        for admin_id in ADMIN_IDS:
            try:
                await sender.send_message(chat_id=admin_id, text=report)
            except Exception:
                pass
    return totals


# ── App builder ───────────────────────────────────────────────────────

def build_app(
    token: str, referral_link: str, bot_desc: str, bot_short_desc: str
) -> Application:
    app = Application.builder().token(token).updater(None).build()
    app.bot_data["referral_link"] = referral_link
    app.bot_data["description"] = bot_desc
    app.bot_data["short_description"] = bot_short_desc

    admin_conv = ConversationHandler(
        entry_points=[CommandHandler("admin", admin_cmd)],
        states={
            MENU: [CallbackQueryHandler(menu_cb)],
            ADD_BOT_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_bot_token_handler),
            ],
            BC_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bc_text_handler),
            ],
            BC_PHOTO: [
                MessageHandler(filters.PHOTO, bc_photo_received),
                CallbackQueryHandler(bc_photo_skip, pattern=r"^skip_photo$"),
            ],
            BC_BTN_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bc_btn_text_handler),
                CallbackQueryHandler(bc_btn_skip, pattern=r"^skip_btn$"),
            ],
            BC_BTN_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bc_btn_url_handler),
            ],
            BC_CONFIRM: [CallbackQueryHandler(bc_confirm_cb)],
            AUTO_BC_CONFIRM: [
                CallbackQueryHandler(
                    auto_bc_confirm_cb, pattern=r"^(auto_bc_go|cancel)$"
                ),
            ],
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
    global webhook_domain, next_bot_number

    port = int(os.getenv("PORT", "8080"))
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("WEBHOOK_DOMAIN")

    if not domain:
        logger.error(
            "Set RAILWAY_PUBLIC_DOMAIN or WEBHOOK_DOMAIN env var "
            "(e.g. myapp.up.railway.app)"
        )
        return

    if not DATABASE_URL:
        logger.error("Set DATABASE_URL env var")
        return

    webhook_domain = domain
    await init_db()

    default_link = default_referral_link()

    apps: dict[str, Application] = {}
    used_tokens: set[str] = set()

    # Load from env: BOT_TOKEN_1 .. BOT_TOKEN_400
    for i in range(1, MAX_BOTS + 1):
        token = os.getenv(f"BOT_TOKEN_{i}")
        if not token:
            continue
        used_tokens.add(token)
        referral_link = os.getenv(f"REFERRAL_LINK_{i}", default_link)
        bot_desc = os.getenv(f"BOT_DESCRIPTION_{i}", BOT_DESCRIPTION)
        bot_short_desc = os.getenv(f"BOT_SHORT_DESCRIPTION_{i}", BOT_SHORT_DESCRIPTION)

        path = webhook_path(token)
        app = build_app(token, referral_link, bot_desc, bot_short_desc)
        app.bot_data["bot_number"] = i
        apps[path] = app
        next_bot_number = max(next_bot_number, i + 1)
        logger.info("Bot %d configured from env (token ...%s)", i, token[-6:])

    # Load from DB (tokens added via admin panel)
    db_tokens = await get_db_bot_tokens()
    for row in db_tokens:
        token = row["token"]
        if token in used_tokens:
            continue
        used_tokens.add(token)
        referral_link = row["referral_link"] or default_link
        path = webhook_path(token)
        app = build_app(token, referral_link, BOT_DESCRIPTION, BOT_SHORT_DESCRIPTION)
        num = next_bot_number
        next_bot_number += 1
        app.bot_data["bot_number"] = num
        apps[path] = app
        logger.info("Bot %d configured from DB (token ...%s)", num, token[-6:])

    if not apps:
        logger.error("No bot tokens found in env or DB.")
        return

    # Initialize bots in parallel batches
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
            wh_url = f"https://{domain}{path}"
            await app.bot.set_webhook(
                url=wh_url,
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True,
            )
            logger.info("Webhook set: %s", wh_url)
            bot_numbers[app.bot.id] = app.bot_data["bot_number"]
            username = app.bot.username or ""
            bot_usernames[app.bot.id] = f"@{username}" if username else "—"
            bot_links[app.bot.id] = f"https://t.me/{username}" if username else "—"
            await app.start()
            return None
        except Exception as exc:
            logger.error("Bot %s failed to start, skipping: %s", path, exc)
            return path

    items = list(apps.items())
    for batch_start in range(0, len(items), INIT_BATCH):
        batch = items[batch_start : batch_start + INIT_BATCH]
        results = await asyncio.gather(
            *(init_bot(p, a) for p, a in batch)
        )
        for p in results:
            if p is not None:
                del apps[p]

    all_apps.update(apps)

    # Wildcard webhook route — supports dynamically added bots
    async def handle_webhook(request: web.Request) -> web.Response:
        p = request.path
        app = all_apps.get(p)
        if not app:
            return web.Response(status=404)
        try:
            data = await request.json()
            update = Update.de_json(data, app.bot)
            await app.process_update(update)
        except Exception:
            logger.exception("Error processing update for %s", p)
        return web.Response(status=200)

    async def handle_health(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    http_app = web.Application()
    http_app.router.add_get("/", handle_health)
    http_app.router.add_post("/webhook/{token_hash}", handle_webhook)

    runner = web.AppRunner(http_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(
        "Started %d/%d bot(s) on port %d. Admins: %s",
        len(all_apps), MAX_BOTS, port, ADMIN_IDS or "not set",
    )

    broadcast_task = asyncio.create_task(auto_broadcast_loop())
    logger.info(
        "Auto broadcast scheduled every %d days", AUTO_BROADCAST_INTERVAL // 86400
    )

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
