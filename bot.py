import logging
import asyncio
import os
import json
from pathlib import Path
from typing import Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from doma_events import watch_events
from dotenv import load_dotenv

# 🔐 Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

DEFAULT_CHAT_ID = int(os.getenv("DEFAULT_CHAT_ID", "123456789"))
FILTERS_PATH = Path(__file__).with_name("filters.json")
FILTER_CALLBACK_PREFIX = "flt"

TLD_OPTIONS = (".com", ".dev", ".app", ".ai")
PRICE_OPTIONS = (50, 100)
MIN_APPRAISAL_OPTIONS = (1000,)
MAX_LENGTH_OPTIONS = (10,)
KEYWORD_OPTIONS = ("tech", "ai", "finance")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def default_filters() -> dict[str, Any]:
    return {
        "tlds": list(TLD_OPTIONS),
        "max_price": None,
        "min_appraisal": None,
        "max_length": None,
        "keywords": [],
        "trademark_check": True,
    }


def load_filter_store() -> dict[str, Any]:
    if not FILTERS_PATH.exists():
        return {}
    try:
        data = json.loads(FILTERS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def save_filter_store(store: dict[str, Any]) -> None:
    FILTERS_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")


def get_chat_filters(store: dict[str, Any], chat_id: int) -> dict[str, Any]:
    chat_key = str(chat_id)
    raw = store.get(chat_key)
    if not isinstance(raw, dict):
        store[chat_key] = default_filters()
        return store[chat_key]

    merged = default_filters()
    merged.update(raw)

    raw_tlds = merged.get("tlds") if isinstance(merged.get("tlds"), list) else []
    tld_set = {t for t in raw_tlds if isinstance(t, str) and t in TLD_OPTIONS}
    merged["tlds"] = [t for t in TLD_OPTIONS if t in tld_set]
    merged["keywords"] = [
        kw for kw in (merged.get("keywords") if isinstance(merged.get("keywords"), list) else [])
        if isinstance(kw, str) and kw in KEYWORD_OPTIONS
    ]

    numeric_allowed = {
        "max_price": set(PRICE_OPTIONS),
        "min_appraisal": set(MIN_APPRAISAL_OPTIONS),
        "max_length": set(MAX_LENGTH_OPTIONS),
    }
    for key, allowed_values in numeric_allowed.items():
        raw_value = merged.get(key)
        if raw_value is None:
            continue
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            merged[key] = None
            continue
        merged[key] = parsed if parsed in allowed_values else None

    merged["trademark_check"] = bool(merged.get("trademark_check", True))
    store[chat_key] = merged
    return merged


def summary_text(filters: dict[str, Any]) -> str:
    tlds = ", ".join(filters["tlds"]) if filters["tlds"] else "Any"
    price = f"${filters['max_price']}" if filters["max_price"] else "Any"
    appraisal = f"${filters['min_appraisal']}+" if filters["min_appraisal"] else "Any"
    length = f"≤ {filters['max_length']}" if filters["max_length"] else "Any"
    keywords = ", ".join(kw.upper() for kw in filters["keywords"]) if filters["keywords"] else "Any"
    tm = "ON" if filters["trademark_check"] else "OFF"
    return (
        "🎛️ *Pro Filter Command Center*\n"
        f"• TLDs: {tlds}\n"
        f"• Max Price: {price}\n"
        f"• Min Appraisal: {appraisal}\n"
        f"• Max Length: {length}\n"
        f"• Keywords: {keywords}\n"
        f"• Trademark Check: {tm}"
    )


def build_filter_keyboard(filters: dict[str, Any]) -> InlineKeyboardMarkup:
    def chip(on: bool, label: str) -> str:
        return f"{'✅' if on else '▫️'} {label}"

    tlds = set(filters["tlds"])
    keywords = set(filters["keywords"])
    max_price = filters["max_price"]
    min_appraisal = filters["min_appraisal"]
    max_length = filters["max_length"]
    trademark_check = filters["trademark_check"]

    keyboard = [
        [
            InlineKeyboardButton(chip(".com" in tlds, ".com"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tld:com"),
            InlineKeyboardButton(chip(".dev" in tlds, ".dev"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tld:dev"),
            InlineKeyboardButton(chip(".app" in tlds, ".app"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tld:app"),
            InlineKeyboardButton(chip(".ai" in tlds, ".ai"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tld:ai"),
        ],
        [
            InlineKeyboardButton(chip(max_price is None, "Max Price: Any"), callback_data=f"{FILTER_CALLBACK_PREFIX}:maxp:0"),
            InlineKeyboardButton(chip(max_price == 50, "≤$50"), callback_data=f"{FILTER_CALLBACK_PREFIX}:maxp:50"),
            InlineKeyboardButton(chip(max_price == 100, "≤$100"), callback_data=f"{FILTER_CALLBACK_PREFIX}:maxp:100"),
        ],
        [
            InlineKeyboardButton(chip(min_appraisal is None, "Min Appraisal: Any"), callback_data=f"{FILTER_CALLBACK_PREFIX}:mina:0"),
            InlineKeyboardButton(chip(min_appraisal == 1000, "$1000+"), callback_data=f"{FILTER_CALLBACK_PREFIX}:mina:1000"),
        ],
        [
            InlineKeyboardButton(chip(max_length is None, "Max Length: Any"), callback_data=f"{FILTER_CALLBACK_PREFIX}:maxl:0"),
            InlineKeyboardButton(chip(max_length == 10, "≤10 chars"), callback_data=f"{FILTER_CALLBACK_PREFIX}:maxl:10"),
        ],
        [
            InlineKeyboardButton(chip("tech" in keywords, "Tech"), callback_data=f"{FILTER_CALLBACK_PREFIX}:kw:tech"),
            InlineKeyboardButton(chip("ai" in keywords, "AI"), callback_data=f"{FILTER_CALLBACK_PREFIX}:kw:ai"),
            InlineKeyboardButton(chip("finance" in keywords, "Finance"), callback_data=f"{FILTER_CALLBACK_PREFIX}:kw:finance"),
        ],
        [
            InlineKeyboardButton(chip(trademark_check, "Trademark ON"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tm:1"),
            InlineKeyboardButton(chip(not trademark_check, "Trademark OFF"), callback_data=f"{FILTER_CALLBACK_PREFIX}:tm:0"),
        ],
        [InlineKeyboardButton("♻️ Reset Filters", callback_data=f"{FILTER_CALLBACK_PREFIX}:reset")],
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    store = context.application.bot_data.setdefault("chat_filters", load_filter_store())
    get_chat_filters(store, chat_id)
    save_filter_store(store)
    await update.message.reply_text(
        f"👋 Welcome to Doma PulseBot!\n\n"
        f"Your pro domain command center is live.\n"
        f"I'll notify you about high-margin domain arbitrage opportunities.\n"
        f"📡 Registered Chat ID: `{chat_id}`\n\n"
        f"Commands:\n"
        f"/filter - Pro filter menu\n"
        f"/stats - Watcher status\n"
        f"/pause - Pause scans\n"
        f"/resume - Resume scans\n"
        f"/force_scan - Scan now\n"
        f"/help - Full command list",
        parse_mode="Markdown"
    )
    print(f"🆔 New user Chat ID: {chat_id}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧭 *Doma PulseBot Command Center*\n\n"
        "/start — Register chat and quick-start commands\n"
        "/help — Show all commands\n"
        "/filter — Open advanced filter toggles\n"
        "/stats — Show watcher schedule status\n"
        "/pause — Pause polling loop (save API quota)\n"
        "/resume — Resume polling loop\n"
        "/force_scan — Force immediate Atom scan/evaluation",
        parse_mode="Markdown",
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    paused = bool(context.application.bot_data.get("watcher_paused", False))
    await update.message.reply_text(
        "📊 Watcher Status:\n"
        "✅ Bot is online\n"
        f"⏯️ Polling: {'Paused' if paused else 'Running'}\n"
        f"⚡ Turbo window (UTC): {os.getenv('TURBO_HOURS_UTC', '18-21')}\n"
        f"🐢 Eco polling: {os.getenv('ECO_POLL_SECONDS', '120')}s\n"
        f"🚀 Turbo polling: {os.getenv('TURBO_POLL_SECONDS', '8')}s"
    )

async def filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    store = context.application.bot_data.setdefault("chat_filters", load_filter_store())
    filters = get_chat_filters(store, chat_id)
    save_filter_store(store)
    await update.message.reply_text(
        summary_text(filters),
        reply_markup=build_filter_keyboard(filters),
        parse_mode="Markdown",
    )


async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["watcher_paused"] = True
    await update.message.reply_text("⏸️ Polling loop paused. Use /resume to continue or /force_scan to run one immediate cycle.")


async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["watcher_paused"] = False
    force_scan_event = context.application.bot_data.get("force_scan_event")
    if force_scan_event is not None:
        force_scan_event.set()
    await update.message.reply_text("▶️ Polling loop resumed.")


async def force_scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    force_scan_event = context.application.bot_data.get("force_scan_event")
    if force_scan_event is None:
        force_scan_event = asyncio.Event()
        context.application.bot_data["force_scan_event"] = force_scan_event
    force_scan_event.set()
    await update.message.reply_text("🚨 Forced scan queued. Running immediate Atom fetch + evaluation.")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith(f"{FILTER_CALLBACK_PREFIX}:"):
        return

    chat_id = query.message.chat.id
    store = context.application.bot_data.setdefault("chat_filters", load_filter_store())
    filters = get_chat_filters(store, chat_id)

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "reset":
        filters = default_filters()
        store[str(chat_id)] = filters
    elif action == "tld" and len(parts) == 3:
        tld = f".{parts[2].lower()}"
        if tld in TLD_OPTIONS:
            selected = set(filters["tlds"])
            if tld in selected:
                selected.remove(tld)
            else:
                selected.add(tld)
            filters["tlds"] = [value for value in TLD_OPTIONS if value in selected]
    elif action == "maxp" and len(parts) == 3:
        try:
            value = int(parts[2])
        except ValueError:
            value = 0
        filters["max_price"] = value if value > 0 else None
    elif action == "mina" and len(parts) == 3:
        try:
            value = int(parts[2])
        except ValueError:
            value = 0
        filters["min_appraisal"] = value if value > 0 else None
    elif action == "maxl" and len(parts) == 3:
        try:
            value = int(parts[2])
        except ValueError:
            value = 0
        filters["max_length"] = value if value > 0 else None
    elif action == "kw" and len(parts) == 3:
        keyword = parts[2].lower()
        if keyword in KEYWORD_OPTIONS:
            selected = set(filters["keywords"])
            if keyword in selected:
                selected.remove(keyword)
            else:
                selected.add(keyword)
            filters["keywords"] = [value for value in KEYWORD_OPTIONS if value in selected]
    elif action == "tm" and len(parts) == 3:
        filters["trademark_check"] = parts[2] == "1"

    store[str(chat_id)] = filters
    save_filter_store(store)

    await query.edit_message_text(
        summary_text(filters),
        reply_markup=build_filter_keyboard(filters),
        parse_mode="Markdown",
    )

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN not set in .env")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    watcher_task: asyncio.Task | None = None

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("filter", filter_command))
    app.add_handler(CommandHandler("pause", pause_command))
    app.add_handler(CommandHandler("resume", resume_command))
    app.add_handler(CommandHandler("force_scan", force_scan_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def post_init(application):
        nonlocal watcher_task
        application.bot_data.setdefault("chat_filters", load_filter_store())
        application.bot_data.setdefault("watcher_paused", False)
        application.bot_data.setdefault("force_scan_event", asyncio.Event())
        watcher_task = asyncio.create_task(watch_events(application, DEFAULT_CHAT_ID))
        logging.info("✅ Background domain watcher started")

    async def post_shutdown(_application):
        if watcher_task:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
            logging.info("🛑 Background domain watcher stopped")

    app.post_init = post_init
    app.post_shutdown = post_shutdown
    print("🤖 Bot & Event Watcher running...")
    app.run_polling()

if __name__ == "__main__":
    main()
