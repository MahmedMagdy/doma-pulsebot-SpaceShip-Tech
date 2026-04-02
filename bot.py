# bot.py (safe + works with doma_events.py)
import logging
import asyncio
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from doma_events import watch_events
from dotenv import load_dotenv

# 🔐 Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# 🆔 Replace with your actual chat ID after testing /start
DEFAULT_CHAT_ID = int(os.getenv("DEFAULT_CHAT_ID", "123456789"))

# 🔧 Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ✅ Start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"👋 Welcome to Doma PulseBot!\n\n"
        f"I'll notify you about domain sales, expirations, and deals.\n"
        f"📡 Registered your Chat ID: `{chat_id}`\n\n"
        f"Use /filter to set preferences.\nUse /stats for market insights.",
        parse_mode="Markdown"
    )
    # Print ID in logs so you can update .env
    print(f"🆔 New user Chat ID: {chat_id}")

# 📊 Show fake stats for demo
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 Daily Stats:\n"
        "🔥 23 domains sold today\n"
        "💰 Average price: 8.4 USDC\n"
        "🚀 Hottest TLD: .ape"
    )

# ⚙️ Set filters (for UI)
async def filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton(".ape", callback_data='filter_ape')],
        [InlineKeyboardButton("Price < 10 USDC", callback_data='filter_price')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose your filter:", reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(f"✅ Filter set: {query.data}")

# 🚀 Main
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN not set in .env")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    watcher_task: asyncio.Task | None = None

    # Bot Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("filter", filter_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def post_init(application):
        nonlocal watcher_task
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
