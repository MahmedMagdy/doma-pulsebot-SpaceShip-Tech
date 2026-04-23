import asyncio
import logging
import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from doma_events import MAIN_CHAT_ID, fetch_spaceship_domains, watch_events

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger(__name__)


async def start(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 Doma PulseBot is live.\n"
        f"🎯 Alert routing is locked to chat/topic: {MAIN_CHAT_ID} / 20253\n"
        "Commands: /stats /pause /resume /force_scan /help"
    )


async def help_command(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "/start - show mission routing\n"
        "/stats - watcher status\n"
        "/pause - pause scans\n"
        "/resume - resume scans\n"
        "/force_scan - run one scan cycle now"
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    paused = bool(context.application.bot_data.get("watcher_paused", False))
    latest = context.application.bot_data.get("latest_scan_summary", {})
    await update.message.reply_text(
        "📊 Watcher Status\n"
        f"Polling: {'Paused' if paused else 'Running'}\n"
        f"Checked: {int(latest.get('domains_checked', 0))}\n"
        f"Available VIP: {int(latest.get('vip_matches', 0))}\n"
        f"Available General: {int(latest.get('general_finds', 0))}\n"
        f"API Blocked/Failed: {int(latest.get('api_blocked_failed', 0))}"
    )


async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.application.bot_data["watcher_paused"] = True
    resume_event = context.application.bot_data.get("watcher_resume_event")
    if isinstance(resume_event, asyncio.Event):
        resume_event.set()
    await update.message.reply_text("⏸️ Polling loop paused.")


async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.application.bot_data["watcher_paused"] = False
    resume_event = context.application.bot_data.get("watcher_resume_event")
    if isinstance(resume_event, asyncio.Event):
        resume_event.set()
    await update.message.reply_text("▶️ Polling loop resumed.")


async def force_scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🚨 Forced scan started.")
    try:
        summary = await fetch_spaceship_domains(context.application)
    except Exception as exc:
        LOGGER.exception("Direct /force_scan failed: %s", exc)
        summary = context.application.bot_data.get("latest_scan_summary", {}) or {}
    await update.message.reply_text(
        "✅ Scan Cycle Finished\n"
        f"Checked: {int(summary.get('domains_checked', 0))}\n"
        f"API Blocked/Failed: {int(summary.get('api_blocked_failed', 0))}\n"
        f"Available VIP: {int(summary.get('vip_matches', 0))}\n"
        f"Available General: {int(summary.get('general_finds', 0))}"
    )


def main() -> None:
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN not set in .env")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    watcher_task: asyncio.Task | None = None

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("pause", pause_command))
    app.add_handler(CommandHandler("resume", resume_command))
    app.add_handler(CommandHandler("force_scan", force_scan_command))

    async def post_init(application):
        nonlocal watcher_task
        application.bot_data.setdefault("watcher_paused", False)
        application.bot_data.setdefault("watcher_resume_event", asyncio.Event())

        def _start_watcher() -> asyncio.Task:
            task = asyncio.create_task(watch_events(application))
            task.add_done_callback(_watcher_done)
            return task

        async def _restart_watcher() -> None:
            nonlocal watcher_task
            await asyncio.sleep(2)
            watcher_task = _start_watcher()
            LOGGER.info("Background domain watcher restarted")

        def _watcher_done(task: asyncio.Task) -> None:
            nonlocal watcher_task
            try:
                task.result()
            except asyncio.CancelledError:
                LOGGER.info("Background domain watcher cancelled")
            except Exception:
                LOGGER.exception("Background domain watcher crashed")
                watcher_task = asyncio.create_task(_restart_watcher())

        watcher_task = _start_watcher()
        LOGGER.info("Background domain watcher started")

    async def post_shutdown(_application):
        if watcher_task:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
            LOGGER.info("Background domain watcher stopped")

    app.post_init = post_init
    app.post_shutdown = post_shutdown
    app.run_polling()


if __name__ == "__main__":
    main()
