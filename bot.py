import os
import asyncio
import logging
import hashlib
import tempfile
import subprocess
import json
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatMember,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.environ["BOT_TOKEN"]
FORCE_CHANNEL = os.environ.get("FORCE_CHANNEL", "")   # e.g. "@mychannel"
WEBHOOK_URL   = os.environ.get("WEBHOOK_URL", "")     # e.g. "https://myapp.herokuapp.com"
PORT          = int(os.environ.get("PORT", 8443))
MAX_FILE_SIZE = 50 * 1024 * 1024                       # 50 MB Telegram limit

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── In-memory session store ───────────────────────────────────────────────────
# url_hash → {"url": str, "title": str, "formats": list}
pending: dict = {}


# ── Subscription Check ────────────────────────────────────────────────────────
async def is_subscribed(bot, user_id: int) -> bool:
    if not FORCE_CHANNEL:
        return True
    try:
        member = await bot.get_chat_member(chat_id=FORCE_CHANNEL, user_id=user_id)
        return member.status in (
            ChatMember.MEMBER,
            ChatMember.ADMINISTRATOR,
            ChatMember.OWNER,
        )
    except Exception:
        return False


# ── Keyboards ─────────────────────────────────────────────────────────────────
def kb_join() -> InlineKeyboardMarkup:
    channel = FORCE_CHANNEL.lstrip("@")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢  Join Our Channel", url=f"https://t.me/{channel}")],
        [InlineKeyboardButton("✅  I've Joined — Let Me In!", callback_data="check_sub")],
    ])


def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎥  Download a Video", callback_data="howto")],
        [
            InlineKeyboardButton("❓  Help", callback_data="help"),
            InlineKeyboardButton("ℹ️  About", callback_data="about"),
        ],
    ])


def kb_back_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠  Home", callback_data="home")],
    ])


def kb_qualities(url_hash: str, formats: list) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f["label"], callback_data=f"dl:{url_hash}:{f['id']}")]
        for f in formats
    ]
    rows.append([InlineKeyboardButton("❌  Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


# ── Text blocks ───────────────────────────────────────────────────────────────
WELCOME_LOCKED = (
    "👋 *Welcome to YT Downloader Bot!*\n\n"
    "🔒 This bot is exclusive to our community members.\n"
    "📢 Please join our channel to unlock full access.\n\n"
    "👇 Tap the button below, then come back and click *I've Joined*."
)

HOME_TEXT = (
    "🏠 *Home Menu*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🎬 *YouTube Video Downloader*\n\n"
    "Simply paste any YouTube link and I'll show you all available qualities "
    "so you can pick exactly what you want.\n\n"
    "📌 *Supported links:*\n"
    "  • `youtube.com/watch?v=…`\n"
    "  • `youtu.be/…`\n"
    "  • `m.youtube.com/…`\n\n"
    "⚡ Fast  •  🔒 Private  •  🎯 Your Quality"
)

HOWTO_TEXT = (
    "🎥 *How to Download a Video*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "1️⃣  Copy a YouTube video link\n"
    "2️⃣  Paste it directly in this chat\n"
    "3️⃣  Pick your preferred quality\n"
    "4️⃣  Sit back — the file comes straight to you!\n\n"
    "📦 *Available formats:*  MP4 (video) · MP3 (audio only)\n"
    "⚠️ *Max file size:*  50 MB (Telegram limit)"
)

HELP_TEXT = (
    "❓ *Help & FAQ*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🔹 *What can this bot do?*\n"
    "Download YouTube videos in various resolutions.\n\n"
    "🔹 *Why does it say 'file too large'?*\n"
    "Telegram limits uploads to 50 MB. Try a lower quality.\n\n"
    "🔹 *Download is slow — why?*\n"
    "It depends on video size and server load. Please wait.\n\n"
    "🔹 *What links are supported?*\n"
    "YouTube only — `youtube.com` and `youtu.be`.\n\n"
    "🔹 *Is my data stored?*\n"
    "No. Nothing is saved after the download is complete."
)

ABOUT_TEXT = (
    "ℹ️ *About This Bot*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🤖  *YT Downloader Bot*\n"
    "⚡  Powered by `yt-dlp`\n"
    "🐍  Built with `python-telegram-bot`\n"
    "🔒  Privacy-first — zero data retention\n"
    "🌐  Language: English\n\n"
    "📌  *Version:* 1.0.0"
)


# ── yt-dlp helpers ────────────────────────────────────────────────────────────
QUALITY_EMOJI = {1080: "🎬", 720: "📺", 480: "📱", 360: "📱", 240: "🔅", 144: "🔅"}
HD_LABEL      = {1080: " Full HD", 720: " HD", 480: "", 360: "", 240: "", 144: ""}


def _fetch_formats(url: str):
    """Blocking call — run via executor."""
    try:
        res = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-playlist", url],
            capture_output=True, text=True, timeout=45,
        )
        if res.returncode != 0:
            return None, []
        info = json.loads(res.stdout)
        title = info.get("title", "Unknown Video")

        seen_heights: dict = {}
        for fmt in info.get("formats", []):
            h = fmt.get("height")
            if not h or fmt.get("vcodec", "none") == "none":
                continue
            if h not in seen_heights:
                seen_heights[h] = {
                    "id": fmt["format_id"] + "+bestaudio/best",
                    "height": h,
                    "filesize": fmt.get("filesize") or fmt.get("filesize_approx"),
                }

        formats = []
        for h, data in sorted(seen_heights.items(), reverse=True)[:6]:
            emoji = QUALITY_EMOJI.get(h, "🎥")
            tag   = HD_LABEL.get(h, "")
            size  = f"  (~{data['filesize'] / 1048576:.0f} MB)" if data["filesize"] else ""
            formats.append({
                "id":    data["id"],
                "label": f"{emoji}  {h}p{tag}{size}",
                "height": h,
            })

        formats.append({
            "id":    "bestaudio/best",
            "label": "🎵  Audio Only  (MP3)",
            "height": 0,
        })
        return title, formats

    except subprocess.TimeoutExpired:
        return None, []
    except Exception as e:
        logger.error("fetch_formats error: %s", e)
        return None, []


def _download_file(url: str, format_id: str, out_dir: str, is_audio: bool) -> str | None:
    """Blocking call — run via executor. Returns file path or None."""
    tpl = os.path.join(out_dir, "%(title).60s.%(ext)s")
    if is_audio:
        cmd = [
            "yt-dlp", "-f", format_id,
            "--extract-audio", "--audio-format", "mp3", "--audio-quality", "0",
            "-o", tpl, "--no-playlist", url,
        ]
    else:
        cmd = [
            "yt-dlp", "-f", format_id,
            "--merge-output-format", "mp4",
            "-o", tpl, "--no-playlist", url,
        ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if res.returncode != 0:
            logger.error("yt-dlp stderr: %s", res.stderr[:500])
            return None
        files = os.listdir(out_dir)
        return os.path.join(out_dir, files[0]) if files else None
    except subprocess.TimeoutExpired:
        return None
    except Exception as e:
        logger.error("download_file error: %s", e)
        return None


# ── Shared helpers ────────────────────────────────────────────────────────────
async def send_home(target, user):
    text = HOME_TEXT.replace("{name}", user.first_name)
    if hasattr(target, "message") and target.message:
        await target.message.reply_text(text, parse_mode="Markdown", reply_markup=kb_home())
    else:
        await target.edit_message_text(text, parse_mode="Markdown", reply_markup=kb_home())


# ── Handlers ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await is_subscribed(ctx.bot, user.id):
        await update.message.reply_text(WELCOME_LOCKED, parse_mode="Markdown", reply_markup=kb_join())
        return
    await send_home(update, user)


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    user = update.effective_user
    await q.answer()
    data = q.data

    if data == "check_sub":
        if await is_subscribed(ctx.bot, user.id):
            await send_home(q, user)
        else:
            await q.answer("⚠️  You haven't joined yet! Please join first.", show_alert=True)

    elif data == "home":
        await send_home(q, user)

    elif data == "howto":
        await q.edit_message_text(HOWTO_TEXT, parse_mode="Markdown", reply_markup=kb_back_home())

    elif data == "help":
        await q.edit_message_text(HELP_TEXT, parse_mode="Markdown", reply_markup=kb_back_home())

    elif data == "about":
        await q.edit_message_text(ABOUT_TEXT, parse_mode="Markdown", reply_markup=kb_back_home())

    elif data == "cancel":
        await q.edit_message_text(
            "❌ *Cancelled.*\n\nSend a YouTube link anytime to start over.",
            parse_mode="Markdown",
        )

    elif data.startswith("dl:"):
        parts = data.split(":", 2)
        if len(parts) == 3:
            await do_download(update, ctx, url_hash=parts[1], format_id=parts[2])


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not (update.message and update.message.text):
        return
    text = update.message.text.strip()
    user = update.effective_user

    if not any(d in text.lower() for d in ("youtube.com", "youtu.be")):
        if not await is_subscribed(ctx.bot, user.id):
            await update.message.reply_text(WELCOME_LOCKED, parse_mode="Markdown", reply_markup=kb_join())
            return
        await update.message.reply_text(
            "🔗 *Send a YouTube link to download a video!*\n\n"
            "Example: `https://youtube.com/watch?v=dQw4w9WgXcQ`",
            parse_mode="Markdown",
            reply_markup=kb_home(),
        )
        return

    if not await is_subscribed(ctx.bot, user.id):
        await update.message.reply_text(WELCOME_LOCKED, parse_mode="Markdown", reply_markup=kb_join())
        return

    await handle_youtube_url(update, ctx, text)


async def handle_youtube_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url: str):
    status = await update.message.reply_text(
        "🔍 *Fetching video info…*\nHang tight ⏳", parse_mode="Markdown"
    )

    loop = asyncio.get_event_loop()
    title, formats = await loop.run_in_executor(None, _fetch_formats, url)

    if not formats:
        await status.edit_text(
            "❌ *Could not fetch video info!*\n\n"
            "• Make sure it's a valid YouTube link\n"
            "• The video must be publicly available\n"
            "• Try again in a few seconds",
            parse_mode="Markdown",
        )
        return

    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    pending[url_hash] = {"url": url, "title": title, "formats": formats}

    short_title = title[:60] + ("…" if len(title) > 60 else "")
    await status.edit_text(
        f"🎬 *{short_title}*\n\n"
        "📊 *Choose your quality:*\n"
        "_(Tap a button to begin downloading)_",
        parse_mode="Markdown",
        reply_markup=kb_qualities(url_hash, formats),
    )


async def do_download(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url_hash: str, format_id: str):
    q    = update.callback_query
    user = update.effective_user

    if url_hash not in pending:
        await q.edit_message_text(
            "⚠️ *Session expired.*\nPlease send the link again.", parse_mode="Markdown"
        )
        return

    session   = pending[url_hash]
    url       = session["url"]
    formats   = session["formats"]
    is_audio  = format_id == "bestaudio/best"
    fmt_label = next((f["label"] for f in formats if f["id"] == format_id), format_id)

    await q.edit_message_text(
        f"⬇️ *Downloading…*\n\n"
        f"📌 Quality: {fmt_label}\n"
        f"⏳ This may take a moment — please wait…",
        parse_mode="Markdown",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        loop     = asyncio.get_event_loop()
        filepath = await loop.run_in_executor(
            None, _download_file, url, format_id, tmpdir, is_audio
        )

        if not filepath:
            await q.edit_message_text(
                "❌ *Download failed!*\n\n"
                "Try a different quality or resend the link.",
                parse_mode="Markdown",
            )
            return

        filesize = os.path.getsize(filepath)
        if filesize > MAX_FILE_SIZE:
            await q.edit_message_text(
                f"⚠️ *File too large!*\n\n"
                f"📦 Size: {filesize / 1048576:.1f} MB\n"
                f"📏 Telegram limit: 50 MB\n\n"
                "Please choose a *lower quality*.",
                parse_mode="Markdown",
            )
            return

        await q.edit_message_text("📤 *Uploading…*\nAlmost there! 🚀", parse_mode="Markdown")

        caption = (
            f"✅ *Download Complete!*\n\n"
            f"📌 Quality: {fmt_label}\n"
            f"📦 Size: {filesize / 1048576:.1f} MB"
        )

        with open(filepath, "rb") as fh:
            if is_audio:
                await ctx.bot.send_audio(
                    chat_id=user.id, audio=fh,
                    caption=caption, parse_mode="Markdown",
                )
            else:
                await ctx.bot.send_video(
                    chat_id=user.id, video=fh,
                    caption=caption, parse_mode="Markdown",
                    supports_streaming=True,
                )

        await q.delete_message()


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    if WEBHOOK_URL:
        logger.info("Starting in WEBHOOK mode on port %s", PORT)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        )
    else:
        logger.info("Starting in POLLING mode")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
