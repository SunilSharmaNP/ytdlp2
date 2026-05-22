"""
YT-DLP Downloader Bot  v4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━
• All 1500+ yt-dlp sites supported
• Live download + upload progress bar with Cancel button
• 2 force-subscribe channels
• Custom user thumbnail (stored in MongoDB)
• MongoDB for user tracking & persistence
• YouTube cookies hot-reload without restart
• Admin broadcast (text / photo / video / document) to all users
• Commands auto-registered on every startup
• Up to 2 GB uploads via Pyrogram MTProto
"""

import asyncio
import base64
import hashlib
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.errors import ChatAdminRequired, UserNotParticipant
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import config

# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
#  PYROGRAM CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
app = Client(
    ":memory:",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    workers=16,
)

# ═══════════════════════════════════════════════════════════════════════════════
#  MONGODB
# ═══════════════════════════════════════════════════════════════════════════════
_mongo_client = AsyncIOMotorClient(config.MONGO_URI) if config.MONGO_URI else None
_db           = _mongo_client[config.DB_NAME] if _mongo_client is not None else None
_users        = _db["users"] if _db is not None else None


async def db_upsert_user(user_id: int, first_name: str, username: str | None):
    if not _users:
        return
    try:
        await _users.update_one(
            {"_id": user_id},
            {
                "$setOnInsert": {"joined_at": datetime.now(timezone.utc), "custom_thumb": None},
                "$set": {"first_name": first_name, "username": username},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error("db_upsert_user: %s", e)


async def db_get_thumb(user_id: int) -> str | None:
    if not _users:
        return None
    try:
        doc = await _users.find_one({"_id": user_id}, {"custom_thumb": 1})
        return doc.get("custom_thumb") if doc else None
    except Exception:
        return None


async def db_set_thumb(user_id: int, file_id: str | None):
    if not _users:
        return
    try:
        await _users.update_one(
            {"_id": user_id},
            {"$set": {"custom_thumb": file_id}},
            upsert=True,
        )
    except Exception as e:
        logger.error("db_set_thumb: %s", e)


async def db_count_users() -> int:
    if not _users:
        return 0
    try:
        return await _users.count_documents({})
    except Exception:
        return 0


async def db_iter_user_ids():
    """Async generator — yields user _id one by one."""
    if not _users:
        return
    try:
        async for doc in _users.find({}, {"_id": 1}):
            yield doc["_id"]
    except Exception as e:
        logger.error("db_iter_user_ids: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  COOKIES SETUP
# ═══════════════════════════════════════════════════════════════════════════════
_RUNTIME_COOKIES = Path("runtime_cookies.txt")
_cookies_path: Path | None = None


def _init_cookies() -> Path | None:
    global _cookies_path
    if config.COOKIES_B64:
        try:
            _RUNTIME_COOKIES.write_bytes(base64.b64decode(config.COOKIES_B64))
            logger.info("Cookies loaded from COOKIES_B64.")
            return _RUNTIME_COOKIES
        except Exception as e:
            logger.error("COOKIES_B64 decode failed: %s", e)
    p = Path(config.COOKIES_FILE)
    if p.exists():
        logger.info("Cookies loaded from file: %s", p)
        return p
    logger.info("No cookies — public content only.")
    return None


_cookies_path = _init_cookies()

# ═══════════════════════════════════════════════════════════════════════════════
#  IN-MEMORY STATE
# ═══════════════════════════════════════════════════════════════════════════════
# url_hash → {"url", "title", "thumb", "formats": list[dict]}
pending: dict[str, dict] = {}

# url_hash → asyncio.subprocess.Process  (for cancellation)
active_procs: dict[str, asyncio.subprocess.Process] = {}

# url_hash → asyncio.Event  (cancel signal)
cancel_events: dict[str, asyncio.Event] = {}

# user_ids currently waiting to send their thumbnail photo
thumb_waiting: set[int] = set()

# admin_id → True   (waiting for the message to broadcast)
broadcast_waiting: set[int] = set()

# admin_id → pyrogram.Message  (message captured, waiting for confirm/cancel)
broadcast_pending: dict[int, object] = {}

# ═══════════════════════════════════════════════════════════════════════════════
#  PROGRESS REGEX  (yt-dlp --newline output)
# ═══════════════════════════════════════════════════════════════════════════════
_PROG_RE = re.compile(
    r"\[download\]\s+([\d.]+)%\s+of\s+~?([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)\s+ETA\s+([\d:]+)"
)
_FRAG_RE = re.compile(r"\(frag\s+(\d+)/(\d+)\)")


# ═══════════════════════════════════════════════════════════════════════════════
#  SUBSCRIPTION CHECK  (2 channels)
# ═══════════════════════════════════════════════════════════════════════════════
async def _check_one(user_id: int, channel: str) -> bool:
    if not channel:
        return True
    try:
        m = await app.get_chat_member(channel, user_id)
        return m.status.value in ("member", "administrator", "owner", "creator")
    except (UserNotParticipant, ChatAdminRequired):
        return False
    except Exception:
        return False


async def check_sub(user_id: int) -> dict:
    """Returns {"ch1": bool, "ch2": bool, "ok": bool}"""
    c1 = await _check_one(user_id, config.FORCE_CHANNEL_1)
    c2 = await _check_one(user_id, config.FORCE_CHANNEL_2)
    return {"ch1": c1, "ch2": c2, "ok": c1 and c2}


# ═══════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
def _ch_url(channel: str, override: str) -> str:
    if override:
        return override
    return f"https://t.me/{channel.lstrip('@')}"


def kb_join(sub: dict) -> InlineKeyboardMarkup:
    rows = []
    if not sub["ch1"] and config.FORCE_CHANNEL_1:
        rows.append([InlineKeyboardButton(
            f"📢  Join {config.FORCE_CHANNEL_1_NAME}",
            url=_ch_url(config.FORCE_CHANNEL_1, config.FORCE_CHANNEL_1_URL),
        )])
    if not sub["ch2"] and config.FORCE_CHANNEL_2:
        rows.append([InlineKeyboardButton(
            f"📢  Join {config.FORCE_CHANNEL_2_NAME}",
            url=_ch_url(config.FORCE_CHANNEL_2, config.FORCE_CHANNEL_2_URL),
        )])
    rows.append([InlineKeyboardButton("✅  I've Joined Both!", callback_data="check_sub")])
    return InlineKeyboardMarkup(rows)


def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬇️  Download Media", callback_data="howto")],
        [
            InlineKeyboardButton("🖼️  My Thumbnail",  callback_data="thumb_menu"),
            InlineKeyboardButton("❓  Help",           callback_data="help"),
        ],
        [InlineKeyboardButton("ℹ️  About",            callback_data="about")],
    ])


def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠  Home", callback_data="home")]])


def kb_thumb_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🖼️  Set My Thumbnail",    callback_data="set_thumb")],
        [InlineKeyboardButton("🗑️  Remove My Thumbnail", callback_data="del_thumb")],
        [InlineKeyboardButton("🏠  Home",               callback_data="home")],
    ])


def kb_qualities(url_hash: str, formats: list) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f["label"], callback_data=f"dl:{url_hash}:{i}")]
        for i, f in enumerate(formats)
    ]
    rows.append([InlineKeyboardButton("❌  Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def kb_cancel(url_hash: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑  Cancel Download", callback_data=f"cancel_dl:{url_hash}")]
    ])


# ═══════════════════════════════════════════════════════════════════════════════
#  MENU CAPTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def cap_welcome(name: str) -> str:
    lines = [
        f"👋 *Welcome, {name}!*\n",
        "🔒 *Members-only bot.*\n",
        "You need to join *both* channels below to unlock access.\n",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if config.FORCE_CHANNEL_1:
        lines.append(f"  📡 *{config.FORCE_CHANNEL_1_NAME}*")
    if config.FORCE_CHANNEL_2:
        lines.append(f"  🏷️ *{config.FORCE_CHANNEL_2_NAME}*")
    lines += [
        "━━━━━━━━━━━━━━━━━━━━",
        "\n🎬 *What you'll get after joining:*",
        "  • Download from YouTube, Instagram, TikTok, Twitter & 1500+ sites",
        "  • Pick any quality — 144p up to 4K",
        "  • MP4 video or MP3 / AAC / FLAC audio",
        "  • Live progress bar while downloading",
        "  • Custom thumbnail on your downloads",
        "  • Up to **2 GB** per file",
    ]
    return "\n".join(lines)


def cap_home(name: str) -> str:
    return (
        f"🏠 *Home — Hello, {name}!*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌐 *Universal Media Downloader*\n\n"
        "Send me any video link — YouTube, Instagram, TikTok, Twitter,\n"
        "Facebook, Reddit, Vimeo and **1500+ other sites**.\n\n"
        "I'll fetch all available qualities so you can pick exactly what you want.\n\n"
        f"📦 *Max file:*  {config.MAX_FILE_SIZE // (1024**3)} GB  "
        "⚡ Fast  🔒 Private  🎯 Your Choice"
    )


CAP_HOWTO = (
    "⬇️ *How to Download*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "1️⃣  Copy any video/audio link\n"
    "2️⃣  Paste it in this chat\n"
    "3️⃣  Choose your quality from the buttons\n"
    "4️⃣  Watch live progress, then receive your file 🚀\n\n"
    "🌐 *Supported sites:*\n"
    "YouTube · Instagram · TikTok · Twitter/X · Facebook\n"
    "Reddit · Vimeo · Dailymotion · SoundCloud · and 1500+ more\n\n"
    "📦 *Video:*  MP4  |  🎵 *Audio:*  MP3 · AAC · FLAC · Opus · WAV\n"
    "📏 *Resolutions:*  144p  →  4K (2160p)\n\n"
    "🖼️ *Custom thumbnail:*  Set your own thumb via *My Thumbnail* in the menu"
)

CAP_HELP = (
    "❓ *Help & FAQ*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🔹 *What sites are supported?*\n"
    "1500+ platforms — YouTube, Instagram, TikTok, Twitter, Facebook, etc.\n\n"
    "🔹 *Why is a quality missing?*\n"
    "Not all platforms offer all resolutions.\n\n"
    "🔹 *Age-restricted / private videos?*\n"
    "Possible if the admin has set cookies.\n\n"
    "🔹 *How to set my own thumbnail?*\n"
    "Tap *My Thumbnail* in the home menu, then *Set My Thumbnail* and send a photo.\n\n"
    "🔹 *Can I cancel a download?*\n"
    "Yes! Tap the *Cancel* button that appears during download.\n\n"
    "🔹 *Max file size?*\n"
    f"{config.MAX_FILE_SIZE // (1024**3)} GB — Pyrogram MTProto handles it.\n\n"
    "🔹 *Is my data stored?*\n"
    "Only your custom thumbnail file_id. No video data is saved."
)

CAP_ABOUT = (
    "ℹ️ *About This Bot*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🤖  *Universal Downloader Bot*\n"
    "🌐  Powered by `yt-dlp` — 1500+ sites\n"
    "🐍  Built with `Pyrogram` (MTProto)\n"
    f"📦  Supports up to *{config.MAX_FILE_SIZE // (1024**3)} GB*\n"
    "🍪  Cookie-aware for restricted content\n"
    "🗄️  MongoDB for user data\n"
    "🖼️  Custom thumbnails per user\n"
    "🔒  Privacy-first — zero video data stored\n"
    "🌐  Language: English\n\n"
    "📌  *Version:* 3.0.0\n"
    "_Made with ❤️ for fast, quality downloads._"
)

CAP_THUMB_MENU = (
    "🖼️ *My Thumbnail*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "Set a custom photo that will appear as the thumbnail on *every video* you download.\n\n"
    "✅ *Set:* Tap *Set My Thumbnail* → send any photo\n"
    "🗑️ *Remove:* Tap *Remove My Thumbnail* to go back to default\n\n"
    "💡 Best size: 1280×720 (landscape, JPG/PNG)"
)


# ═══════════════════════════════════════════════════════════════════════════════
#  MENU SENDER
# ═══════════════════════════════════════════════════════════════════════════════
async def send_menu(
    target,
    *,
    chat_id: int,
    thumb: str | None,
    text: str,
    keyboard: InlineKeyboardMarkup,
    is_edit: bool = False,
):
    if is_edit:
        if thumb:
            await target.edit_message_caption(caption=text, reply_markup=keyboard)
        else:
            await target.edit_message_text(text, reply_markup=keyboard)
    else:
        if thumb:
            await app.send_photo(chat_id, photo=thumb, caption=text, reply_markup=keyboard)
        else:
            await app.send_message(chat_id, text, reply_markup=keyboard)


# ═══════════════════════════════════════════════════════════════════════════════
#  URL VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════
_URL_RE = re.compile(r"https?://[^\s<>\"{}|\\^`\[\]]+")


def extract_url(text: str) -> str | None:
    m = _URL_RE.search(text)
    return m.group(0) if m else None


# ═══════════════════════════════════════════════════════════════════════════════
#  READABLE SIZES
# ═══════════════════════════════════════════════════════════════════════════════
def readable_size(b: int | float | None) -> str:
    if not b:
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"  (~{b:.0f} {unit})"
        b /= 1024
    return f"  (~{b:.2f} TB)"


def readable_bytes(b: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.2f} TB"


# ═══════════════════════════════════════════════════════════════════════════════
#  FORMAT FETCHING  (yt-dlp Python API, run in executor)
# ═══════════════════════════════════════════════════════════════════════════════
def _build_ydl_opts() -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": config.INFO_TIMEOUT,
    }
    if _cookies_path and _cookies_path.exists():
        opts["cookiefile"] = str(_cookies_path)
    return opts


def _fetch_formats_sync(url: str):
    """Blocking. Returns (title, thumbnail_url, formats_list)."""
    import yt_dlp

    try:
        with yt_dlp.YoutubeDL(_build_ydl_opts()) as ydl:
            info = ydl.extract_info(url, download=False)
            if info is None:
                return None, None, []
    except Exception as e:
        logger.warning("_fetch_formats_sync error: %s", e)
        return None, None, []

    title     = info.get("title", "Unknown")
    thumbnail = info.get("thumbnail")

    # ── Video formats ─────────────────────────────────────────────────────────
    seen: dict[int, dict] = {}
    for fmt in info.get("formats", []):
        h      = fmt.get("height")
        vcodec = fmt.get("vcodec", "none")
        if not h or vcodec in ("none", None, ""):
            continue
        fs = fmt.get("filesize") or fmt.get("filesize_approx") or 0
        if h not in seen or fs > seen[h].get("filesize", 0):
            seen[h] = {
                "format_id": fmt["format_id"],
                "ext":       fmt.get("ext", "mp4"),
                "filesize":  fs,
                "fps":       fmt.get("fps") or "",
                "height":    h,
            }

    formats: list[dict] = []
    for h, d in sorted(seen.items(), reverse=True):
        emoji    = config.QUALITY_EMOJI.get(h, "🎥")
        tag      = config.HD_LABEL.get(h, "")
        fps_str  = f" {d['fps']}fps" if d["fps"] else ""
        size_str = readable_size(d["filesize"])
        formats.append({
            "label":       f"{emoji}  {h}p{tag}{fps_str}{size_str}",
            "ytdl_fmt":    f"{d['format_id']}+bestaudio[ext=m4a]/+bestaudio",
            "is_audio":    False,
            "height":      h,
        })

    # ── Audio formats ─────────────────────────────────────────────────────────
    for label, aud_fmt, aud_qual in [
        ("🎵  MP3  320 Kbps",   "mp3",   "0"),
        ("🎵  MP3  128 Kbps",   "mp3",   "5"),
        ("🔊  AAC",             "aac",   "0"),
        ("🔊  FLAC  (lossless)","flac",  "0"),
        ("🔊  Opus",            "opus",  "0"),
        ("🔊  WAV",             "wav",   "0"),
    ]:
        formats.append({
            "label":       label,
            "ytdl_fmt":    "bestaudio/best",
            "is_audio":    True,
            "audio_fmt":   aud_fmt,
            "audio_qual":  aud_qual,
        })

    return title, thumbnail, formats


# ═══════════════════════════════════════════════════════════════════════════════
#  YT-DLP COMMAND BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
def _build_cmd(url: str, fmt: dict, out_dir: str) -> list[str]:
    tpl = os.path.join(out_dir, "%(title).80s.%(ext)s")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--progress",
        "--newline",
        "--retries", "5",
        "--fragment-retries", "5",
        "--http-chunk-size", "1048576",
        "-o", tpl,
    ]
    if _cookies_path and _cookies_path.exists():
        cmd += ["--cookies", str(_cookies_path)]

    if fmt["is_audio"]:
        cmd += [
            "-f", fmt["ytdl_fmt"],
            "-x",
            "--audio-format", fmt["audio_fmt"],
            "--audio-quality", fmt["audio_qual"],
        ]
    else:
        cmd += [
            "-f", fmt["ytdl_fmt"],
            "--merge-output-format", "mp4",
        ]

    cmd.append(url)
    return cmd


# ═══════════════════════════════════════════════════════════════════════════════
#  PROGRESS BAR FORMATTER
# ═══════════════════════════════════════════════════════════════════════════════
def _progress_text(
    fmt_label: str,
    url_hash: str,
    percent: str = "0%",
    speed: str = "N/A",
    eta: str = "N/A",
    size: str = "?",
    phase: str = "📥 Downloading",
) -> str:
    try:
        pct_val = float(percent.rstrip("%"))
    except ValueError:
        pct_val = 0.0
    filled = min(int(pct_val / 10), 10)
    bar    = "█" * filled + "░" * (10 - filled)
    return (
        f"{phase}…\n\n"
        f"📌 *{fmt_label}*\n\n"
        f"`[{bar}] {percent}`\n\n"
        f"📦 Size : `{size}`\n"
        f"⚡ Speed: `{speed}`\n"
        f"⏱ ETA  : `{eta}`"
    )


def _upload_text(fmt_label: str, current: int, total: int) -> str:
    pct_val = (current / total * 100) if total else 0
    filled  = min(int(pct_val / 10), 10)
    bar     = "█" * filled + "░" * (10 - filled)
    return (
        f"📤 *Uploading…*\n\n"
        f"📌 *{fmt_label}*\n\n"
        f"`[{bar}] {pct_val:.1f}%`\n\n"
        f"`{readable_bytes(current)} / {readable_bytes(total)}`"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  ASYNC DOWNLOAD WITH LIVE PROGRESS + CANCEL
# ═══════════════════════════════════════════════════════════════════════════════
async def _run_download(
    url: str,
    fmt: dict,
    out_dir: str,
    url_hash: str,
    cancel_ev: asyncio.Event,
    status_msg,
) -> tuple[str | None, str]:
    """
    Runs yt-dlp as an async subprocess, reads stdout line-by-line,
    updates `status_msg` with a live progress bar, and respects `cancel_ev`.
    Returns (filepath, "done" | "cancelled" | "error").
    """
    cmd = _build_cmd(url, fmt, out_dir)
    fmt_label = fmt["label"]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as e:
        logger.error("subprocess start error: %s", e)
        return None, "error"

    active_procs[url_hash] = proc

    percent = "0%"
    speed   = "N/A"
    eta     = "N/A"
    size    = "?"
    phase   = "📥 Downloading"
    last_edit = 0.0

    async def _update():
        nonlocal last_edit
        now = asyncio.get_event_loop().time()
        if now - last_edit < 4:
            return
        last_edit = now
        text = _progress_text(fmt_label, url_hash, percent, speed, eta, size, phase)
        try:
            await status_msg.edit_text(text, reply_markup=kb_cancel(url_hash))
        except Exception:
            pass

    # Read stdout until EOF or cancel
    try:
        async for raw in proc.stdout:
            if cancel_ev.is_set():
                proc.kill()
                await proc.wait()
                active_procs.pop(url_hash, None)
                return None, "cancelled"

            line = raw.decode("utf-8", errors="ignore").strip()

            m = _PROG_RE.match(line)
            if m:
                percent = m.group(1).strip() + "%"
                size    = m.group(2).strip()
                speed   = m.group(3).strip()
                eta     = m.group(4).strip()
                phase   = "📥 Downloading"
                await _update()
            elif "[Merger]" in line:
                phase   = "🔀 Merging"
                percent = "100%"
                eta     = "0:00"
                await _update()
            elif "[ExtractAudio]" in line:
                phase   = "🎵 Extracting Audio"
                percent = "100%"
                eta     = "0:00"
                await _update()
            elif "[ffmpeg]" in line:
                phase   = "⚙️ Processing"
                await _update()

    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        return None, "cancelled"

    await proc.wait()
    active_procs.pop(url_hash, None)

    if cancel_ev.is_set():
        return None, "cancelled"
    if proc.returncode != 0:
        return None, "error"

    # Find downloaded file (skip temp/partial files)
    files = [
        f for f in os.listdir(out_dir)
        if not f.endswith((".part", ".ytdl", ".tmp"))
    ]
    if not files:
        return None, "error"

    return os.path.join(out_dir, files[0]), "done"


# ═══════════════════════════════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    user = msg.from_user
    await db_upsert_user(user.id, user.first_name, user.username)
    sub = await check_sub(user.id)
    if not sub["ok"]:
        await send_menu(
            None, chat_id=user.id,
            thumb=config.THUMB_WELCOME,
            text=cap_welcome(user.first_name),
            keyboard=kb_join(sub),
        )
        return
    await send_menu(
        None, chat_id=user.id,
        thumb=config.THUMB_HOME,
        text=cap_home(user.first_name),
        keyboard=kb_home(),
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  /setthumb  /delthumb
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("setthumb") & filters.private)
async def cmd_setthumb(_, msg: Message):
    user = msg.from_user
    sub  = await check_sub(user.id)
    if not sub["ok"]:
        return
    thumb_waiting.add(user.id)
    await msg.reply(
        "🖼️ *Set Your Thumbnail*\n\n"
        "Now send me the photo you want to use as your download thumbnail.\n\n"
        "_Send /cancel to abort._"
    )


@app.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(_, msg: Message):
    await db_set_thumb(msg.from_user.id, None)
    await msg.reply("🗑️ *Thumbnail removed.* Your downloads will use the default thumbnail.")


# ═══════════════════════════════════════════════════════════════════════════════
#  /setcookies  /delcookies  (admin only)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("setcookies") & filters.private)
async def cmd_setcookies(_, msg: Message):
    global _cookies_path
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.reply("⛔ *Admin only.*")
        return
    reply = msg.reply_to_message
    if not (reply and reply.document):
        await msg.reply(
            "📎 *How to set cookies:*\n\n"
            "1. Export `cookies.txt` using the *Get cookies.txt LOCALLY* browser extension\n"
            "2. Send the file in this chat, then reply to it with `/setcookies`"
        )
        return
    info = await msg.reply("⏳ Downloading cookies file…")
    fp   = await reply.download("cookies.txt")
    _cookies_path = Path(fp)
    logger.info("Cookies hot-reloaded from admin upload: %s", fp)
    await info.edit_text(
        "✅ *Cookies updated & active immediately!*\n\n"
        "🍪 New cookies are already in use — **no restart needed**.\n"
        "All future downloads will use the new cookies file.\n\n"
        f"📄 Saved as: `{fp}`\n"
        "🗑 Use /delcookies to remove them."
    )


@app.on_message(filters.command("delcookies") & filters.private)
async def cmd_delcookies(_, msg: Message):
    global _cookies_path
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.reply("⛔ *Admin only.*")
        return
    _cookies_path = None
    for p in (Path("cookies.txt"), _RUNTIME_COOKIES):
        if p.exists():
            p.unlink()
    await msg.reply("✅ *Cookies removed.* Bot will only access public content.")


# ═══════════════════════════════════════════════════════════════════════════════
#  /broadcast  (admin only)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("broadcast") & filters.private)
async def cmd_broadcast(_, msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.reply("⛔ *Admin only.*")
        return
    total = await db_count_users()
    broadcast_waiting.add(msg.from_user.id)
    await msg.reply(
        f"📣 *Broadcast Mode*\n\n"
        f"👥 Total users: *{total}*\n\n"
        "Send me the message you want to broadcast.\n"
        "Supported: text, photo with caption, video with caption, document.\n\n"
        "_Send /cancel to abort._"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  /cancel
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(_, msg: Message):
    user = msg.from_user
    thumb_waiting.discard(user.id)
    broadcast_waiting.discard(user.id)
    broadcast_pending.pop(user.id, None)
    await msg.reply("❌ Cancelled.")


# ═══════════════════════════════════════════════════════════════════════════════
#  GENERAL MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.private & ~filters.command(""))
async def on_message(_, msg: Message):
    user = msg.from_user

    # ── Broadcast message capture (admin) ────────────────────────────────────
    if user.id in broadcast_waiting:
        broadcast_waiting.discard(user.id)
        broadcast_pending[user.id] = msg
        total = await db_count_users()
        await msg.reply(
            f"📣 *Ready to Broadcast*\n\n"
            f"👥 This will be sent to *{total}* users.\n\n"
            "⚠️ Are you sure you want to send this message to everyone?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Send Now", callback_data="bc_confirm"),
                    InlineKeyboardButton("❌ Cancel",        callback_data="bc_cancel"),
                ]
            ]),
        )
        return

    # ── Thumbnail photo waiting ───────────────────────────────────────────────
    if msg.photo and user.id in thumb_waiting:
        thumb_waiting.discard(user.id)
        file_id = msg.photo.file_id
        await db_set_thumb(user.id, file_id)
        await msg.reply(
            "✅ *Thumbnail saved!*\n\n"
            "It will appear on all your future video downloads.\n"
            "_Use /delthumb to remove it._"
        )
        return

    if not msg.text:
        return

    text = msg.text.strip()

    # ── Subscription gate ─────────────────────────────────────────────────────
    sub = await check_sub(user.id)
    if not sub["ok"]:
        await send_menu(
            None, chat_id=user.id,
            thumb=config.THUMB_WELCOME,
            text=cap_welcome(user.first_name),
            keyboard=kb_join(sub),
        )
        return

    # ── URL detection ─────────────────────────────────────────────────────────
    url = extract_url(text)
    if not url:
        await send_menu(
            None, chat_id=user.id,
            thumb=config.THUMB_HOME,
            text=(
                "🔗 *Send a media link to download!*\n\n"
                "Supports YouTube, Instagram, TikTok, Twitter, and 1500+ sites.\n\n"
                + cap_home(user.first_name)
            ),
            keyboard=kb_home(),
        )
        return

    await handle_url(msg, user, url)


async def handle_url(msg: Message, user, url: str):
    status = await msg.reply("🔍 *Fetching media info…*\nHang tight ⏳")

    loop                  = asyncio.get_event_loop()
    title, thumb, formats = await loop.run_in_executor(None, _fetch_formats_sync, url)

    if not formats:
        await status.edit_text(
            "❌ *Could not fetch media info!*\n\n"
            "• Check that the link is valid and publicly accessible\n"
            "• Age-restricted content requires cookies (ask the admin)\n"
            "• Try again in a few seconds"
        )
        return

    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    pending[url_hash] = {
        "url":     url,
        "title":   title,
        "thumb":   thumb or config.THUMB_DEFAULT,
        "formats": formats,
    }

    short = title[:60] + ("…" if len(title) > 60 else "")
    cap   = (
        f"🎬 *{short}*\n\n"
        f"📊 *Choose your quality:*\n"
        f"_({len(formats)} options — tap to start downloading)_"
    )

    await status.delete()
    video_thumb = thumb or config.THUMB_DEFAULT
    if video_thumb:
        await app.send_photo(user.id, photo=video_thumb, caption=cap,
                             reply_markup=kb_qualities(url_hash, formats))
    else:
        await app.send_message(user.id, cap,
                               reply_markup=kb_qualities(url_hash, formats))


# ═══════════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_callback_query()
async def on_callback(_, query: CallbackQuery):
    user = query.from_user
    data = query.data
    await query.answer()

    # ── Auth gate ─────────────────────────────────────────────────────────────
    if data == "check_sub":
        sub = await check_sub(user.id)
        if sub["ok"]:
            await db_upsert_user(user.id, user.first_name, user.username)
            await send_menu(
                query, chat_id=user.id,
                thumb=config.THUMB_HOME,
                text=cap_home(user.first_name),
                keyboard=kb_home(),
                is_edit=True,
            )
        else:
            await query.answer(
                "⚠️  Please join BOTH channels first!", show_alert=True
            )
        return

    # ── Navigation ────────────────────────────────────────────────────────────
    if data == "home":
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_HOME,
                        text=cap_home(user.first_name), keyboard=kb_home(), is_edit=True)
        return

    if data == "howto":
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_HOWTO,
                        text=CAP_HOWTO, keyboard=kb_back(), is_edit=True)
        return

    if data == "help":
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_HELP,
                        text=CAP_HELP, keyboard=kb_back(), is_edit=True)
        return

    if data == "about":
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_ABOUT,
                        text=CAP_ABOUT, keyboard=kb_back(), is_edit=True)
        return

    if data == "thumb_menu":
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_HOME,
                        text=CAP_THUMB_MENU, keyboard=kb_thumb_menu(), is_edit=True)
        return

    if data == "set_thumb":
        thumb_waiting.add(user.id)
        try:
            await query.edit_message_caption(
                caption=(
                    "🖼️ *Send your thumbnail photo now.*\n\n"
                    "Any photo you send next will be saved as your thumbnail.\n"
                    "_Send /cancel to abort._"
                ),
                reply_markup=None,
            )
        except Exception:
            pass
        return

    if data == "del_thumb":
        await db_set_thumb(user.id, None)
        await query.answer("🗑️ Thumbnail removed!", show_alert=True)
        await send_menu(query, chat_id=user.id, thumb=config.THUMB_HOME,
                        text=cap_home(user.first_name), keyboard=kb_home(), is_edit=True)
        return

    # ── Broadcast confirm / cancel ────────────────────────────────────────────
    if data == "bc_cancel":
        broadcast_pending.pop(user.id, None)
        await query.answer("❌ Broadcast cancelled.", show_alert=False)
        try:
            await query.edit_message_text("❌ *Broadcast cancelled.*")
        except Exception:
            pass
        return

    if data == "bc_confirm":
        bcast_msg = broadcast_pending.pop(user.id, None)
        if not bcast_msg:
            await query.answer("Nothing to broadcast.", show_alert=True)
            return
        await query.answer("📣 Sending…", show_alert=False)
        status = await app.send_message(user.id, "📣 *Broadcasting…* 0 sent | 0 failed")
        sent = failed = 0
        async for uid in db_iter_user_ids():
            try:
                await bcast_msg.copy(uid)
                sent += 1
            except Exception:
                failed += 1
            if (sent + failed) % 25 == 0:
                try:
                    await status.edit_text(
                        f"📣 *Broadcasting…*\n✅ Sent: {sent}  |  ❌ Failed: {failed}"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.05)  # 20 msg/sec — stay under Telegram rate limit
        try:
            await status.edit_text(
                f"✅ *Broadcast Complete!*\n\n"
                f"✅ Sent: *{sent}*\n"
                f"❌ Failed/Blocked: *{failed}*\n"
                f"👥 Total: *{sent + failed}*"
            )
        except Exception:
            pass
        return

    # ── Cancel quality picker ─────────────────────────────────────────────────
    if data == "cancel":
        try:
            await query.edit_message_caption(caption="❌ *Cancelled.*\nSend a link anytime to start over.")
        except Exception:
            await query.edit_message_text("❌ *Cancelled.*\nSend a link anytime to start over.")
        return

    # ── Cancel active download ────────────────────────────────────────────────
    if data.startswith("cancel_dl:"):
        url_hash = data.split(":", 1)[1]
        ev = cancel_events.get(url_hash)
        if ev:
            ev.set()
        proc = active_procs.get(url_hash)
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        await query.answer("🛑 Cancelling…", show_alert=False)
        return

    # ── Start download ────────────────────────────────────────────────────────
    if data.startswith("dl:"):
        _, url_hash, fmt_idx = data.split(":", 2)
        await do_download(query, user, url_hash, fmt_idx)
        return


# ═══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════════
async def do_download(query: CallbackQuery, user, url_hash: str, fmt_idx: str):
    if url_hash not in pending:
        try:
            await query.edit_message_caption(caption="⚠️ *Session expired.* Please send the link again.")
        except Exception:
            await query.edit_message_text("⚠️ *Session expired.* Please send the link again.")
        return

    session = pending[url_hash]
    formats = session["formats"]

    try:
        idx = int(fmt_idx)
        fmt = formats[idx]
    except (ValueError, IndexError):
        await query.answer("❌ Invalid selection. Try again.", show_alert=True)
        return

    fmt_label = fmt["label"]

    # Initial status message (replaces the quality picker photo)
    initial_text = _progress_text(fmt_label, url_hash, "0%", "N/A", "N/A", "?", "📥 Starting")
    try:
        status_msg = await query.edit_message_caption(
            caption=initial_text,
            reply_markup=kb_cancel(url_hash),
        )
    except Exception:
        status_msg = await query.edit_message_text(
            initial_text,
            reply_markup=kb_cancel(url_hash),
        )

    cancel_ev = asyncio.Event()
    cancel_events[url_hash] = cancel_ev

    with tempfile.TemporaryDirectory() as tmpdir:
        # ── Download ──────────────────────────────────────────────────────────
        filepath, result = await _run_download(
            session["url"], fmt, tmpdir, url_hash, cancel_ev, status_msg
        )
        cancel_events.pop(url_hash, None)

        if result == "cancelled":
            try:
                await status_msg.edit_text("🛑 *Download cancelled.*\nSend the link again anytime.")
            except Exception:
                pass
            return

        if result == "error" or not filepath:
            try:
                await status_msg.edit_text(
                    "❌ *Download failed!*\n\n"
                    "Try a different quality or resend the link.\n"
                    "_(Restricted content needs cookies — ask admin)_"
                )
            except Exception:
                pass
            return

        filesize = os.path.getsize(filepath)
        if filesize > config.MAX_FILE_SIZE:
            try:
                await status_msg.edit_text(
                    f"⚠️ *File too large!*\n\n"
                    f"📦 Size: {readable_bytes(filesize)}\n"
                    f"📏 Limit: {readable_bytes(config.MAX_FILE_SIZE)}\n\n"
                    "Please choose a *lower quality*."
                )
            except Exception:
                pass
            return

        # ── Upload ────────────────────────────────────────────────────────────
        last_upload_edit = [0.0]

        async def upload_progress(current: int, total: int):
            now = asyncio.get_event_loop().time()
            if now - last_upload_edit[0] < 3 and current < total:
                return
            last_upload_edit[0] = now
            try:
                await status_msg.edit_text(
                    _upload_text(fmt_label, current, total),
                    reply_markup=None,
                )
            except Exception:
                pass

        try:
            await status_msg.edit_text(
                f"📤 *Uploading…*\n\n📌 *{fmt_label}*\n📦 {readable_bytes(filesize)}",
                reply_markup=None,
            )
        except Exception:
            pass

        # Fetch user's custom thumbnail
        user_thumb = await db_get_thumb(user.id)

        file_caption = (
            f"✅ *Done!*\n\n"
            f"📌 {fmt_label}\n"
            f"📦 {readable_bytes(filesize)}"
        )

        try:
            if fmt["is_audio"]:
                await app.send_audio(
                    user.id,
                    audio=filepath,
                    caption=file_caption,
                    thumb=user_thumb,
                    progress=upload_progress,
                )
            else:
                await app.send_video(
                    user.id,
                    video=filepath,
                    caption=file_caption,
                    thumb=user_thumb,
                    supports_streaming=True,
                    progress=upload_progress,
                )
        except Exception as e:
            logger.error("Upload error: %s", e)
            try:
                await status_msg.edit_text(
                    "❌ *Upload failed!*\n\n"
                    "The file was downloaded but couldn't be sent.\n"
                    "It may be corrupted — try again."
                )
            except Exception:
                pass
            return

    # Clean up
    pending.pop(url_hash, None)
    try:
        await status_msg.delete()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  COMMANDS  (auto-registered every startup)
# ═══════════════════════════════════════════════════════════════════════════════
from pyrogram.types import BotCommand  # noqa: E402
from pyrogram import idle              # noqa: E402

_BOT_COMMANDS = [
    BotCommand("start",     "🏠 Open home menu"),
    BotCommand("setthumb",  "🖼️ Set your custom video thumbnail"),
    BotCommand("delthumb",  "🗑️ Remove your custom thumbnail"),
    BotCommand("cancel",    "❌ Cancel current action"),
    BotCommand("setcookies","🍪 [Admin] Update cookies file (no restart needed)"),
    BotCommand("delcookies","🗑️ [Admin] Remove cookies"),
    BotCommand("broadcast", "📣 [Admin] Send message to all users"),
]


async def _register_commands():
    try:
        await app.set_bot_commands(_BOT_COMMANDS)
        logger.info("Bot commands registered (%d commands)", len(_BOT_COMMANDS))
    except Exception as e:
        logger.warning("Could not register bot commands: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
async def _main():
    async with app:
        logger.info(
            "Bot v4.0 online | max_size=%s GB | mongo=%s | cookies=%s",
            config.MAX_FILE_SIZE // (1024**3),
            "✓" if config.MONGO_URI else "✗",
            "✓" if _cookies_path else "✗",
        )
        await _register_commands()
        await idle()


if __name__ == "__main__":
    app.run(_main())
