"""
YouTube Downloader Bot  v6.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━
• YouTube download via ytdown.to (y2mate API — no bot detection)
• Live download + upload progress bar with Cancel button
• 2 force-subscribe channels
• Custom user thumbnail (stored in MongoDB)
• MongoDB for user tracking
• Admin broadcast (text / photo / video / document) to all users
• Commands auto-registered on every startup
• Up to 2 GB uploads via Pyrogram MTProto
"""

import asyncio
import hashlib
import http.server
import logging
import os
import re
import tempfile
import threading
from datetime import datetime, timezone

import aiohttp

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
_session_name = config.STRING_SESSION if config.STRING_SESSION else ":memory:"
app = Client(
    _session_name,
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
_users        = _db["users"]    if _db is not None else None
_settings     = _db["settings"] if _db is not None else None


async def db_upsert_user(user_id: int, first_name: str, username: str | None):
    if _users is None:
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
    if _users is None:
        return None
    try:
        doc = await _users.find_one({"_id": user_id}, {"custom_thumb": 1})
        return doc.get("custom_thumb") if doc else None
    except Exception:
        return None


async def db_set_thumb(user_id: int, file_id: str | None):
    if _users is None:
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
    if _users is None:
        return 0
    try:
        return await _users.count_documents({})
    except Exception:
        return 0


async def db_iter_user_ids():
    if _users is None:
        return
    try:
        async for doc in _users.find({}, {"_id": 1}):
            yield doc["_id"]
    except Exception as e:
        logger.error("db_iter_user_ids: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  IN-MEMORY STATE
# ═══════════════════════════════════════════════════════════════════════════════
# url_hash → {"url", "title", "thumb", "formats": list[dict]}
pending: dict[str, dict] = {}

# url_hash → asyncio.Event  (cancel signal)
cancel_events: dict[str, asyncio.Event] = {}

# user_ids currently waiting to send their thumbnail photo
thumb_waiting: set[int] = set()

# admin_id → True   (waiting for the message to broadcast)
broadcast_waiting: set[int] = set()

# admin_id → pyrogram.Message  (message captured, waiting for confirm/cancel)
broadcast_pending: dict[int, object] = {}


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
        "  • Download YouTube videos in any quality",
        "  • Pick 144p up to 4K, or MP3 audio",
        "  • Live progress bar while downloading",
        "  • Custom thumbnail on your downloads",
        "  • Up to **2 GB** per file",
    ]
    return "\n".join(lines)


def cap_home(name: str) -> str:
    return (
        f"🏠 *Home — Hello, {name}!*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎬 *YouTube Video Downloader*\n\n"
        "Send me any YouTube link and I'll fetch all available\n"
        "qualities so you can pick exactly what you want.\n\n"
        f"📦 *Max file:*  {config.MAX_FILE_SIZE // (1024**3)} GB  "
        "⚡ Fast  🔒 Private  🎯 Your Choice"
    )


CAP_HOWTO = (
    "⬇️ *How to Download*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "1️⃣  Copy any YouTube link\n"
    "2️⃣  Paste it in this chat\n"
    "3️⃣  Choose your quality from the buttons\n"
    "4️⃣  Watch live progress, then receive your file 🚀\n\n"
    "🎬 *Supported:* YouTube (youtube.com, youtu.be)\n\n"
    "📦 *Video:*  MP4  |  🎵 *Audio:*  MP3\n"
    "📏 *Resolutions:*  144p  →  4K (2160p)"
)

CAP_HELP = (
    "❓ *Help & FAQ*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🔹 *What sites are supported?*\n"
    "YouTube (youtube.com and youtu.be links).\n\n"
    "🔹 *Why is a quality missing?*\n"
    "Not all videos are available in all resolutions.\n\n"
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
    "🤖  *YouTube Downloader Bot*\n"
    "🌐  Powered by *ytdown.to* API\n"
    "🐍  Built with `Pyrogram` (MTProto)\n"
    f"📦  Supports up to *{config.MAX_FILE_SIZE // (1024**3)} GB*\n"
    "🗄️  MongoDB for user data\n"
    "🖼️  Custom thumbnails per user\n"
    "🔒  Privacy-first — zero video data stored\n\n"
    "📌  *Version:* 6.0\n"
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
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
_URL_RE = re.compile(r"https?://[^\s<>\"{}|\\^`\[\]]+")
_YT_RE  = re.compile(
    r"(?:https?://)?(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be|yt\.be)(?:/[^\s]*)?",
    re.IGNORECASE,
)
_YT_ID_RE = re.compile(
    r"(?:v=|/v/|youtu\.be/|/embed/|/shorts/|/live/)([A-Za-z0-9_-]{11})"
)


def extract_url(text: str) -> str | None:
    m = _URL_RE.search(text)
    return m.group(0) if m else None


def _is_youtube_url(url: str) -> bool:
    return bool(_YT_RE.match(url))


def _extract_youtube_id(url: str) -> str | None:
    m = _YT_ID_RE.search(url)
    return m.group(1) if m else None


def readable_bytes(b: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.2f} TB"


def _upload_text(fmt_label: str, current: int, total: int) -> str:
    pct = (current / total * 100) if total else 0
    filled = min(int(pct / 10), 10)
    bar = "█" * filled + "░" * (10 - filled)
    return (
        f"📤 Uploading…\n\n"
        f"📌 *{fmt_label}*\n\n"
        f"`[{bar}] {pct:.1f}%`\n\n"
        f"`{readable_bytes(current)} / {readable_bytes(total)}`"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  YTDOWN.TO API  — YouTube download (y2mate-compatible, no auth, no cookies)
# ═══════════════════════════════════════════════════════════════════════════════
# ytdown.to uses the exact same /en27/ API path as y2mate — it is a y2mate
# variant. Their servers process the YouTube URL themselves — Heroku's IP
# block on YouTube does NOT affect this approach.
#
# Flow:
#   1. POST /en27/analyze/ajax  → get (vid, title, quality_map)
#   2. User picks quality       → bot stores (ytdown_vid, ytdown_k)
#   3. At download time: POST /en27/convert/ajax  → direct CDN link
#   4. Stream-download CDN link → upload to Telegram

_YTDOWN_MIRRORS = [
    "https://app.ytdown.to",
    "https://www.y2mate.com/mates",
    "https://y2mate.guru",
    "https://www.yt5s.io",
]
_YTDOWN_HDRS = {
    "Accept":           "*/*",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent":       (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}
_YTDOWN_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def _ytdown_analyze(yt_url: str) -> tuple[str | None, str | None, str | None, dict]:
    """
    Try each ytdown mirror until one returns a successful analyze response.
    Returns (vid, title, thumbnail_url, info_dict) or (None,...) on all failures.
    info_dict: {"mp4": {"1080": {"k": "...", "q_text": "...", "size": "..."}}, "mp3": {...}}
    """
    for mirror in _YTDOWN_MIRRORS:
        analyze_url = f"{mirror}/en27/analyze/ajax"
        hdrs = {**_YTDOWN_HDRS, "Origin": mirror, "Referer": f"{mirror}/en27/"}
        try:
            async with aiohttp.ClientSession(headers=hdrs, timeout=_YTDOWN_TIMEOUT) as sess:
                async with sess.post(
                    analyze_url,
                    data={"url": yt_url, "q_auto": 0, "ajax": 1},
                ) as resp:
                    if resp.status != 200:
                        logger.warning("ytdown %s analyze HTTP %s", mirror, resp.status)
                        continue
                    data = await resp.json(content_type=None)
        except Exception as e:
            logger.warning("ytdown %s analyze error: %s", mirror, e)
            continue

        if str(data.get("status", "")).lower() not in ("ok", "1", "true"):
            logger.warning("ytdown %s status not ok: %s", mirror, data.get("status"))
            continue

        vid   = data.get("vid") or data.get("id")
        title = data.get("title")
        thumb = (
            data.get("thumbnail") or data.get("thumb")
            or (f"https://img.youtube.com/vi/{vid}/hqdefault.jpg" if vid else None)
        )
        info  = data.get("info") or data.get("links") or {}
        if vid and info:
            logger.info("ytdown OK via %s for vid=%s", mirror, vid)
            return vid, title, thumb, info

    return None, None, None, {}


async def _ytdown_get_link(vid: str, k: str) -> tuple[str | None, str]:
    """
    Call ytdown convert endpoint to get a direct CDN download URL.
    Returns (download_url, error_string).
    """
    for mirror in _YTDOWN_MIRRORS:
        convert_url = f"{mirror}/en27/convert/ajax"
        hdrs = {**_YTDOWN_HDRS, "Origin": mirror, "Referer": f"{mirror}/en27/"}
        try:
            async with aiohttp.ClientSession(headers=hdrs, timeout=_YTDOWN_TIMEOUT) as sess:
                async with sess.post(
                    convert_url,
                    data={"vid": vid, "k": k, "q_auto": 0, "ajax": 1},
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
        except Exception as e:
            logger.warning("ytdown %s convert error: %s", mirror, e)
            continue

        dl = (
            data.get("dlink") or data.get("downloadUrl")
            or data.get("url") or data.get("dl")
        )
        if dl:
            return dl, ""

    return None, "all ytdown mirrors failed for convert"


def _parse_ytdown_formats(vid: str, info: dict) -> list[dict]:
    """
    Convert ytdown analyze info dict → internal format list.
    info = {"mp4": {"1080": {k, q_text, size}, ...}, "mp3": {"128": {...}}}
    """
    _VE = {
        "2160": "🎥", "1440": "🎬", "1080": "🎬", "720": "📺",
        "480": "📱", "360": "📱", "240": "🔅", "144": "🔅",
    }
    fmts: list[dict] = []

    mp4 = info.get("mp4") or info.get("video") or {}
    for qual in sorted(mp4.keys(), key=lambda q: int(q) if q.isdigit() else 0, reverse=True):
        val   = mp4[qual]
        k     = val.get("k", "")
        if not k:
            continue
        q_txt = val.get("q_text") or f"{qual}p (.mp4)"
        size  = val.get("size", "")
        emoji = _VE.get(qual, "🎬")
        fmts.append({
            "label":      f"{emoji}  {q_txt}" + (f"  [{size}]" if size else ""),
            "is_audio":   False,
            "ytdown_vid": vid,
            "ytdown_k":   k,
            "ytdown_ext": "mp4",
        })

    mp3 = info.get("mp3") or info.get("audio") or {}
    for qual in sorted(mp3.keys(), reverse=True):
        val   = mp3[qual]
        k     = val.get("k", "")
        if not k:
            continue
        q_txt = val.get("q_text") or f"{qual}kbps (.mp3)"
        fmts.append({
            "label":      f"🎵  {q_txt}",
            "is_audio":   True,
            "ytdown_vid": vid,
            "ytdown_k":   k,
            "ytdown_ext": "mp3",
        })

    return fmts


async def _ytdown_download_file(
    fmt: dict,
    out_dir: str,
    url_hash: str,
    cancel_ev: asyncio.Event,
    status_msg,
    fmt_label: str,
) -> tuple[str | None, str]:
    """
    1. Call ytdown convert API → get fresh CDN URL
    2. Stream-download to disk with live progress
    Returns (filepath, "done"|"cancelled"|"error").
    """
    try:
        await status_msg.edit_text(
            f"🔗 *Getting download link from ytdown.to…*\n\n📌 *{fmt_label}*",
            reply_markup=kb_cancel(url_hash),
        )
    except Exception:
        pass

    dl_url, err = await _ytdown_get_link(fmt["ytdown_vid"], fmt["ytdown_k"])
    if not dl_url:
        logger.error("ytdown get_link failed: %s", err)
        return None, "error"

    ext      = fmt.get("ytdown_ext", "mp4")
    filepath = os.path.join(out_dir, f"{fmt['ytdown_vid']}.{ext}")
    last_edit = 0.0

    async def _update(pct: float, speed_bps: float, total: int):
        nonlocal last_edit
        now = asyncio.get_event_loop().time()
        if now - last_edit < 4:
            return
        last_edit = now
        filled = min(int(pct / 10), 10)
        bar    = "█" * filled + "░" * (10 - filled)
        spd    = readable_bytes(speed_bps) + "/s" if speed_bps else "N/A"
        sz     = readable_bytes(total) if total else "?"
        try:
            await status_msg.edit_text(
                f"📥 Downloading…\n\n📌 *{fmt_label}*\n\n"
                f"`[{bar}] {pct:.1f}%`\n\n"
                f"📦 Size : `{sz}`\n⚡ Speed: `{spd}`",
                reply_markup=kb_cancel(url_hash),
            )
        except Exception:
            pass

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3600)) as sess:
            async with sess.get(dl_url) as resp:
                if resp.status not in (200, 206):
                    logger.error("ytdown CDN HTTP %s", resp.status)
                    return None, "error"
                total      = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                t0         = asyncio.get_event_loop().time()
                with open(filepath, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(65536):
                        if cancel_ev.is_set():
                            return None, "cancelled"
                        fh.write(chunk)
                        downloaded += len(chunk)
                        elapsed = asyncio.get_event_loop().time() - t0 or 0.001
                        pct     = (downloaded / total * 100) if total else 0
                        await _update(pct, downloaded / elapsed, total)
    except asyncio.CancelledError:
        return None, "cancelled"
    except Exception as e:
        logger.error("ytdown CDN download error: %s", e)
        return None, "error"

    if cancel_ev.is_set():
        return None, "cancelled"
    return filepath, "done"


# ═══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    user = msg.from_user
    sub  = await check_sub(user.id)
    if not sub["ok"]:
        await app.send_photo(
            user.id,
            photo=config.THUMB_WELCOME,
            caption=cap_welcome(user.first_name),
            reply_markup=kb_join(sub),
        ) if config.THUMB_WELCOME else await app.send_message(
            user.id, cap_welcome(user.first_name), reply_markup=kb_join(sub)
        )
        return
    await db_upsert_user(user.id, user.first_name, user.username)
    await send_menu(
        None, chat_id=user.id,
        thumb=config.THUMB_HOME,
        text=cap_home(user.first_name),
        keyboard=kb_home(),
    )


@app.on_message(filters.command("setthumb") & filters.private)
async def cmd_setthumb(_, msg: Message):
    user = msg.from_user
    sub  = await check_sub(user.id)
    if not sub["ok"]:
        await msg.reply("❌ Join required channels first. Send /start.")
        return
    thumb_waiting.add(user.id)
    await msg.reply(
        "🖼️ *Send your thumbnail photo now.*\n\n"
        "Any photo you send next will be saved as your thumbnail.\n"
        "_Send /cancel to abort._"
    )


@app.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(_, msg: Message):
    user = msg.from_user
    await db_set_thumb(user.id, None)
    await msg.reply("🗑️ *Thumbnail removed!*")


@app.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(_, msg: Message):
    user = msg.from_user
    thumb_waiting.discard(user.id)
    broadcast_waiting.discard(user.id)
    broadcast_pending.pop(user.id, None)
    await msg.reply("❌ *Cancelled.*")


@app.on_message(filters.command("broadcast") & filters.private)
async def cmd_broadcast(_, msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.reply("⛔ Admin only.")
        return
    broadcast_waiting.add(msg.from_user.id)
    await msg.reply(
        "📣 *Broadcast mode.*\n\n"
        "Send me the message (text, photo, video, or document) you want to broadcast.\n"
        "_/cancel to abort._"
    )


@app.on_message(filters.command("stats") & filters.private)
async def cmd_stats(_, msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        await msg.reply("⛔ Admin only.")
        return
    count = await db_count_users()
    await msg.reply(f"👥 *Total users:* `{count}`")


# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE HANDLER  (URLs + photos + broadcast capture)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.private & ~filters.command(["start","setthumb","delthumb","cancel","broadcast","stats"]))
async def on_message(_, msg: Message):
    user = msg.from_user

    # ── Broadcast capture ─────────────────────────────────────────────────────
    if user.id in broadcast_waiting:
        broadcast_waiting.discard(user.id)
        broadcast_pending[user.id] = msg
        await msg.reply(
            "📣 *Ready to broadcast this message to all users.*\n\n"
            "Confirm?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Send", callback_data="bc_confirm"),
                    InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"),
                ]
            ]),
        )
        return

    # ── Thumbnail capture ─────────────────────────────────────────────────────
    if user.id in thumb_waiting and msg.photo:
        thumb_waiting.discard(user.id)
        file_id = msg.photo.file_id
        await db_set_thumb(user.id, file_id)
        await msg.reply(
            "✅ *Thumbnail saved!*\n\n"
            "It will appear on all your future downloads.\n"
            "_Tap *My Thumbnail* → *Remove* to delete it._"
        )
        return

    # ── Auth gate ─────────────────────────────────────────────────────────────
    sub = await check_sub(user.id)
    if not sub["ok"]:
        await app.send_message(
            user.id,
            "❌ *You must join both channels first.*",
            reply_markup=kb_join(sub),
        )
        return

    # ── URL detection ─────────────────────────────────────────────────────────
    text = msg.text or msg.caption or ""
    url  = extract_url(text)
    if url:
        await db_upsert_user(user.id, user.first_name, user.username)
        await handle_url(msg, user, url)
        return

    # ── Fallthrough ───────────────────────────────────────────────────────────
    await msg.reply(
        "🔗 *Send a YouTube link to download.*\n\n"
        "_Example: https://youtube.com/watch?v=..._"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  URL HANDLER — ytdown.to only
# ═══════════════════════════════════════════════════════════════════════════════
async def handle_url(msg: Message, user, url: str):
    if not _is_youtube_url(url):
        await msg.reply(
            "⚠️ *Only YouTube links are supported.*\n\n"
            "Send a youtube.com or youtu.be link."
        )
        return

    status = await msg.reply(
        "🔍 *Fetching video info from ytdown.to…*\n"
        "_Please wait…_"
    )

    vid, title, thumb, info = await _ytdown_analyze(url)

    if not vid or not info:
        try:
            await status.edit_text(
                "❌ *Could not fetch video info!*\n\n"
                "• Make sure the link is a valid public YouTube video\n"
                "• Age-restricted or members-only videos are not supported\n"
                "• Try again in a few seconds"
            )
        except Exception:
            pass
        return

    formats = _parse_ytdown_formats(vid, info)
    if not formats:
        try:
            await status.edit_text(
                "❌ *No downloadable formats found for this video.*\n\n"
                "The video might be restricted. Try a different link."
            )
        except Exception:
            pass
        return

    title = title or "YouTube Video"
    thumb = thumb or config.THUMB_DEFAULT

    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    pending[url_hash] = {
        "url":     url,
        "title":   title,
        "thumb":   thumb,
        "formats": formats,
    }

    short = title[:60] + ("…" if len(title) > 60 else "")
    cap   = (
        f"🎬 *{short}*\n\n"
        f"📊 *Choose your quality:*\n"
        f"_({len(formats)} options — tap to start downloading)_\n"
        f"_⚡ Via ytdown.to — no bot detection_"
    )

    await status.delete()
    if thumb:
        await app.send_photo(
            user.id, photo=thumb, caption=cap,
            reply_markup=kb_qualities(url_hash, formats),
        )
    else:
        await app.send_message(
            user.id, cap,
            reply_markup=kb_qualities(url_hash, formats),
        )


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
            await query.answer("⚠️  Please join BOTH channels first!", show_alert=True)
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
            await asyncio.sleep(0.05)
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
            await query.edit_message_caption(
                caption="❌ *Cancelled.*\nSend a YouTube link anytime to start over."
            )
        except Exception:
            await query.edit_message_text(
                "❌ *Cancelled.*\nSend a YouTube link anytime to start over."
            )
        return

    # ── Cancel active download ────────────────────────────────────────────────
    if data.startswith("cancel_dl:"):
        url_hash = data.split(":", 1)[1]
        ev = cancel_events.get(url_hash)
        if ev:
            ev.set()
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
            await query.edit_message_caption(
                caption="⚠️ *Session expired.* Please send the link again."
            )
        except Exception:
            await query.edit_message_text(
                "⚠️ *Session expired.* Please send the link again."
            )
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

    try:
        status_msg = await query.edit_message_caption(
            caption=f"🔗 *Getting download link…*\n\n📌 *{fmt_label}*",
            reply_markup=kb_cancel(url_hash),
        )
    except Exception:
        status_msg = await query.edit_message_text(
            f"🔗 *Getting download link…*\n\n📌 *{fmt_label}*",
            reply_markup=kb_cancel(url_hash),
        )

    cancel_ev = asyncio.Event()
    cancel_events[url_hash] = cancel_ev

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath, result = await _ytdown_download_file(
            fmt, tmpdir, url_hash, cancel_ev, status_msg, fmt_label
        )
        cancel_events.pop(url_hash, None)

        if result == "cancelled":
            try:
                await status_msg.edit_text(
                    "🛑 *Download cancelled.*\nSend the link again anytime."
                )
            except Exception:
                pass
            return

        if result == "error" or not filepath:
            try:
                await status_msg.edit_text(
                    "❌ *Download failed!*\n\n"
                    "Try a different quality or resend the link.\n"
                    "_(The video might be restricted or unavailable.)_"
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

        user_thumb    = await db_get_thumb(user.id)
        file_caption  = f"✅ *Done!*\n\n📌 {fmt_label}\n📦 {readable_bytes(filesize)}"
        _AUDIO_EXTS   = {".mp3", ".aac", ".flac", ".opus", ".ogg", ".wav", ".m4a"}
        send_as_audio = fmt["is_audio"] or os.path.splitext(filepath)[1].lower() in _AUDIO_EXTS

        try:
            if send_as_audio:
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
                await app.send_document(
                    user.id,
                    document=filepath,
                    caption=file_caption,
                    progress=upload_progress,
                )
            except Exception as e2:
                logger.error("Document upload also failed: %s", e2)
                try:
                    await status_msg.edit_text(
                        "❌ *Upload failed!*\n\n"
                        "The file was downloaded but couldn't be sent.\n"
                        "It may be corrupted — try again."
                    )
                except Exception:
                    pass
                return

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
    BotCommand("broadcast", "📣 [Admin] Send message to all users"),
    BotCommand("stats",     "📊 [Admin] Total user count"),
]


async def _register_commands():
    try:
        await app.set_bot_commands(_BOT_COMMANDS)
        logger.info("Bot commands registered (%d commands)", len(_BOT_COMMANDS))
    except Exception as e:
        logger.warning("Could not register bot commands: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  HEROKU WEB DYNO KEEPALIVE
# ═══════════════════════════════════════════════════════════════════════════════
def _start_keepalive_server() -> None:
    port = int(os.environ.get("PORT", 8080))

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = http.server.HTTPServer(("0.0.0.0", port), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info("Keepalive HTTP server listening on port %d", port)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
async def _main():
    from pyrogram.errors import FloodWait as _FloodWait

    _start_keepalive_server()

    retries = 0
    while True:
        try:
            async with app:
                logger.info(
                    "Bot v6.0 online | max_size=%s GB | mongo=%s | session=%s",
                    config.MAX_FILE_SIZE // (1024**3),
                    "✓" if config.MONGO_URI else "✗",
                    "string" if config.STRING_SESSION else "memory",
                )
                await _register_commands()
                await idle()
            break
        except _FloodWait as e:
            wait = e.value + 5
            retries += 1
            logger.warning(
                "FloodWait %ds from Telegram (attempt %d). Waiting…", wait, retries
            )
            await asyncio.sleep(wait)
        except Exception as e:
            logger.error("Startup error: %s", e)
            raise


if __name__ == "__main__":
    app.run(_main())
