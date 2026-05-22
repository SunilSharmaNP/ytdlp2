"""
╔══════════════════════════════════════════════════════════════════╗
║         YT Downloader Bot v3.0 — Configuration                  ║
║  All values can be overridden via environment variables.         ║
║  On Heroku: Dashboard → Settings → Config Vars                   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os

# ── Telegram Bot Credentials ───────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]

# From https://my.telegram.org → My Applications (required for 2 GB uploads)
API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

# ── MongoDB ────────────────────────────────────────────────────────────────────
# MongoDB Atlas connection string. Free tier at https://cloud.mongodb.com
# Example: mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/ytdlpbot
MONGO_URI = os.environ.get("MONGO_URI", "")
DB_NAME   = os.environ.get("DB_NAME", "ytdlpbot")

# ── Force Subscribe — Channel 1 (Bot Updates) ──────────────────────────────────
# Users must join BOTH channels before using the bot.
# Bot MUST be ADMIN of both channels.
# Format: "@username"  or  "-100xxxxxxxxxx"  or "" to disable.
FORCE_CHANNEL_1      = os.environ.get("FORCE_CHANNEL_1", "@yourchannel1")
FORCE_CHANNEL_1_NAME = os.environ.get("FORCE_CHANNEL_1_NAME", "Bot Updates")
FORCE_CHANNEL_1_URL  = os.environ.get("FORCE_CHANNEL_1_URL", "")  # override link

# ── Force Subscribe — Channel 2 (Deals) ───────────────────────────────────────
FORCE_CHANNEL_2      = os.environ.get("FORCE_CHANNEL_2", "@yourchannel2")
FORCE_CHANNEL_2_NAME = os.environ.get("FORCE_CHANNEL_2_NAME", "Deals Channel")
FORCE_CHANNEL_2_URL  = os.environ.get("FORCE_CHANNEL_2_URL", "")  # override link

# ── Admin User IDs ─────────────────────────────────────────────────────────────
# Comma-separated Telegram user IDs. Admins can use /setcookies, /delcookies.
ADMIN_IDS_RAW = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = [
    int(uid.strip()) for uid in ADMIN_IDS_RAW.split(",") if uid.strip().isdigit()
]

# ── File Size Limit ────────────────────────────────────────────────────────────
# Pyrogram (MTProto) supports up to 2 GB.
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 2 * 1024 * 1024 * 1024))

# ── Timeouts ───────────────────────────────────────────────────────────────────
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", 1800))  # 30 min
INFO_TIMEOUT     = int(os.environ.get("INFO_TIMEOUT", 60))        # format fetch

# ── YouTube Cookies ────────────────────────────────────────────────────────────
# Option A — File path:  COOKIES_FILE=/app/cookies.txt
# Option B — Base64 env: base64 cookies.txt | tr -d '\n'  → set COOKIES_B64
# Option C — Admin upload: admin replies to cookies.txt with /setcookies
COOKIES_FILE = os.environ.get("COOKIES_FILE", "cookies.txt")
COOKIES_B64  = os.environ.get("COOKIES_B64", "")

# ── Thumbnail Images ───────────────────────────────────────────────────────────
# Direct HTTPS image URL for menu banners. Upload free at https://telegra.ph
THUMB_DEFAULT = os.environ.get(
    "THUMB_DEFAULT",
    "https://telegra.ph/file/a1c8a804e6b7f1d1dc462.jpg",
)
THUMB_WELCOME = os.environ.get("THUMB_WELCOME") or THUMB_DEFAULT
THUMB_HOME    = os.environ.get("THUMB_HOME")    or THUMB_DEFAULT
THUMB_HOWTO   = os.environ.get("THUMB_HOWTO")   or THUMB_DEFAULT
THUMB_HELP    = os.environ.get("THUMB_HELP")    or THUMB_DEFAULT
THUMB_ABOUT   = os.environ.get("THUMB_ABOUT")   or THUMB_DEFAULT

# ── Quality Display ────────────────────────────────────────────────────────────
QUALITY_EMOJI = {
    2160: "🎥", 1440: "🎬", 1080: "🎬",
    720: "📺", 480: "📱", 360: "📱", 240: "🔅", 144: "🔅",
}
HD_LABEL = {
    2160: " 4K", 1440: " 2K", 1080: " Full HD",
    720: " HD", 480: "", 360: "", 240: "", 144: "",
}
