#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║        SMART TELEGRAM DOWNLOADER BOT  v6.0  ⚡  OWNER EDITION       ║
║                                                                      ║
║  🔒 ACCESS CONTROL                                                   ║
║  • Owner-only by default (set OWNER_ID env var)                      ║
║  • Owner can whitelist other users via /adduser                      ║
║  • Persistent whitelist saved to users.json                          ║
║  • All unauthorized access attempts are logged & notified to owner   ║
║                                                                      ║
║  ⚡ DOWNLOAD ENGINE                                                  ║
║  • Parallel chunked download (like IDM / aria2c)                     ║
║  •   → splits file into N segments, downloads simultaneously         ║
║  •   → auto-detects if server supports Range requests                ║
║  •   → falls back to single-stream if not supported                  ║
║  • Configurable worker count (default: 32 parallel streams)          ║
║  • Async file I/O with aiofiles + aiohttp                            ║
║  • Smart retry with exponential backoff                              ║
║                                                                      ║
║  🌐 SMART LINK RESOLVER                                              ║
║  • Google Drive (with large-file confirm bypass)                     ║
║  • Dropbox, OneDrive, MediaFire                                      ║
║  • Terabox / 4shared / Gofile.io                                     ║
║  • LinkForge CDN (cdn2.linkforge.xyz and *.linkforge.xyz)            ║
║  • Flowbit Heroku direct links (flowbit-*.herokuapp.com/dl/…)        ║
║  • Telegram CDN direct links (cdn*.telegram.org)                     ║
║  • YouTube & 1000+ video sites via yt-dlp                            ║
║  • Generic JS-obfuscated page scraper (regex + meta-refresh)         ║
║  • Password-protected page detection & prompting                     ║
║                                                                      ║
║  📊 OTHER FEATURES                                                   ║
║  • /stats    — per-user download stats                               ║
║  • /history  — 📜 full download history (paginated)                  ║
║  • /delete   — 🗑 wipe all downloaded files & free disk space        ║
║  • /queue    — 📋 view / manage the download queue                   ║
║  • /pause    — ⏸ pause active download                               ║
║  • /resume   — ▶️ resume paused download                             ║
║  • /users    — list whitelisted users (owner only)                   ║
║  • /adduser /removeuser — manage whitelist (owner only)              ║
║  • Live progress: current · peak · avg speed, ETA & elapsed time     ║
║  • 📋 Multi-URL queue mode — paste multiple links in one message     ║
║  •   → downloads sequentially, summary shown at end                  ║
║  • 🌙 Bandwidth scheduler — slow hours (22:00–07:00) auto-throttle   ║
║  • 🔔 Auto-notify on finish — DM when long download completes        ║
║  • Unlimited file size (auto-splits >1.9 GB into parts)             ║
║  • 🔍 Keyword extractor with fuzzy dedup (/keyword)                  ║
╚══════════════════════════════════════════════════════════════════════╝

Install:
    pip install python-telegram-bot aiohttp aiofiles requests \
                yt-dlp cloudscraper beautifulsoup4

Run:
    export TELEGRAM_BOT_TOKEN=your_token
    export OWNER_ID=your_telegram_user_id
    python telegram_downloader_bot.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import mimetypes
import os
import re
import shutil
import tempfile
import time
import io
import collections
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlencode, urlparse, urljoin

import aiofiles
import aiohttp
import requests
import cloudscraper
from bs4 import BeautifulSoup

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
BOT_TOKEN        = os.environ.get("TELEGRAM_BOT_TOKEN", "8661120242:AAEO3UUHVKrcQha_XCGsVA0tJJq3qy5_8YQ")
OWNER_ID         = int(os.environ.get("OWNER_ID", "5028065177"))          # your Telegram user ID
USERS_FILE       = Path("users.json")                             # persistent whitelist
STATS_FILE       = Path("stats.json")                             # download stats

DOWNLOAD_DIR     = Path(tempfile.gettempdir()) / "tg_downloader"
PARALLEL_WORKERS = int(os.environ.get("DL_WORKERS", "32"))
SEGMENT_SIZE     = 25 * 1024 * 1024      # 25 MB per segment (larger = fewer segments, less overhead)
CHUNK_SIZE       = 2 * 1024 * 1024       # 2 MB read chunks (larger = fewer syscalls, faster I/O)
SPLIT_SIZE       = 1_900 * 1024 * 1024   # 1.9 GB Telegram cap
PROGRESS_EVERY   = 3                      # seconds between progress edits
MAX_RETRIES      = 4
CONNECT_TIMEOUT  = aiohttp.ClientTimeout(total=None, connect=10, sock_read=30)
# ── Speed cap: 0 = UNLIMITED. Set MAX_SPEED_MBS env var to throttle (e.g. 300 for 300 MB/s).
_speed_env       = int(os.environ.get("MAX_SPEED_MBS", "0"))
MAX_SPEED_BPS    = _speed_env * 1024 * 1024   # 0 = no limit
NOTIFY_THRESHOLD = int(os.environ.get("NOTIFY_SECS", "30"))  # auto-notify if download > N seconds
HISTORY_FILE     = Path("history.json")                       # per-user download history
# ─────────────────────────────────────────────────────────────

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Conversation states ────────────────────────────────────────
WAITING_PASSWORD   = 1
KW_WAITING_SITE    = 20   # step 1: ask for site/domain
KW_WAITING_KEYWORD = 21   # step 2: ask for keywords

# ── Global pause/resume per-user flag ─────────────────────────
# Maps user_id -> asyncio.Event (set = running, clear = paused)
_pause_events: dict[int, asyncio.Event] = {}

URL_RE = re.compile(
    r"https?://"
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
    r"localhost|\d{1,3}(?:\.\d{1,3}){3})"
    r"(?::\d+)?(?:/?|[/?]\S+)",
    re.IGNORECASE,
)

PASSWORD_HINTS = [
    "password", "passwd", "pwd", "pass required",
    "enter password", "file password", "download password",
    "protected", "this file is locked",
]

# ─────────────────────────────────────────────────────────────
#  PERSISTENT STORAGE  (users.json / stats.json)
# ─────────────────────────────────────────────────────────────

def _load_json(path: Path, default) -> dict | list:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return default


def _save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2))


def load_whitelist() -> set[int]:
    raw = _load_json(USERS_FILE, [])
    wl = set(int(x) for x in raw)
    if OWNER_ID:
        wl.add(OWNER_ID)
    return wl


def save_whitelist(wl: set[int]):
    data = sorted(wl - {OWNER_ID})   # owner is always implicit
    _save_json(USERS_FILE, data)


def load_stats() -> dict:
    return _load_json(STATS_FILE, {})


def save_stats(stats: dict):
    _save_json(STATS_FILE, stats)


def record_download(user_id: int, filename: str, size_bytes: int):
    stats = load_stats()
    uid = str(user_id)
    if uid not in stats:
        stats[uid] = {"count": 0, "total_bytes": 0, "files": []}
    stats[uid]["count"] += 1
    stats[uid]["total_bytes"] += size_bytes
    stats[uid]["files"].append({
        "name": filename,
        "size": size_bytes,
        "ts": datetime.utcnow().isoformat(),
    })
    # keep only last 50 file entries
    stats[uid]["files"] = stats[uid]["files"][-50:]
    save_stats(stats)


def load_history() -> dict:
    return _load_json(HISTORY_FILE, {})


def save_history(hist: dict):
    _save_json(HISTORY_FILE, hist)


def record_history(user_id: int, filename: str, size_bytes: int, url: str, elapsed: float):
    hist = load_history()
    uid = str(user_id)
    if uid not in hist:
        hist[uid] = []
    hist[uid].append({
        "name": filename,
        "size": size_bytes,
        "url": url,
        "elapsed": round(elapsed, 1),
        "ts": datetime.utcnow().isoformat(),
    })
    hist[uid] = hist[uid][-200:]   # keep last 200 entries
    save_history(hist)


# ─────────────────────────────────────────────────────────────
#  ACCESS CONTROL
# ─────────────────────────────────────────────────────────────

def is_owner(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID


def is_authorized(user_id: int) -> bool:
    if OWNER_ID == 0:
        return True   # no owner set → open to all (dev mode)
    wl = load_whitelist()
    return user_id in wl


def owner_only(func):
    """Decorator: only owner can invoke the command."""
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        uid = update.effective_user.id
        if not is_owner(uid):
            await update.message.reply_text("🚫 This command is reserved for the bot owner.")
            return
        return await func(update, ctx, *args, **kwargs)
    return wrapper


def authorized_only(func):
    """Decorator: only whitelisted users (+ owner) can use the bot."""
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not is_authorized(user.id):
            name = user.full_name or str(user.id)
            logger.warning(f"Unauthorized access attempt: {name} ({user.id})")
            await update.message.reply_text(
                "🔒 *Access Denied*\n\n"
                "This bot is private. Contact the owner to request access.",
                parse_mode=ParseMode.MARKDOWN,
            )
            # Notify owner
            if OWNER_ID:
                try:
                    await ctx.bot.send_message(
                        OWNER_ID,
                        f"⚠️ *Unauthorized access attempt*\n"
                        f"👤 Name: {name}\n"
                        f"🆔 ID: `{user.id}`\n"
                        f"🕐 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
                        f"Use `/adduser {user.id}` to grant access.",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass
            return ConversationHandler.END
        return await func(update, ctx, *args, **kwargs)
    return wrapper


# ─────────────────────────────────────────────────────────────
#  DISPLAY HELPERS
# ─────────────────────────────────────────────────────────────

def fmt_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    for unit in ("KB", "MB", "GB", "TB"):
        b /= 1024
        if b < 1024:
            return f"{b:.2f} {unit}"
    return f"{b:.2f} PB"


def fmt_speed(bps: float) -> str:
    return fmt_size(int(bps)) + "/s"


def fmt_eta(seconds: float) -> str:
    if seconds <= 0 or math.isinf(seconds):
        return "∞"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def progress_bar(done: int, total: int, width: int = 18) -> str:
    if not total:
        return "▒" * width
    filled = int(width * done / total)
    return "█" * filled + "░" * (width - filled)


def sanitize_filename(name: str) -> str:
    """Remove dangerous characters from filenames."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip(". ")
    return name or "download"


# ─────────────────────────────────────────────────────────────
#  LINK RESOLVERS
# ─────────────────────────────────────────────────────────────

def resolve_google_drive(url: str) -> Optional[str]:
    patterns = [
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"/open\?id=([a-zA-Z0-9_-]+)",
    ]
    fid = None
    for p in patterns:
        m = re.search(p, url)
        if m:
            fid = m.group(1)
            break
    if not fid:
        return None
    direct = f"https://drive.google.com/uc?export=download&id={fid}"
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    r = s.get(direct, stream=True, timeout=20, allow_redirects=True)
    if "Content-Disposition" in r.headers:
        return r.url
    # Large-file confirm page bypass
    soup = BeautifulSoup(r.text, "html.parser")
    # New GDrive confirm form
    form = soup.find("form", id="download-form") or soup.find("form")
    if form:
        action = form.get("action", direct)
        if not action.startswith("http"):
            action = "https://drive.google.com" + action
        inputs = {
            i.get("name"): i.get("value", "")
            for i in form.find_all("input")
            if i.get("name")
        }
        return action + ("&" if "?" in action else "?") + urlencode(inputs)
    # Fallback: extract confirm token
    confirm = re.search(r'confirm=([0-9A-Za-z_-]+)', r.text)
    if confirm:
        return f"{direct}&confirm={confirm.group(1)}"
    # uuid-style warning bypass (newer GDrive)
    uuid_m = re.search(r'"(https://drive\.usercontent\.google\.com/download[^"]+)"', r.text)
    if uuid_m:
        return uuid_m.group(1)
    return direct


def resolve_dropbox(url: str) -> str:
    url = re.sub(r"[?&]dl=\d", "", url)
    sep = "&" if "?" in url else "?"
    return url.replace("www.dropbox.com", "dl.dropboxusercontent.com") + sep + "dl=1"


def resolve_onedrive(url: str) -> str:
    import base64
    encoded = base64.b64encode(url.encode()).decode().rstrip("=").replace("/", "_").replace("+", "-")
    return f"https://api.onedrive.com/v1.0/shares/u!{encoded}/root/content"


def resolve_mediafire(url: str) -> Optional[str]:
    scraper = cloudscraper.create_scraper()
    r = scraper.get(url, timeout=20)
    soup = BeautifulSoup(r.text, "html.parser")
    btn = soup.find("a", {"id": "downloadButton"}) or \
          soup.find("a", {"class": re.compile(r"download", re.I)})
    if btn and btn.get("href"):
        return btn["href"]
    m = re.search(r'"(https://download\d+\.mediafire\.com/[^"]+)"', r.text)
    return m.group(1) if m else None


def resolve_terabox(url: str) -> Optional[str]:
    """Resolve Terabox / Freeterabox / 1024tera direct link."""
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        # Look for direct download button
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "download" in href.lower() and href.startswith("http"):
                return href
        # Try embedded JSON config (some Terabox variants)
        m = re.search(r'"dlink"\s*:\s*"(https?://[^"]+)"', r.text)
        if m:
            return m.group(1).replace("\\/", "/")
    except Exception as e:
        logger.warning(f"Terabox resolve failed: {e}")
    return None


def resolve_gofile(url: str) -> Optional[str]:
    """Resolve gofile.io share links."""
    m = re.search(r"gofile\.io/d/([a-zA-Z0-9]+)", url)
    if not m:
        return None
    code = m.group(1)
    try:
        # Get a guest token
        tok_r = requests.post("https://api.gofile.io/accounts", timeout=10)
        if tok_r.ok:
            token = tok_r.json().get("data", {}).get("token")
        else:
            token = None
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        info = requests.get(
            f"https://api.gofile.io/contents/{code}?wt=4fd6sg89d7s6",
            headers=headers, timeout=15
        ).json()
        files = info.get("data", {}).get("children", {})
        for fid, fdata in files.items():
            if fdata.get("type") == "file":
                link = fdata.get("link")
                if link:
                    return link
    except Exception as e:
        logger.warning(f"Gofile resolve failed: {e}")
    return None


def resolve_linkforge(url: str) -> Optional[str]:
    """
    Resolve cdn2.linkforge.xyz (and any *.linkforge.xyz) short/CDN redirect links.
    These sometimes return a JSON metadata file (download.json) with the real URL inside.
    We parse that JSON and follow to the actual file.
    """
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, allow_redirects=True, timeout=20,
                        headers={"User-Agent": "Mozilla/5.0"})
        ct = r.headers.get("Content-Type", "")
        cd = r.headers.get("Content-Disposition", "")

        # ── JSON metadata response (e.g. download.json with a "url" or "direct" field) ──
        if "application/json" in ct or (cd and "download.json" in cd.lower()) or (
            r.url.endswith(".json") or "download.json" in r.url
        ):
            try:
                data = r.json()
                # Common field names LinkForge CDN uses
                for key in ("url", "direct", "download_url", "link", "file_url", "direct_url"):
                    val = data.get(key)
                    if val and isinstance(val, str) and val.startswith("http"):
                        logger.info(f"LinkForge: resolved real URL from JSON key '{key}': {val}")
                        return val
                # If JSON is a list, grab first item
                if isinstance(data, list) and data:
                    first = data[0]
                    if isinstance(first, dict):
                        for key in ("url", "direct", "download_url", "link", "file_url"):
                            val = first.get(key)
                            if val and isinstance(val, str) and val.startswith("http"):
                                return val
                    elif isinstance(first, str) and first.startswith("http"):
                        return first
                logger.warning(f"LinkForge: JSON response but no URL field found: {list(data.keys()) if isinstance(data, dict) else data}")
            except Exception as je:
                logger.warning(f"LinkForge: JSON parse failed: {je}")

        # ── Direct file response (non-HTML, or has Content-Disposition) ──
        if "text/html" not in ct or cd:
            return r.url

        # ── HTML page — look for direct download link ──
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if any(href.lower().endswith(ext) for ext in (
                ".txt", ".zip", ".rar", ".7z", ".tar.gz", ".gz", ".pdf",
                ".mp4", ".mkv", ".mp3", ".exe", ".apk", ".iso",
            )):
                return href if href.startswith("http") else urljoin(r.url, href)

        # ── Meta-refresh ──
        meta = soup.find("meta", {"http-equiv": re.compile(r"refresh", re.I)})
        if meta:
            content = meta.get("content", "")
            target = re.search(r"url=(.+)", content, re.I)
            if target:
                loc = target.group(1).strip().strip("'\"")
                return loc if loc.startswith("http") else urljoin(r.url, loc)

        # Last resort: return the final URL and let the downloader try
        return r.url
    except Exception as e:
        logger.warning(f"LinkForge resolve failed: {e}")
    return None


def resolve_flowbit(url: str) -> Optional[str]:
    """
    Resolve flowbit-*.herokuapp.com/dl/<id>/<filename>?code=... direct download links.
    These are Heroku-hosted file servers that return the file directly or via redirect,
    sometimes requiring a valid Referer or the ?code= query parameter.
    """
    scraper = cloudscraper.create_scraper()
    try:
        # Strategy 1: Try with Accept header that signals we want the raw file, not HTML
        headers_variants = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": url,
            },
            {
                "User-Agent": "python-requests/2.31.0",
                "Accept": "*/*",
                "Referer": url,
            },
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/octet-stream, */*",
                "Referer": url,
            },
        ]

        for hdrs in headers_variants:
            r = scraper.get(url, allow_redirects=True, timeout=25, headers=hdrs)
            ct = r.headers.get("Content-Type", "")
            cd = r.headers.get("Content-Disposition", "")

            # If response is NOT html, or has Content-Disposition → it's the file
            if "text/html" not in ct or cd:
                logger.info(f"Flowbit: got file directly (ct={ct}, cd={cd})")
                return r.url

            # Got HTML — try to extract a direct download link from the page
            soup = BeautifulSoup(r.text, "html.parser")

            # Look for <a> tags with download-related hrefs
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if any(kw in href.lower() for kw in ("download", "/dl/", "?code=", "file=")):
                    resolved = href if href.startswith("http") else urljoin(r.url, href)
                    logger.info(f"Flowbit: found link in HTML: {resolved}")
                    return resolved

            # Look for meta-refresh redirect
            meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
            if meta and meta.get("content"):
                m = re.search(r"url=(.+)", meta["content"], re.I)
                if m:
                    redirect_url = m.group(1).strip().strip("'\"")
                    if not redirect_url.startswith("http"):
                        redirect_url = urljoin(r.url, redirect_url)
                    logger.info(f"Flowbit: meta-refresh to {redirect_url}")
                    return redirect_url

            # Look for JS window.location or direct URL patterns
            for script in soup.find_all("script"):
                text = script.string or ""
                js_m = re.search(
                    r"""(?:window\.location|location\.href)\s*=\s*['"]([^'"]+)['"]""",
                    text,
                )
                if js_m:
                    redirect_url = js_m.group(1)
                    if not redirect_url.startswith("http"):
                        redirect_url = urljoin(r.url, redirect_url)
                    logger.info(f"Flowbit: JS redirect to {redirect_url}")
                    return redirect_url

            logger.warning(f"Flowbit: HTML page returned but no download link found (attempt with {hdrs.get('User-Agent','?')[:30]})")

        # All strategies returned HTML. Do NOT return r.url as it would download HTML.
        logger.warning("Flowbit: all header variants returned HTML — cannot resolve direct link")
        return None

    except Exception as e:
        logger.warning(f"Flowbit resolve failed: {e}")
    return None


def resolve_telegram_file(url: str) -> Optional[str]:
    """
    Handle Telegram-related direct download URLs:
      • cdn*.telegram.org / cdn-telegram.org  — CDN direct file links (already direct)
      • t.me/c/<channel_id>/<msg_id>           — channel message links (cannot resolve
        without Bot API / user session; inform user)
      • t.me/<username>/<msg_id>               — public channel message links (same)

    For CDN links we just return the URL as-is (they are already direct).
    For t.me links we return a descriptive error so the user is informed.
    """
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    # Direct Telegram CDN links — serve files directly
    if re.search(r"cdn\d*[\.-]telegram\.(org|com)", host) or "cdn-telegram.org" in host:
        return url   # already a direct link; downloader handles it

    # t.me message links — require Bot API with the file's file_id, not a URL
    if "t.me" in host:
        return None  # signal that we can't resolve this type

    return None


def resolve_generic_page(url: str) -> Optional[str]:
    """
    Generic smart extractor for unknown hosting pages.
    Tries multiple strategies:
    1. Direct file links in <a href>
    2. Meta-refresh redirect
    3. JS window.location / window.open patterns
    4. og:url / canonical link meta tags pointing to files
    """
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=20, allow_redirects=True)
        # If the final URL is already a file, return it
        ct = r.headers.get("Content-Type", "")
        cd = r.headers.get("Content-Disposition", "")
        if "text/html" not in ct or cd:
            return r.url

        html = r.text
        parsed = urlparse(r.url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: direct file links
        ext_re = re.compile(
            r"\.(zip|rar|7z|tar\.gz|gz|bz2|xz|pdf|mp4|mkv|avi|mov|mp3|"
            r"flac|wav|exe|apk|iso|dmg|docx?|xlsx?|pptx?|torrent|bin|img)\b",
            re.I,
        )
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ext_re.search(href):
                return href if href.startswith("http") else urljoin(r.url, href)

        # Strategy 2: meta-refresh
        meta = soup.find("meta", {"http-equiv": re.compile(r"refresh", re.I)})
        if meta:
            content = meta.get("content", "")
            target = re.search(r"url=(.+)", content, re.I)
            if target:
                loc = target.group(1).strip().strip("'\"")
                if ext_re.search(loc):
                    return loc if loc.startswith("http") else urljoin(r.url, loc)

        # Strategy 3: JS patterns
        js_patterns = [
            r'window\.location(?:\.href)?\s*=\s*[\'"]([^\'"]+\.(?:zip|rar|7z|mp4|pdf|apk)[^\'"]*)[\'"]',
            r'window\.open\([\'"]([^\'"]+\.(?:zip|rar|7z|mp4|pdf|apk)[^\'"]*)[\'"]',
            r'location\.replace\([\'"]([^\'"]+)[\'"]',
            r'"downloadUrl"\s*:\s*"([^"]+)"',
            r'"download_url"\s*:\s*"([^"]+)"',
            r'"url"\s*:\s*"(https?://[^"]+\.(?:zip|rar|mp4|pdf|apk)[^"]*)"',
        ]
        for pattern in js_patterns:
            js_m = re.search(pattern, html, re.I)
            if js_m:
                loc = js_m.group(1).replace("\\/", "/")
                return loc if loc.startswith("http") else urljoin(r.url, loc)

        # Strategy 4: og:url / canonical
        for selector in [
            {"property": "og:url"},
            {"name": "twitter:url"},
            {"rel": "canonical"},
        ]:
            tag = soup.find("meta", selector) or soup.find("link", selector)
            if tag:
                href = tag.get("content") or tag.get("href", "")
                if href and ext_re.search(href):
                    return href

    except Exception as e:
        logger.warning(f"Generic resolve failed: {e}")
    return None


def page_asks_for_password(html: str) -> bool:
    low = html.lower()
    has_pw = bool(re.search(r'<input[^>]+type=["\']?password', low))
    score = sum(1 for h in PASSWORD_HINTS if h in low)
    return has_pw or score >= 2


def extract_filename(url: str, headers: dict, hint: str = None) -> str:
    if hint:
        return sanitize_filename(hint)
    cd = headers.get("Content-Disposition", "")
    if "filename*=" in cd:
        m = re.search(r"filename\*=(?:UTF-8'')?([^\s;]+)", cd, re.I)
        if m:
            return sanitize_filename(unquote(m.group(1)))
    if "filename=" in cd:
        fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
        if fname:
            return sanitize_filename(fname)
    path = unquote(urlparse(url).path)
    name = Path(path).name
    if name and "." in name:
        return sanitize_filename(name)
    ct = headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
    ext = mimetypes.guess_extension(ct) or ".bin"
    return f"download{ext}"


def smart_resolve(url: str) -> dict:
    result = dict(
        direct_url=None, needs_password=False,
        password_form=None, original_url=url,
        filename_hint=None, error=None,
    )
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    try:
        # ── Known hosting services ──────────────────────────────────
        if "drive.google.com" in host or "drive.usercontent.google.com" in host:
            result["direct_url"] = resolve_google_drive(url)
            return result

        if "dropbox.com" in host:
            result["direct_url"] = resolve_dropbox(url)
            return result

        if "onedrive.live.com" in host or "1drv.ms" in host or "sharepoint.com" in host:
            result["direct_url"] = resolve_onedrive(url)
            return result

        if "mediafire.com" in host:
            direct = resolve_mediafire(url)
            result["direct_url"] = direct
            if not direct:
                result["error"] = "Could not extract MediaFire link."
            return result

        if any(h in host for h in ("terabox.com", "freeterabox.com", "1024tera.com", "terabox.app")):
            direct = resolve_terabox(url)
            result["direct_url"] = direct
            if not direct:
                result["error"] = "Could not resolve Terabox link."
            return result

        if "gofile.io" in host:
            direct = resolve_gofile(url)
            result["direct_url"] = direct
            if not direct:
                result["error"] = "Could not resolve Gofile link."
            return result

        # ── LinkForge CDN (cdn2.linkforge.xyz and any *.linkforge.xyz) ──
        if "linkforge.xyz" in host:
            direct = resolve_linkforge(url)
            result["direct_url"] = direct
            if not direct:
                result["error"] = "Could not resolve LinkForge link."
            return result

        # ── Flowbit Heroku direct download links ─────────────────────
        if "flowbit" in host and "herokuapp.com" in host:
            direct = resolve_flowbit(url)
            result["direct_url"] = direct
            if not direct:
                result["error"] = (
                    "⚠️ Could not resolve Flowbit download link.\n\n"
                    "The server returned an HTML page instead of the actual file. "
                    "The download link may have expired or the ?code= token is invalid.\n\n"
                    "Please generate a fresh download link and try again."
                )
            return result

        # ── Telegram CDN / t.me links ─────────────────────────────────
        if "t.me" in host or re.search(r"cdn\d*[\.-]telegram\.(org|com)", host) or "cdn-telegram.org" in host:
            direct = resolve_telegram_file(url)
            if direct:
                result["direct_url"] = direct
                return result
            result["error"] = (
                "⚠️ *Telegram message links (t.me/…) cannot be downloaded as URLs.*\n\n"
                "To download a file from Telegram:\n"
                "1️⃣ Forward the message containing the file *directly to this bot*.\n"
                "2️⃣ Or use a Telegram client to download it first, then send the file here.\n\n"
                "ℹ️ Telegram CDN links (`cdn*.telegram.org`) *are* supported — just paste the direct CDN URL."
            )
            return result

        if "mega.nz" in host or "mega.co.nz" in host:
            result["error"] = (
                "⚠️ MEGA links require the `megatools` CLI.\n"
                "Install: https://megatools.megous.com\n"
                f"Then run: `megadl '{url}'`"
            )
            return result

        # ── Generic: HEAD probe first ───────────────────────────────
        scraper = cloudscraper.create_scraper()
        head = scraper.head(url, allow_redirects=True, timeout=15)
        ct   = head.headers.get("Content-Type", "")
        cd   = head.headers.get("Content-Disposition", "")

        if "text/html" not in ct or cd:
            result["direct_url"] = head.url
            if "filename=" in cd:
                fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
                result["filename_hint"] = fname
            return result

        if head.status_code == 401:
            result["needs_password"] = True
            return result

        # ── Full page fetch ─────────────────────────────────────────
        r = scraper.get(url, timeout=20)
        if page_asks_for_password(r.text):
            result["needs_password"] = True
            soup = BeautifulSoup(r.text, "html.parser")
            form = soup.find("form")
            if form:
                action = form.get("action") or url
                if not action.startswith("http"):
                    action = f"{parsed.scheme}://{parsed.netloc}{action}"
                fields = {
                    i.get("name"): i.get("value", "")
                    for i in form.find_all("input")
                    if i.get("name") and i.get("type") != "password"
                }
                result["password_form"] = {"action": action, "fields": fields}
            return result

        # ── Generic page smart resolver ─────────────────────────────
        generic = resolve_generic_page(url)
        if generic:
            result["direct_url"] = generic
            return result

        result["error"] = "⚠️ No direct download link found. Try sending the actual file URL."

    except Exception as e:
        logger.exception("smart_resolve error")
        result["error"] = f"Link analysis failed: {e}"

    return result


def resolve_with_password(info: dict, password: str) -> Optional[str]:
    pf = info.get("password_form")
    if not pf:
        return None
    scraper = cloudscraper.create_scraper()
    try:
        r0 = scraper.get(info["original_url"], timeout=15)
        soup = BeautifulSoup(r0.text, "html.parser")
        form = soup.find("form")
        fields = dict(pf["fields"])
        pw_input = soup.find("input", {"type": "password"})
        pw_name = pw_input.get("name", "password") if pw_input else "password"
        if form:
            for inp in form.find_all("input"):
                n = inp.get("name")
                if n and inp.get("type") not in ("password", "submit", "button"):
                    fields[n] = inp.get("value", "")
        fields[pw_name] = password
    except Exception:
        fields = dict(pf["fields"])
        fields["password"] = password

    r = scraper.post(pf["action"], data=fields, allow_redirects=True, timeout=20)
    ct = r.headers.get("Content-Type", "")
    cd = r.headers.get("Content-Disposition", "")
    if "text/html" not in ct or cd:
        return r.url
    if page_asks_for_password(r.text):
        return None
    parsed = urlparse(pf["action"])
    soup2 = BeautifulSoup(r.text, "html.parser")
    ext_re = re.compile(r"\.(zip|rar|7z|tar\.gz|gz|pdf|mp4|mkv|mp3|exe|apk|iso|dmg)\b", re.I)
    for a in soup2.find_all("a", href=True):
        href = a["href"]
        if ext_re.search(href):
            if not href.startswith("http"):
                href = f"{parsed.scheme}://{parsed.netloc}{href}"
            return href
    return r.url


def _validate_file_type(path: Path) -> tuple[bool, str]:
    """
    Validate that the downloaded file is an allowed type.
    Allowed: .txt files, and .zip files that contain at least one .txt file.
    Returns (is_valid, error_message).
    """
    import zipfile
    suffix = path.suffix.lower()

    if suffix == ".txt":
        return True, ""

    if suffix == ".zip":
        try:
            with zipfile.ZipFile(path, "r") as zf:
                names = zf.namelist()
                has_txt = any(n.lower().endswith(".txt") for n in names)
                if has_txt:
                    return True, ""
                else:
                    return False, (
                        "❌ *File rejected:* The ZIP archive does not contain any `.txt` files.\n\n"
                        "This bot only accepts `.txt` files and ZIP archives that contain `.txt` files."
                    )
        except zipfile.BadZipFile:
            return False, "❌ *File rejected:* The file appears to be a corrupt or invalid ZIP archive."
        except Exception as e:
            return False, f"❌ *File rejected:* Could not inspect ZIP contents: {e}"

    # Any other extension is rejected
    return False, (
        f"❌ *File rejected:* `{path.name}`\n\n"
        f"This bot only downloads `.txt` files and `.zip` archives containing `.txt` files.\n"
        f"The downloaded file has an unsupported type: `{suffix or '(no extension)'}`"
    )





# ─────────────────────────────────────────────────────────────
#  ⚡  PARALLEL CHUNKED DOWNLOADER
# ─────────────────────────────────────────────────────────────

class DownloadState:
    def __init__(self, total: int, user_id: int = 0):
        self.total = total
        self.downloaded = 0
        self.lock = asyncio.Lock()
        self.start_time = time.time()
        self.speed_samples: list[tuple[float, int]] = []
        self.peak_speed: float = 0.0
        self.user_id = user_id
        # Throttle state (token bucket per-state, shared across segments)
        self._throttle_lock = asyncio.Lock()
        self._tokens: float = float(current_speed_cap())
        self._last_refill: float = time.monotonic()
        # Pause support
        if user_id not in _pause_events:
            _pause_events[user_id] = asyncio.Event()
        _pause_events[user_id].set()   # start in running state

    async def add(self, n: int):
        async with self.lock:
            self.downloaded += n
            now = time.time()
            self.speed_samples.append((now, n))
            cutoff = now - 5
            self.speed_samples = [(t, b) for t, b in self.speed_samples if t >= cutoff]
            current_spd = self.speed_bps()
            if current_spd > self.peak_speed:
                self.peak_speed = current_spd

    async def throttle(self, n: int):
        """Wait for pause to clear, then optionally apply token-bucket rate limit.
        If MAX_SPEED_BPS == 0, this is a no-op (unlimited speed)."""
        # Pause gate: block here while paused
        if self.user_id in _pause_events:
            await _pause_events[self.user_id].wait()
        cap = current_speed_cap()
        if cap <= 0:
            return   # unlimited: skip all throttle logic
        async with self._throttle_lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(cap), self._tokens + elapsed * cap)
            self._last_refill = now
            if self._tokens >= n:
                self._tokens -= n
            else:
                deficit = n - self._tokens
                self._tokens = 0
                wait = deficit / cap
                await asyncio.sleep(wait)

    def speed_bps(self) -> float:
        if len(self.speed_samples) < 2:
            elapsed = time.time() - self.start_time
            return self.downloaded / elapsed if elapsed > 0 else 0
        window_bytes = sum(b for _, b in self.speed_samples)
        oldest = self.speed_samples[0][0]
        newest = self.speed_samples[-1][0]
        return window_bytes / (newest - oldest or 1)

    def avg_speed_bps(self) -> float:
        elapsed = time.time() - self.start_time
        return self.downloaded / elapsed if elapsed > 0 else 0

    def elapsed_str(self) -> str:
        secs = int(time.time() - self.start_time)
        m, s = divmod(secs, 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h}h{m:02d}m{s:02d}s"
        if m:
            return f"{m}m{s:02d}s"
        return f"{s}s"

    def eta(self) -> float:
        remaining = self.total - self.downloaded
        spd = self.speed_bps()
        return remaining / spd if spd > 0 else float("inf")


async def _download_segment(
    session: aiohttp.ClientSession,
    url: str,
    start: int,
    end: int,
    dest: Path,
    state: DownloadState,
    semaphore: asyncio.Semaphore,
    retries: int = MAX_RETRIES,
):
    headers = {"Range": f"bytes={start}-{end}"}
    for attempt in range(1, retries + 1):
        try:
            async with semaphore:
                async with session.get(url, headers=headers, timeout=CONNECT_TIMEOUT) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(dest, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            await state.throttle(len(chunk))
                            await f.write(chunk)
                            await state.add(len(chunk))
                    return
        except Exception as e:
            if attempt == retries:
                raise RuntimeError(f"Segment {start}-{end} failed after {retries} retries: {e}")
            await asyncio.sleep(2 ** attempt)


async def _merge_segments(seg_files: list[Path], out_path: Path):
    async with aiofiles.open(out_path, "wb") as out:
        for seg in seg_files:
            async with aiofiles.open(seg, "rb") as inp:
                while True:
                    data = await inp.read(16 * 1024 * 1024)
                    if not data:
                        break
                    await out.write(data)
            seg.unlink()


async def _progress_loop(state: DownloadState, status_msg, filename: str, workers: int):
    while state.downloaded < state.total or state.total == 0:
        try:
            paused = state.user_id in _pause_events and not _pause_events[state.user_id].is_set()
            bar = progress_bar(state.downloaded, state.total)
            pct = f"{state.downloaded / state.total * 100:.1f}%" if state.total else "?%"
            size_str = fmt_size(state.downloaded) + (f" / {fmt_size(state.total)}" if state.total else "")
            cur_spd  = state.speed_bps()
            avg_spd  = state.avg_speed_bps()
            peak_spd = state.peak_speed
            eta      = fmt_eta(state.eta()) if state.total else "?"
            elapsed  = state.elapsed_str()
            cap      = current_speed_cap()
            cap_note = f" _(cap: {fmt_speed(cap)})_" if cap > 0 else " _(unlimited)_"
            status_icon = "⏸ *PAUSED*" if paused else f"⚡ *Downloading* ×{workers} streams"
            await status_msg.edit_text(
                f"{status_icon}\n"
                f"`[{bar}]` {pct}\n"
                f"📦 {size_str}\n"
                f"🚀 *Now:* {fmt_speed(cur_spd)}{cap_note}\n"
                f"📈 *Peak:* {fmt_speed(peak_spd)}  |  📊 *Avg:* {fmt_speed(avg_spd)}\n"
                f"⏱ *ETA:* {eta}  |  🕐 *Elapsed:* {elapsed}\n"
                f"📄 `{filename}`"
                + ("\n\n_Send /resume to continue_" if paused else ""),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        await asyncio.sleep(PROGRESS_EVERY)


async def parallel_download(
    url: str,
    dest_dir: Path,
    status_msg,
    filename_hint: str = None,
    auth: str = None,
    user_id: int = 0,
) -> list[Path]:
    req_headers: dict = {}
    if auth:
        import base64
        creds = base64.b64encode(f":{auth}".encode()).decode()
        req_headers["Authorization"] = f"Basic {creds}"

    req_headers.setdefault("User-Agent", "Mozilla/5.0 (compatible; TGDownloadBot/6.0)")

    connector = aiohttp.TCPConnector(limit=PARALLEL_WORKERS + 8, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, headers=req_headers) as session:
        async with session.head(url, allow_redirects=True, timeout=CONNECT_TIMEOUT) as head:
            total = int(head.headers.get("Content-Length", 0))
            accepts_ranges = head.headers.get("Accept-Ranges", "").lower() == "bytes"
            filename = extract_filename(str(head.url), dict(head.headers), filename_hint)
            url = str(head.url)

    out_path = dest_dir / filename
    state = DownloadState(total, user_id=user_id)

    use_parallel = accepts_ranges and total > SEGMENT_SIZE and PARALLEL_WORKERS > 1
    workers = PARALLEL_WORKERS if use_parallel else 1

    progress_task = asyncio.create_task(_progress_loop(state, status_msg, filename, workers))

    try:
        if use_parallel:
            num_segments = math.ceil(total / SEGMENT_SIZE)
            seg_dir = dest_dir / f"_segs_{filename}"
            seg_dir.mkdir(exist_ok=True)
            seg_files: list[Path] = []
            tasks = []
            semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
            connector2 = aiohttp.TCPConnector(limit=PARALLEL_WORKERS + 8, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector2, headers=req_headers) as session2:
                for i in range(num_segments):
                    start = i * SEGMENT_SIZE
                    end = min(start + SEGMENT_SIZE - 1, total - 1)
                    seg_path = seg_dir / f"seg_{i:06d}.tmp"
                    seg_files.append(seg_path)
                    tasks.append(
                        _download_segment(session2, url, start, end, seg_path, state, semaphore)
                    )
                await asyncio.gather(*tasks)
            await _merge_segments(seg_files, out_path)
            seg_dir.rmdir()
        else:
            connector3 = aiohttp.TCPConnector(ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector3, headers=req_headers) as session3:
                async with session3.get(url, timeout=CONNECT_TIMEOUT) as resp:
                    resp.raise_for_status()
                    if not total:
                        state.total = int(resp.headers.get("Content-Length", 0))
                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            await state.throttle(len(chunk))
                            await f.write(chunk)
                            await state.add(len(chunk))
    finally:
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

    file_size = out_path.stat().st_size

    # ── HTML content guard ──────────────────────────────────────────────────
    # If the server silently returned an HTML page (e.g. expired link, CAPTCHA,
    # or a download-page instead of the actual file), abort early with a clear error
    # rather than sending the user an HTML file disguised as their requested file.
    try:
        with open(out_path, "rb") as _f:
            _preview = _f.read(512).lstrip()
        if _preview.lower().startswith((b"<!doctype html", b"<html")):
            out_path.unlink(missing_ok=True)
            raise ValueError(
                "⚠️ The server returned an HTML page instead of the actual file.\n\n"
                "This usually means the download link has *expired* or requires a fresh session.\n"
                "Please get a new download link and try again."
            )
    except ValueError:
        raise
    except Exception:
        pass  # If we can't read the preview, proceed normally
    # ───────────────────────────────────────────────────────────────────────

    if file_size > SPLIT_SIZE:
        await status_msg.edit_text(
            f"✂️ Splitting large file into parts…\n📦 {fmt_size(file_size)}",
            parse_mode=ParseMode.MARKDOWN,
        )
        parts = await asyncio.get_event_loop().run_in_executor(
            None, _split_file_sync, out_path, dest_dir
        )
        return parts

    return [out_path]


def _split_file_sync(path: Path, dest_dir: Path) -> list[Path]:
    parts = []
    idx = 0
    with open(path, "rb") as src:
        while True:
            data = src.read(SPLIT_SIZE)
            if not data:
                break
            part = dest_dir / f"{path.stem}.part{idx:03d}{path.suffix}"
            part.write_bytes(data)
            parts.append(part)
            idx += 1
    path.unlink()
    return parts


async def upload_parts(parts: list[Path], update: Update, display_name: str, user_id: int,
                       url: str = "", elapsed: float = 0.0):
    total_parts = len(parts)
    total_bytes = 0
    for i, part in enumerate(parts, 1):
        size = part.stat().st_size
        total_bytes += size
        label = f" *(part {i}/{total_parts})*" if total_parts > 1 else ""
        caption = f"✅ *{display_name}*{label}\n📦 {fmt_size(size)}"
        with open(part, "rb") as fh:
            await update.message.reply_document(
                document=fh,
                filename=part.name,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
            )
    record_download(user_id, display_name, total_bytes)
    record_history(user_id, display_name, total_bytes, url, elapsed)


# ─────────────────────────────────────────────────────────────
#  YT-DLP FALLBACK
# ─────────────────────────────────────────────────────────────

async def try_ytdlp(url: str, update: Update, status_msg) -> bool:
    try:
        import yt_dlp
    except ImportError:
        return False

    user_id = update.effective_user.id
    dest_dir = DOWNLOAD_DIR / str(user_id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_tpl = str(dest_dir / "%(title).60s.%(ext)s")

    await status_msg.edit_text("🎬 Media site detected — downloading via yt-dlp…")
    try:
        loop = asyncio.get_event_loop()

        def _run():
            opts = {
                "outtmpl": out_tpl,
                "quiet": True,
                "no_warnings": True,
                "merge_output_format": "mp4",
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "concurrent_fragment_downloads": PARALLEL_WORKERS,
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])

        await loop.run_in_executor(None, _run)
        files = list(dest_dir.iterdir())
        if not files:
            await status_msg.edit_text("❌ yt-dlp produced no output.")
            return True

        for f in files:
            size = f.stat().st_size
            await status_msg.edit_text(
                f"📤 Uploading *{f.name}* ({fmt_size(size)})…",
                parse_mode=ParseMode.MARKDOWN,
            )
            if size <= SPLIT_SIZE:
                with open(f, "rb") as fh:
                    await update.message.reply_document(
                        document=fh,
                        filename=f.name,
                        caption=f"✅ *{f.name}*\n📦 {fmt_size(size)}",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                record_download(user_id, f.name, size)
            else:
                parts = await asyncio.get_event_loop().run_in_executor(
                    None, _split_file_sync, f, dest_dir
                )
                await upload_parts(parts, update, f.name, user_id)

        await status_msg.delete()
    except Exception as e:
        logger.exception("yt-dlp error")
        await status_msg.edit_text(f"❌ yt-dlp failed: {e}")
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
    return True


# ─────────────────────────────────────────────────────────────
#  ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

async def do_download(
    url: str,
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    status_msg,
    filename_hint: str = None,
    auth: str = None,
):
    user = update.effective_user
    dest_dir = DOWNLOAD_DIR / str(user.id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    t_start = time.time()

    try:
        await status_msg.edit_text("⚡ Connecting…")
        parts = await parallel_download(
            url=url,
            dest_dir=dest_dir,
            status_msg=status_msg,
            filename_hint=filename_hint,
            auth=auth,
            user_id=user.id,
        )
        elapsed = time.time() - t_start
        display_name = re.sub(r"\.part\d+", "", parts[0].name)
        total_size = sum(p.stat().st_size for p in parts)

        # ── File type validation ─────────────────────────────────────
        is_valid, type_error = _validate_file_type(parts[0])
        if not is_valid:
            for p in parts:
                p.unlink(missing_ok=True)
            await status_msg.edit_text(type_error, parse_mode=ParseMode.MARKDOWN)
            return
        # ────────────────────────────────────────────────────────────

        await status_msg.edit_text(
            f"📤 *Uploading* `{display_name}` ({fmt_size(total_size)})…",
            parse_mode=ParseMode.MARKDOWN,
        )
        await upload_parts(parts, update, display_name, user.id, url=url, elapsed=elapsed)
        await status_msg.delete()

        # Auto-notify if download took longer than threshold
        if elapsed >= NOTIFY_THRESHOLD:
            try:
                await ctx.bot.send_message(
                    user.id,
                    f"🔔 *Download complete!*\n"
                    f"📄 `{display_name}`\n"
                    f"💾 {fmt_size(total_size)}\n"
                    f"⏱ Took {fmt_eta(elapsed)}",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass

    except Exception as e:
        logger.exception("do_download error")
        await status_msg.edit_text(f"❌ Download failed:\n{e}")
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
        # Clean up pause event
        _pause_events.pop(user.id, None)
        ctx.user_data.clear()


# ─────────────────────────────────────────────────────────────
#  BOT COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized(user.id):
        await update.message.reply_text(
            "🔒 *Access Denied*\n\nThis bot is private.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    owner_badge = " 👑 *Owner*" if is_owner(user.id) else ""
    await update.message.reply_text(
        f"⚡ *Smart Download Bot v6.0*{owner_badge}\n\n"
        f"Welcome, {user.first_name}!\n\n"
        f"🚀 *{PARALLEL_WORKERS} parallel streams* · Speed cap: *{fmt_speed(MAX_SPEED_BPS)}*\n\n"
        "📄 *Supported file types:*\n"
        "  • `.txt` files — downloaded directly\n"
        "  • `.zip` archives — only if they contain `.txt` files\n\n"
        "🌐 Direct links · LinkForge CDN · Flowbit\n"
        "☁️ Google Drive · Dropbox · OneDrive · MediaFire · Terabox · Gofile\n"
        "🔐 Password-protected pages *(I'll ask you!)*\n"
        "📂 No size limit *(big files auto-split)*\n\n"
        "*Commands:*\n"
        "_/queue · /pause · /resume · /history · /stats · /delete_\n"
        "_/scheduler · /keyword · /workers · /help · /cancel_\n\n"
        "💡 Paste multiple URLs (one per line) to batch download them all!",
        parse_mode=ParseMode.MARKDOWN,
    )


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        return
    owner_cmds = ""
    if is_owner(update.effective_user.id):
        owner_cmds = (
            "\n*👑 Owner Commands:*\n"
            "• /adduser `<id>` — Whitelist a user\n"
            "• /removeuser `<id>` — Remove a user\n"
            "• /users — List whitelisted users\n"
        )
    await update.message.reply_text(
        "📖 *Help — Smart Download Bot v6.0*\n\n"
        "*📥 Download:*\n"
        "• Paste any URL — auto-detects & downloads\n"
        "• Paste multiple URLs (one per line) — batch queue\n\n"
        "*Commands:*\n"
        "• /start      — Welcome message\n"
        "• /help       — This screen\n"
        "• /stats      — Your download stats\n"
        "• /history    — 📜 Full download history (paginated)\n"
        "• /queue      — 📋 View pending batch queue\n"
        "• /pause      — ⏸ Pause active download\n"
        "• /resume     — ▶️ Resume paused download\n"
        "• /delete     — 🗑 Delete all your downloaded files\n"
        "• /scheduler  — 🌙 View bandwidth schedule & current cap\n"
        f"• /workers    — ⚙️ Show parallel workers ({PARALLEL_WORKERS})\n"
        "• /keyword    — 🔍 Keyword extractor\n"
        "• /cancel     — Cancel current operation\n"
        + owner_cmds +
        "\n*⚡ Speed & Workers:*\n"
        f"• Workers: *{PARALLEL_WORKERS}* parallel streams\n"
        f"• Speed cap: *{fmt_speed(MAX_SPEED_BPS)}* — full speed 24/7 🚀\n"
        "• Progress: current · peak · avg speed, ETA & elapsed\n\n"
        "*🔔 Auto-notify:*\n"
        f"• DM sent when download finishes & took >{NOTIFY_THRESHOLD}s\n\n"
        "*📂 Large files (> 1.9 GB):*\n"
        "Auto-split into parts. Rejoin with:\n"
        "`cat file.part000 file.part001 > original.zip`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def workers_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        return
    await update.message.reply_text(
        f"⚙️ *Parallel workers:* `{PARALLEL_WORKERS}`\n"
        f"📦 Segment size: `{fmt_size(SEGMENT_SIZE)}`\n"
        f"🚀 Speed cap: `{fmt_speed(MAX_SPEED_BPS)}` _(set `MAX_SPEED_MBS` env var)_\n\n"
        "Change workers with: `export DL_WORKERS=16`",
        parse_mode=ParseMode.MARKDOWN,
    )


async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        return
    uid = str(update.effective_user.id)
    stats = load_stats()
    user_stats = stats.get(uid)
    if not user_stats:
        await update.message.reply_text("📊 No downloads yet.")
        return
    recent = user_stats.get("files", [])[-5:]
    recent_text = "\n".join(
        f"  • `{f['name']}` — {fmt_size(f['size'])}" for f in reversed(recent)
    ) or "  None"
    await update.message.reply_text(
        f"📊 *Your Download Stats*\n\n"
        f"🔢 Total downloads: *{user_stats['count']}*\n"
        f"💾 Total data: *{fmt_size(user_stats['total_bytes'])}*\n\n"
        f"📄 *Recent (last 5):*\n{recent_text}",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def add_user_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Usage: `/adduser <user_id>`", parse_mode=ParseMode.MARKDOWN)
        return
    uid = int(args[0])
    wl = load_whitelist()
    wl.add(uid)
    save_whitelist(wl)
    await update.message.reply_text(
        f"✅ User `{uid}` added to whitelist.\nTotal users: {len(wl)}",
        parse_mode=ParseMode.MARKDOWN,
    )
    # Notify the new user
    try:
        await ctx.bot.send_message(uid, "✅ You've been granted access to the downloader bot! Send /start to begin.")
    except Exception:
        pass


@owner_only
async def remove_user_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Usage: `/removeuser <user_id>`", parse_mode=ParseMode.MARKDOWN)
        return
    uid = int(args[0])
    if uid == OWNER_ID:
        await update.message.reply_text("❌ Cannot remove the owner from the whitelist.")
        return
    wl = load_whitelist()
    wl.discard(uid)
    save_whitelist(wl)
    await update.message.reply_text(
        f"✅ User `{uid}` removed.\nTotal users: {len(wl)}",
        parse_mode=ParseMode.MARKDOWN,
    )


@owner_only
async def list_users_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    wl = load_whitelist()
    lines = [f"  • `{uid}`{'  👑 owner' if uid == OWNER_ID else ''}" for uid in sorted(wl)]
    await update.message.reply_text(
        f"👥 *Whitelisted Users ({len(wl)}):*\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
    )


async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


@authorized_only
async def delete_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete all downloaded files for the calling user."""
    user = update.effective_user
    user_dir = DOWNLOAD_DIR / str(user.id)
    if not user_dir.exists():
        await update.message.reply_text(
            "🗑 *Nothing to delete* — your download folder is already empty.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Count files and total size before deleting
    files = list(user_dir.rglob("*"))
    file_list = [f for f in files if f.is_file()]
    total_size = sum(f.stat().st_size for f in file_list)

    shutil.rmtree(user_dir, ignore_errors=True)

    await update.message.reply_text(
        f"🗑 *Downloads Cleared!*\n\n"
        f"🗂 Files deleted: *{len(file_list)}*\n"
        f"💾 Space freed: *{fmt_size(total_size)}*\n\n"
        f"_Your download folder has been wiped._",
        parse_mode=ParseMode.MARKDOWN,
    )


# ─────────────────────────────────────────────────────────────
#  DOWNLOAD CONVERSATION HANDLERS
# ─────────────────────────────────────────────────────────────

async def _do_single_queued(
    url: str,
    idx: int,
    total: int,
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
) -> dict:
    """
    Download one URL from a queue. Returns a result dict:
      { url, ok, filename, size, error }
    """
    label = f"[{idx}/{total}] " if total > 1 else ""
    status = await update.message.reply_text(
        f"🔍 {label}Analysing link…\n`{url}`",
        parse_mode=ParseMode.MARKDOWN,
    )

    info = smart_resolve(url)

    if info.get("error"):
        # Only try yt-dlp for actual media/video sites — NOT for direct download
        # links, Telegram bot links (t.me), or CDN links that just failed
        parsed_url = urlparse(url)
        _host = parsed_url.netloc.lower()
        _is_media_site = any(h in _host for h in (
            "youtube.com", "youtu.be", "vimeo.com", "dailymotion.com",
            "twitch.tv", "tiktok.com", "instagram.com", "twitter.com",
            "x.com", "facebook.com", "fb.com", "soundcloud.com",
            "bandcamp.com", "bilibili.com", "nicovideo.jp",
        ))
        if _is_media_site:
            success = await try_ytdlp(url, update, status)
            if not success:
                await status.edit_text(f"❌ {label}{info['error']}")
                return {"url": url, "ok": False, "error": info["error"]}
            return {"url": url, "ok": True, "filename": "media", "size": 0}
        else:
            # Not a media site — skip yt-dlp and report the real error
            await status.edit_text(f"❌ {label}{info['error']}", parse_mode=ParseMode.MARKDOWN)
            return {"url": url, "ok": False, "error": info["error"]}

    if info.get("needs_password"):
        # For multi-URL queues we skip password-protected links
        # (password flow still works for single URL)
        if total > 1:
            await status.edit_text(
                f"⏭ {label}Skipped — password-protected link.\n`{url}`",
                parse_mode=ParseMode.MARKDOWN,
            )
            return {"url": url, "ok": False, "error": "password required (skipped in batch)"}
        # Single-URL path — hand off to password conversation
        await status.edit_text(
            "🔐 *Password Required!*\n\nPlease reply with the password:",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["status_msg"] = status
        ctx.user_data["resolve_info"] = info
        return {"url": url, "ok": None, "needs_password": True}

    direct = info.get("direct_url") or url
    user = update.effective_user
    dest_dir = DOWNLOAD_DIR / str(user.id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    t_start = time.time()

    try:
        await status.edit_text(f"⚡ {label}Connecting…")
        parts = await parallel_download(
            url=direct,
            dest_dir=dest_dir,
            status_msg=status,
            filename_hint=info.get("filename_hint"),
            user_id=user.id,
        )
        elapsed = time.time() - t_start
        display_name = re.sub(r"\.part\d+", "", parts[0].name)
        total_size = sum(p.stat().st_size for p in parts)

        # ── File type validation ────────────────────────────────────────
        # Only allow .txt files and .zip archives containing .txt files
        is_valid, type_error = _validate_file_type(parts[0])
        if not is_valid:
            # Clean up the rejected file(s)
            for p in parts:
                p.unlink(missing_ok=True)
            await status.edit_text(type_error, parse_mode=ParseMode.MARKDOWN)
            return {"url": url, "ok": False, "error": type_error}
        # ───────────────────────────────────────────────────────────────

        await status.edit_text(
            f"📤 {label}Uploading `{display_name}` ({fmt_size(total_size)})…",
            parse_mode=ParseMode.MARKDOWN,
        )
        await upload_parts(parts, update, display_name, user.id, url=direct, elapsed=elapsed)
        await status.delete()

        # Auto-notify on long downloads (only for batch/queue mode — single URL notified in do_download)
        if total > 1 and elapsed >= NOTIFY_THRESHOLD:
            try:
                await ctx.bot.send_message(
                    user.id,
                    f"🔔 *Download complete!*\n"
                    f"📄 `{display_name}`\n"
                    f"💾 {fmt_size(total_size)}\n"
                    f"⏱ Took {fmt_eta(elapsed)}",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass

        return {"url": url, "ok": True, "filename": display_name, "size": total_size}

    except Exception as e:
        logger.exception("_do_single_queued error")
        await status.edit_text(f"❌ {label}Failed:\n{e}")
        return {"url": url, "ok": False, "error": str(e)}

    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)


@authorized_only
async def handle_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    urls = URL_RE.findall(text)

    if not urls:
        await update.message.reply_text("🔗 No URL detected. Please send one or more download links.")
        return ConversationHandler.END

    # ── Single URL — keep password-conversation flow ────────────────
    if len(urls) == 1:
        url = urls[0]
        ctx.user_data["url"] = url
        result = await _do_single_queued(url, 1, 1, update, ctx)

        if result.get("needs_password"):
            return WAITING_PASSWORD   # hand off to password handler
        return ConversationHandler.END

    # ── Multiple URLs — queue mode ──────────────────────────────────
    total = len(urls)
    header = await update.message.reply_text(
        f"📋 *Queue started:* {total} links\n\n"
        + "\n".join(f"`{i+1}.` `{u}`" for i, u in enumerate(urls)),
        parse_mode=ParseMode.MARKDOWN,
    )

    results: list[dict] = []
    for idx, url in enumerate(urls, 1):
        res = await _do_single_queued(url, idx, total, update, ctx)
        results.append(res)

    # ── Summary ─────────────────────────────────────────────────────
    ok     = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    total_bytes = sum(r.get("size", 0) for r in ok)

    lines = [f"📋 *Batch Complete — {total} links*\n"]
    lines.append(f"✅ Success: *{len(ok)}*   ❌ Failed: *{len(failed)}*")
    if total_bytes:
        lines.append(f"💾 Total downloaded: *{fmt_size(total_bytes)}*")
    if failed:
        lines.append("\n*Failed links:*")
        for r in failed:
            short = r["url"][:50] + ("…" if len(r["url"]) > 50 else "")
            lines.append(f"  • `{short}`\n    _{r.get('error','unknown error')}_")

    try:
        await header.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    ctx.user_data.clear()
    return ConversationHandler.END


@authorized_only
async def handle_password(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    info = ctx.user_data.get("resolve_info", {})
    status = ctx.user_data.get("status_msg")

    if status is None:
        status = await update.message.reply_text("🔑 Trying password…")
    else:
        await status.edit_text("🔑 Trying password…")

    direct = resolve_with_password(info, password)
    if not direct:
        await status.edit_text(
            "❌ *Wrong password.* Please try again or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_PASSWORD

    await do_download(direct, update, ctx, status, info.get("filename_hint"))
    return ConversationHandler.END


# ─────────────────────────────────────────────────────────────
#  🔍  KEYWORD EXTRACTOR ENGINE  (Enhanced v2)
# ─────────────────────────────────────────────────────────────

KW_FETCH_WORKERS = 40           # more parallel workers for speed
KW_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
KW_MAX_PAGES     = 300
KW_FUZZY_RATIO   = 0.85
KW_PROGRESS_EVERY = 2           # seconds between progress updates

# Conversation states for new 3-step flow
KW_WAITING_SITE    = 20         # step 1: site/domain to scrape
KW_WAITING_KEYWORD = 21         # step 2: keywords to search


def _normalise(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    def trigrams(s):
        return {s[i:i+3] for i in range(len(s) - 2)}
    t1, t2 = trigrams(a), trigrams(b)
    if not t1 or not t2:
        return 0.0
    return len(t1 & t2) / len(t1 | t2)


def deduplicate(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    exact_deduped: list[str] = []
    for line in lines:
        key = _normalise(line)
        if key and key not in seen:
            seen.add(key)
            exact_deduped.append(line)
    normed = [_normalise(l) for l in exact_deduped]
    keep = [True] * len(normed)
    KW_WINDOW = 200
    for i in range(1, len(normed)):
        if not keep[i]:
            continue
        for j in range(max(0, i - KW_WINDOW), i):
            if keep[j] and _similarity(normed[i], normed[j]) >= KW_FUZZY_RATIO:
                keep[i] = False
                break
    return [line for line, k in zip(exact_deduped, keep) if k]


def _extract_text_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe", "svg"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    return [raw.strip() for raw in text.splitlines() if raw.strip() and len(raw.strip()) > 2]


def _matches_keywords(line: str, keywords: list[str]) -> bool:
    low = line.lower()
    return any(kw in low for kw in keywords)


async def _fetch_page_fast(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    done_counter: list,   # mutable [int] for atomic-ish counting
) -> tuple[str, str]:
    """Fetch a single page. Returns (url, html). Updates done_counter[0]."""
    async with semaphore:
        for attempt in range(1, 3):
            try:
                async with session.get(
                    url,
                    timeout=KW_FETCH_TIMEOUT,
                    allow_redirects=True,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                ) as r:
                    if r.status == 200:
                        ct = r.headers.get("Content-Type", "")
                        if "text" in ct or "html" in ct:
                            html = await r.text(errors="replace")
                            done_counter[0] += 1
                            return url, html
                    done_counter[0] += 1
                    return url, ""
            except Exception:
                if attempt == 1:
                    await asyncio.sleep(0.5)
        done_counter[0] += 1
        return url, ""


def _discover_subpages(base_url: str, html: str, max_pages: int) -> list[str]:
    parsed = urlparse(base_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = {base_url}
    skip_exts = re.compile(
        r"\.(jpg|jpeg|png|gif|svg|webp|ico|css|js|woff|woff2|ttf|pdf|zip|mp4|mp3)$",
        re.I,
    )
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("javascript") or href.startswith("mailto"):
            continue
        if href.startswith("//"):
            href = parsed.scheme + ":" + href
        elif href.startswith("/"):
            href = base_domain + href
        elif not href.startswith("http"):
            href = base_url.rstrip("/") + "/" + href
        # Strip fragment
        href = href.split("#")[0].rstrip("?&")
        if not href:
            continue
        if skip_exts.search(urlparse(href).path):
            continue
        if urlparse(href).netloc == parsed.netloc and href not in seen:
            seen.add(href)
            links.append(href)
            if len(links) >= max_pages:
                break
    return links


async def _progress_reporter(status_msg, done: list, total: list, matches: list, start: float, stop_event: asyncio.Event):
    """Live progress updater running in parallel with scraping."""
    while not stop_event.is_set():
        try:
            d = done[0]
            t = total[0] or 1
            m = matches[0]
            pct = d / t * 100
            elapsed = time.time() - start
            rate = d / elapsed if elapsed > 0 else 0
            eta = (t - d) / rate if rate > 0 else 0
            bar_w = 16
            filled = int(bar_w * d / t)
            bar = "█" * filled + "░" * (bar_w - filled)
            await status_msg.edit_text(
                f"🔍 *Scanning pages…*\n"
                f"`[{bar}]` {pct:.1f}%\n"
                f"📄 Pages: *{d}* / *{t}*  🚀 Rate: *{rate:.1f}* p/s\n"
                f"✅ Matches so far: *{m}*\n"
                f"⏱ ETA: *{fmt_eta(eta)}*  Elapsed: *{fmt_eta(elapsed)}*",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        await asyncio.sleep(KW_PROGRESS_EVERY)


async def keyword_extract_v2(
    url: str,
    keywords: list[str],
    status_msg,
    deep: bool = True,
) -> tuple[list[str], int]:
    """
    Enhanced extractor. Returns (deduplicated_results, pages_scanned).
    Shows live progress during scraping.
    """
    kw_lower = [k.lower().strip() for k in keywords if k.strip()]
    semaphore = asyncio.Semaphore(KW_FETCH_WORKERS)
    connector = aiohttp.TCPConnector(
        limit=KW_FETCH_WORKERS + 8,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    done_counter  = [0]   # pages done
    total_counter = [1]   # total pages (updated after seed)
    match_counter = [0]   # running match count

    async with aiohttp.ClientSession(connector=connector) as session:
        # ── Step 1: fetch seed page ──────────────────────────────────
        await status_msg.edit_text(
            f"🌐 *Fetching seed page…*\n`{url}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        _, seed_html = await _fetch_page_fast(session, url, semaphore, [0])
        if not seed_html:
            return [], 0

        all_urls = [url]
        if deep:
            sub = _discover_subpages(url, seed_html, KW_MAX_PAGES - 1)
            all_urls += sub

        total_counter[0] = len(all_urls)
        done_counter[0]  = 1   # seed already done

        await status_msg.edit_text(
            f"🔍 *Found {len(all_urls)} page(s) to scan*\n"
            f"🔑 Keywords: `{', '.join(kw_lower)}`\n"
            f"⚡ Launching {KW_FETCH_WORKERS} parallel workers…",
            parse_mode=ParseMode.MARKDOWN,
        )

        # ── Step 2: fetch all sub-pages in parallel with live progress ──
        stop_event = asyncio.Event()
        progress_task = asyncio.create_task(
            _progress_reporter(status_msg, done_counter, total_counter, match_counter, time.time(), stop_event)
        )

        # Process seed matches immediately
        all_matches: list[str] = []
        for line in _extract_text_from_html(seed_html):
            if _matches_keywords(line, kw_lower):
                all_matches.append(line)
        match_counter[0] = len(all_matches)

        # Fetch remaining pages as tasks, processing each result as it completes
        if len(all_urls) > 1:
            fetch_tasks = [
                _fetch_page_fast(session, u, semaphore, done_counter)
                for u in all_urls[1:]
            ]
            for coro in asyncio.as_completed(fetch_tasks):
                _, html = await coro
                if html:
                    for line in _extract_text_from_html(html):
                        if _matches_keywords(line, kw_lower):
                            all_matches.append(line)
                    match_counter[0] = len(all_matches)

        stop_event.set()
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

    # ── Step 3: deduplicate ──────────────────────────────────────────
    await status_msg.edit_text(
        f"🧹 *Deduplicating {len(all_matches)} raw matches…*",
        parse_mode=ParseMode.MARKDOWN,
    )
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, deduplicate, all_matches)
    return results, len(all_urls)


# ─────────────────────────────────────────────────────────────
#  KEYWORD COMMAND HANDLERS  (Enhanced v2 — 3-step flow)
# ─────────────────────────────────────────────────────────────

@authorized_only
async def keyword_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Step 0 — Entry point. Ask for the site/URL to scrape."""
    ctx.user_data.clear()
    ctx.user_data["kw_mode"] = True
    await update.message.reply_text(
        "🔍 *Keyword Extractor v2*\n\n"
        "Step 1️⃣ — Which *site* do you want to search?\n\n"
        "Send a URL, for example:\n"
        "`https://garena.com`\n"
        "`https://shop.garena.com/store`\n\n"
        "I'll scan the page *and all linked sub-pages* on the same domain.\n\n"
        "_/cancel to stop_",
        parse_mode=ParseMode.MARKDOWN,
    )
    return KW_WAITING_SITE


async def kw_receive_site(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Step 1 — Receive the site URL, confirm it, ask for keywords."""
    text = update.message.text.strip()

    # Accept bare domains like "garena.com" and auto-prefix https://
    if not text.startswith("http"):
        text = "https://" + text

    match = URL_RE.search(text)
    if not match:
        await update.message.reply_text(
            "❌ *No valid URL detected.*\n\n"
            "Please send a proper URL like `https://garena.com`, or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return KW_WAITING_SITE

    url = match.group(0)
    ctx.user_data["kw_url"] = url
    parsed = urlparse(url)
    domain = parsed.netloc

    await update.message.reply_text(
        f"✅ *Site locked in:* `{domain}`\n\n"
        f"Step 2️⃣ — What *keywords* should I search for?\n\n"
        f"Send one or more keywords, comma or newline separated:\n"
        f"`price, discount, sale`\n"
        f"`free skin`\n"
        f"`event`\n\n"
        f"_/cancel to stop_",
        parse_mode=ParseMode.MARKDOWN,
    )
    return KW_WAITING_KEYWORD


async def kw_receive_keywords(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Step 2 — Receive keywords, run extraction, send results as .txt file."""
    raw = update.message.text.strip()
    keywords = [k.strip() for k in re.split(r"[,\n]+", raw) if k.strip()]
    if not keywords:
        await update.message.reply_text(
            "❌ *No keywords found.* Try again or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return KW_WAITING_KEYWORD

    url = ctx.user_data.get("kw_url", "")
    parsed = urlparse(url)
    domain = parsed.netloc

    status = await update.message.reply_text(
        f"⚡ *Extraction starting…*\n"
        f"🌐 Site: `{domain}`\n"
        f"🔑 Keywords: `{', '.join(keywords)}`\n\n"
        f"_Fetching pages…_",
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        t_start = time.time()
        results, pages_scanned = await keyword_extract_v2(url, keywords, status, deep=True)
        elapsed = time.time() - t_start

        if not results:
            await status.edit_text(
                f"🔍 *No matches found.*\n\n"
                f"Scanned *{pages_scanned}* page(s) on `{domain}` in *{fmt_eta(elapsed)}*.\n"
                f"Keywords `{', '.join(keywords)}` were not found anywhere.",
                parse_mode=ParseMode.MARKDOWN,
            )
            ctx.user_data.clear()
            return ConversationHandler.END

        # ── Build .txt file ──────────────────────────────────────────
        ts_str    = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        separator = "=" * 70
        header = (
            f"KEYWORD EXTRACTION RESULTS\n"
            f"{separator}\n"
            f"Site       : {url}\n"
            f"Domain     : {domain}\n"
            f"Keywords   : {', '.join(keywords)}\n"
            f"Pages scanned : {pages_scanned}\n"
            f"Unique matches: {len(results)}\n"
            f"Elapsed    : {fmt_eta(elapsed)}\n"
            f"Generated  : {ts_str}\n"
            f"{separator}\n\n"
        )
        body     = "\n".join(f"{i+1:>5}. {line}" for i, line in enumerate(results))
        content  = (header + body + f"\n\n{separator}\nEnd of results\n").encode("utf-8")
        filename = f"kw_{domain.replace('.','_')}_{int(t_start)}.txt"

        buf      = io.BytesIO(content)
        buf.name = filename

        await status.edit_text(
            f"📤 *Sending results file…*\n"
            f"✅ Found *{len(results)}* unique matches across *{pages_scanned}* page(s)",
            parse_mode=ParseMode.MARKDOWN,
        )

        await update.message.reply_document(
            document=buf,
            filename=filename,
            caption=(
                f"🔍 *Keyword Extraction Complete*\n"
                f"🌐 Site: `{domain}`\n"
                f"🔑 Keywords: `{', '.join(keywords)}`\n"
                f"📄 Pages scanned: *{pages_scanned}*\n"
                f"✅ Unique matches: *{len(results)}*\n"
                f"⏱ Time: *{fmt_eta(elapsed)}*"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        await status.delete()

    except Exception as e:
        logger.exception("keyword_extract_v2 error")
        await status.edit_text(f"❌ *Extraction failed:*\n`{e}`", parse_mode=ParseMode.MARKDOWN)

    ctx.user_data.clear()
    return ConversationHandler.END


# ─────────────────────────────────────────────────────────────
#  NEW COMMAND HANDLERS: pause / resume / history / queue / scheduler
# ─────────────────────────────────────────────────────────────

@authorized_only
async def pause_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in _pause_events or _pause_events[uid].is_set() is False:
        await update.message.reply_text("⏸ Already paused (or no active download).")
        return
    _pause_events[uid].clear()   # clear = paused
    await update.message.reply_text(
        "⏸ *Download paused.*\n\nSend /resume to continue.",
        parse_mode=ParseMode.MARKDOWN,
    )


@authorized_only
async def resume_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in _pause_events:
        await update.message.reply_text("▶️ No active download to resume.")
        return
    _pause_events[uid].set()     # set = running
    await update.message.reply_text(
        "▶️ *Download resumed!*",
        parse_mode=ParseMode.MARKDOWN,
    )


HISTORY_PAGE_SIZE = 10

@authorized_only
async def history_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    hist = load_history()
    entries = hist.get(uid, [])
    if not entries:
        await update.message.reply_text("📜 No download history yet.")
        return

    # Parse optional page number: /history 2
    args = ctx.args
    page = int(args[0]) if args and args[0].isdigit() else 1
    total_pages = math.ceil(len(entries) / HISTORY_PAGE_SIZE)
    page = max(1, min(page, total_pages))

    start = len(entries) - page * HISTORY_PAGE_SIZE
    end   = start + HISTORY_PAGE_SIZE
    slice_ = entries[max(start, 0):end]
    slice_ = list(reversed(slice_))   # newest first

    lines = [f"📜 *Download History* (page {page}/{total_pages})\n"]
    for i, e in enumerate(slice_, 1):
        ts = e.get("ts", "")[:16].replace("T", " ")
        size = fmt_size(e.get("size", 0))
        elapsed = fmt_eta(e.get("elapsed", 0))
        name = e.get("name", "?")
        lines.append(f"`{i}.` *{name}*\n    💾 {size}  ⏱ {elapsed}  📅 {ts}")

    nav = []
    if page < total_pages:
        nav.append(f"/history {page + 1} ← older")
    if page > 1:
        nav.append(f"/history {page - 1} → newer")
    if nav:
        lines.append("\n" + "  |  ".join(nav))

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@authorized_only
async def queue_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show currently pending URLs in user's queue (stored in user_data)."""
    pending = ctx.user_data.get("queue_pending", [])
    if not pending:
        await update.message.reply_text(
            "📋 *Download Queue*\n\nNo pending downloads in your queue.\n\n"
            "_Paste multiple URLs in one message to start a batch queue._",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    lines = [f"📋 *Download Queue — {len(pending)} pending*\n"]
    for i, u in enumerate(pending, 1):
        short = u[:60] + ("…" if len(u) > 60 else "")
        lines.append(f"`{i}.` `{short}`")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@authorized_only
async def scheduler_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show current speed cap info."""
    cap_display = fmt_speed(MAX_SPEED_BPS) if MAX_SPEED_BPS > 0 else "🚫 No limit (unlimited)"
    await update.message.reply_text(
        f"🚀 *Speed Settings*\n\n"
        f"⚡ Active cap: `{cap_display}`\n"
        f"🔢 Parallel workers: `{PARALLEL_WORKERS}`\n"
        f"📦 Segment size: `{fmt_size(SEGMENT_SIZE)}`\n"
        f"🧩 Chunk size: `{fmt_size(CHUNK_SIZE)}`\n\n"
        f"_Throttle is {'DISABLED — full speed 24/7!' if MAX_SPEED_BPS == 0 else 'ACTIVE — set MAX_SPEED_MBS=0 to disable.'}_\n\n"
        f"*To set a cap (e.g. 300 MB/s):*\n"
        f"`export MAX_SPEED_MBS=300`\n"
        f"*To remove cap (unlimited):*\n"
        f"`export MAX_SPEED_MBS=0`\n"
        f"*To tune workers:*\n"
        f"`export DL_WORKERS=32`",
        parse_mode=ParseMode.MARKDOWN,
    )


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        raise RuntimeError(
            "\nBot token not set!\n"
            "  export TELEGRAM_BOT_TOKEN=your_token\n"
            "or edit BOT_TOKEN in the script.\n"
        )

    if OWNER_ID == 0:
        logger.warning(
            "⚠️  OWNER_ID not set — bot is open to all users (dev mode).\n"
            "   Set: export OWNER_ID=your_telegram_user_id"
        )
    else:
        logger.info(f"🔒 Owner-only mode — OWNER_ID={OWNER_ID}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    app = Application.builder().token(BOT_TOKEN).build()

    # Download conversation
    dl_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url)],
        states={WAITING_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_password)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True, per_chat=True, allow_reentry=True,
    )

    # Keyword extractor conversation (3-step: /keyword → site → keywords → results)
    kw_conv = ConversationHandler(
        entry_points=[CommandHandler("keyword", keyword_cmd)],
        states={
            KW_WAITING_SITE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, kw_receive_site)],
            KW_WAITING_KEYWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, kw_receive_keywords)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True, per_chat=True, allow_reentry=True,
    )

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("help",        help_cmd))
    app.add_handler(CommandHandler("workers",     workers_cmd))
    app.add_handler(CommandHandler("stats",       stats_cmd))
    app.add_handler(CommandHandler("history",     history_cmd))
    app.add_handler(CommandHandler("delete",      delete_cmd))
    app.add_handler(CommandHandler("queue",       queue_cmd))
    app.add_handler(CommandHandler("pause",       pause_cmd))
    app.add_handler(CommandHandler("resume",      resume_cmd))
    app.add_handler(CommandHandler("scheduler",   scheduler_cmd))
    app.add_handler(CommandHandler("adduser",     add_user_cmd))
    app.add_handler(CommandHandler("removeuser",  remove_user_cmd))
    app.add_handler(CommandHandler("users",       list_users_cmd))
    app.add_handler(CommandHandler("cancel",      cancel))
    app.add_handler(kw_conv)    # keyword conv must be before dl_conv
    app.add_handler(dl_conv)

    logger.info(f"⚡ Smart Download Bot v6.0 — {PARALLEL_WORKERS} parallel workers — running…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
