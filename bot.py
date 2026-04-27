--- bot.py (原始)
"""
╔══════════════════════════════════════════════════════════════╗
║      𝙕𝙔 𝘾𝙤𝙢𝙗𝙤 𝙎𝙘𝙖𝙣𝙣𝙚𝙧 𝘽𝙤𝙩  ·  𝙑𝟴.𝟱 𝙐𝙇𝙏𝙍𝘼         ║
╚══════════════════════════════════════════════════════════════╝

Features:
  • Free tier  — 1 link at a time, 10-part split, 500 MB cap (default)
  • VIP tier   — unlimited links, 30-part split, no cap, max speed
  • GOD MODE   — 50-part split, 32 concurrent workers, 8 MB chunks, ULTRA speed
  • Plan-based split download engine:
      FREE → 10 parts (4 active)  VIP → 30 parts (15 active)
      GOD  → 50 parts (32 active)  ⚡ ULTRA
  • Streaming scan — download chunk → filter → write results immediately
    No intermediate raw file stored on disk.
  • Early filter (colon/at check) before full keyword parse
  • Single-writer queue — zero concurrent file-write contention
  • Range-fallback — auto single-stream if server doesn't support ranges
  • Per-part retry (15× GOD / 3× others) with exponential backoff + jitter
  • Live download progress (edited in-place, no spam)
  • /stop command — stop current scan, sends already-processed results
  • /lockall /unlockall — admin: freeze all running scans
  • Admin panel: /stats /broadcast /addvip /rmvip /viptrial
                 /subscribers /addfreeserver /rmfreeserver
                 /addrequiredchannel /rmrequiredchannel
                 /lockall /unlockall /setlinklimit /setfreeserverlimit
                 /genkey (days/hours/minutes) maxuser
                 /rmkey all | /rmkey (userid)
  • Required channel join gate before any scan
  • Keyword-named result files, URL-stripped + deduplicated
  • Ask user before processing: merge into single file or separate?
  • Crash-safe cleanup on Railway / any PaaS
  • Global total GB downloaded counter
  • Railway-optimised: persistent JSON, crash-resume, enlarged Telegram timeouts
"""

import os, re, json, asyncio, aiohttp, aiofiles, logging, traceback, secrets, string
import random
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

# ─────────────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
#  Config - GOD MODE ULTRA — Railway-Optimised
# ─────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "8661120242:AAEO3UUHVKrcQha_XCGsVA0tJJq3qy5_8YQ")
ADMIN_IDS = {5028065177}
RESULTS_DIR      = Path(os.getenv("RESULTS_DIR",      "results"))
DATA_FILE        = Path(os.getenv("DATA_FILE",        "botdata.json"))
PENDING_JOB_FILE = Path(os.getenv("PENDING_JOB_FILE", "pending_job.json"))

# ── Chunk sizes ─────────────────────────────────────────────────────────
# GOD: 512 KB — semaphore is now released before streaming (not after),
# so all 32 parts stream freely. Larger chunks = fewer syscalls = higher
# sustained throughput. The old 256 KB comment was a workaround for the
# semaphore bug; with that fixed, bigger chunks win.
CHUNK_SIZE_FREE  = 1024 * 128        # 128 KB
CHUNK_SIZE_VIP   = 1024 * 384        # 384 KB
CHUNK_SIZE_GOD   = 1024 * 512        # 512 KB ⚡ max throughput

# Connection pool limits
MAX_PAR_FREE     = 3
MAX_PAR_GOD      = 96               # 32 active + 64 queued headroom

# ── Plan-based split parts ──────────────────────────────────────────────
N_PARTS_FREE = 8    # FREE  → 8 parts
N_PARTS_VIP  = 16   # VIP   → 16 parts
N_PARTS_GOD  = 32   # GOD   → 32 parts ⚡ all run concurrently

# Max concurrent parts
MAX_WORKERS_FREE = 4    # 8 parts, 4 active
MAX_WORKERS_VIP  = 12   # 16 parts, 12 active
MAX_WORKERS_GOD  = 32   # 32 parts, all 32 active simultaneously ⚡

# Legacy thread-count names (kept so vipinfo display still works)
N_THREADS_FREE = N_PARTS_FREE
N_THREADS_VIP  = N_PARTS_VIP
N_THREADS_GOD  = N_PARTS_GOD

FREE_SIZE_LIMIT  = 500 * 1024 * 1024  # 500 MB (overridable via /setfreeserverlimit)

# Retry settings - CRASH SAFE
MAX_RETRIES      = 3
MAX_RETRIES_GOD  = 15               # GOD MODE: Maximum retries for ultra reliability
PROGRESS_STEP    = 5               # % step for live bar update

# Timeout settings (seconds)
CONNECT_TIMEOUT_FREE = 60
CONNECT_TIMEOUT_VIP  = 20
CONNECT_TIMEOUT_GOD  = 8            # Fast fail → fast retry → no long idle gaps

SOCK_READ_TIMEOUT_FREE = 300
SOCK_READ_TIMEOUT_VIP  = 120
# KEY FIX: 15s read timeout for GOD MODE.
# The old 60s value meant a stalled part sat silent for a full minute
# before retrying — that's the "drop to 0.5 MB/s" window you see.
# At 15s, a stalled part fails fast, retries immediately, and the other
# 31 parts keep running at full speed the whole time.
SOCK_READ_TIMEOUT_GOD  = 15

# Exponential backoff settings
BASE_RETRY_DELAY = 0.2              # Start retrying almost immediately
MAX_RETRY_DELAY  = 5.0             # Low cap — don't wait long in GOD MODE
JITTER_RANGE     = 0.2             # Tight jitter to avoid thundering herd

RESULTS_DIR.mkdir(exist_ok=True)

URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────────
#  Persistent data  (JSON — survives Railway restarts)
# ─────────────────────────────────────────────────────────────────────
def _load_data() -> dict:
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            pass
    return {
        "vip": {},
        "vip_keys": {},
        "free_enabled": True,
        "required_channels": [],
        "stats": {"total_scans": 0, "total_users": [], "total_bytes_downloaded": 0},
        "link_limit": None,          # None = unlimited for VIP
        "free_size_limit_mb": 500,   # MB, configurable via /setfreeserverlimit
        "locked": False,             # /lockall state
    }

def _save_data() -> None:
    d = dict(_db)
    d["stats"] = {**d["stats"], "total_users": list(d["stats"]["total_users"])}
    DATA_FILE.write_text(json.dumps(d, indent=2))

async def _asave() -> None:
    """Non-blocking wrapper — runs _save_data in a thread so the event loop never stalls."""
    await asyncio.get_event_loop().run_in_executor(None, _save_data)

# ─────────────────────────────────────────────────────────────────────
#  Pending-job persistence  (crash-resume)
# ─────────────────────────────────────────────────────────────────────
def _save_pending_job(job) -> None:
    """Persist the active job to disk so it can be resumed after a restart."""
    try:
        data = {
            "uid":         job.uid,
            "chat_id":     job.chat_id,
            "links":       job.links,
            "keywords":    job.keywords,
            "vip":         job.vip,
            "god":         job.god,
            "single_file": job.single_file,
            # Track which link indices are already done so we skip them on resume
            "done_indices": [
                idx for idx, info in job.results.items()
                if info.get("status") == "done"
            ],
        }
        PENDING_JOB_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        log.warning(f"_save_pending_job failed: {e}")

def _load_pending_job() -> dict | None:
    """Load a previously-saved pending job, or return None if none exists."""
    if not PENDING_JOB_FILE.exists():
        return None
    try:
        return json.loads(PENDING_JOB_FILE.read_text())
    except Exception as e:
        log.warning(f"_load_pending_job failed: {e}")
        return None

def _clear_pending_job() -> None:
    """Delete the pending-job file once the job completes or is cancelled."""
    try:
        if PENDING_JOB_FILE.exists():
            PENDING_JOB_FILE.unlink()
    except Exception as e:
        log.warning(f"_clear_pending_job failed: {e}")

_db = _load_data()
_db["stats"]["total_users"] = set(_db["stats"].get("total_users", []))
if "total_bytes_downloaded" not in _db["stats"]:
    _db["stats"]["total_bytes_downloaded"] = 0
if "link_limit" not in _db:
    _db["link_limit"] = None
if "free_size_limit_mb" not in _db:
    _db["free_size_limit_mb"] = 500
if "locked" not in _db:
    _db["locked"] = False

def is_vip(uid: int) -> bool:
    entry = _db["vip"].get(str(uid))
    if not entry:
        return False
    if entry.get("permanent"):
        return True
    exp = entry.get("expires")
    if exp and datetime.fromisoformat(exp) > datetime.utcnow():
        return True
    return False

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def is_god(uid: int) -> bool:
    """God Mode — admin-only tier with 50x threads and maximum crash safety."""
    return uid in ADMIN_IDS

def get_free_size_limit() -> int:
    """Returns free size limit in bytes."""
    mb = _db.get("free_size_limit_mb", 500)
    return int(mb * 1024 * 1024)

def _fmt_gb_total() -> str:
    total_bytes = _db["stats"].get("total_bytes_downloaded", 0)
    gb = total_bytes / (1024 ** 3)
    return f"{gb:.2f} GB"

# ─────────────────────────────────────────────────────────────────────
#  Exponential Backoff with Jitter - CRASH SAFE RETRY
# ─────────────────────────────────────────────────────────────────────
def calculate_backoff(attempt: int, base_delay: float = BASE_RETRY_DELAY,
                      max_delay: float = MAX_RETRY_DELAY,
                      jitter: float = JITTER_RANGE) -> float:
    """
    Calculate exponential backoff with jitter for retry delays.
    This prevents thundering herd problems and improves crash safety.
    """
    # Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s (capped)
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    # Add random jitter (0% to jitter% of delay)
    jitter_amount = delay * jitter * random.random()
    return delay + jitter_amount

# ─────────────────────────────────────────────────────────────────────
#  Stats Card Builder
# ─────────────────────────────────────────────────────────────────────
def _build_stats_card(job, pct: int, speed_mb: float, eta: str,
                      done_mb: float, total_mb: float, phase: str) -> str:
    """Render the single persistent live stats card with beautiful formatting."""
    # ── progress bar (20 blocks) ──
    filled = round(pct / 5)
    bar    = "█" * filled + "▒" * (20 - filled)

    # ── size display — cap done at total so we never display e.g. "3.60 GB / 3.40 GB" ──
    if total_mb > 0:
        display_done_mb = min(done_mb, total_mb)
    else:
        display_done_mb = done_mb
    if total_mb >= 1024:
        size_str = f"`{display_done_mb/1024:.2f} GB` / `{total_mb/1024:.2f} GB`"
    elif total_mb > 0:
        size_str = f"`{display_done_mb:.1f} MB` / `{total_mb:.1f} MB`"
    else:
        size_str = f"`{display_done_mb:.1f} MB`"

    speed_str = f"`{speed_mb:.1f} MB/s`" if speed_mb > 0 else "`—`"
    eta_str   = f"`{eta}`"

    return (
        f"`{bar}` *{pct}%*\n"
        f"📦 {size_str}\n"
        f"🚀 Speed: {speed_str} | ⏱ ETA: {eta_str}\n\n"
        f"📡 *Phase:* `{phase}`"
    )

CARD_MIN_INTERVAL = 5.0   # seconds — Telegram allows ~1 edit/msg/5s before flood

async def _update_card(job, pct: int = 0, speed_mb: float = 0.0,
                       eta: str = "—", done_mb: float = 0.0,
                       total_mb: float = 0.0, phase: str = "…",
                       force: bool = False):
    """
    Edit the single persistent card in-place.
    Throttled to CARD_MIN_INTERVAL seconds to avoid Telegram 429 flood errors.
    Pass force=True for milestone events (link complete, scan done) to bypass throttle.
    """
    if not job.live_stats_mid:
        return

    if job._card_lock is None:
        job._card_lock = asyncio.Lock()

    job._card_pct   = pct
    job._card_speed = speed_mb
    job._card_eta   = eta
    job._card_phase = phase

    now = asyncio.get_event_loop().time()
    if not force and now - job._card_last_edit < CARD_MIN_INTERVAL:
        return

    async with job._card_lock:
        now = asyncio.get_event_loop().time()
        if not force and now - job._card_last_edit < CARD_MIN_INTERVAL:
            return

        card = _build_stats_card(
            job,
            job._card_pct, job._card_speed, job._card_eta,
            done_mb, total_mb, job._card_phase
        )
        try:
            await job.bot.edit_message_text(
                chat_id=job.chat_id,
                message_id=job.live_stats_mid,
                text=card,
                parse_mode=ParseMode.MARKDOWN,
            )
            job._card_last_edit = asyncio.get_event_loop().time()
        except TelegramError as e:
            err = str(e).lower()
            if "not modified" in err:
                job._card_last_edit = asyncio.get_event_loop().time()
            elif "429" in err or "flood" in err:
                log.debug(f"Card flood-control — will retry next cycle")
            else:
                log.warning(f"card edit error: {e}")

# ─────────────────────────────────────────────────────────────────────
#  Job dataclass
# ─────────────────────────────────────────────────────────────────────
@dataclass
class Job:
    uid:        int
    chat_id:    int
    links:      list
    keywords:   list
    bot:        object
    vip:        bool = False
    god:        bool = False
    single_file: bool = False
    results:    dict = field(default_factory=dict)
    total_bytes_expected: int = 0
    total_bytes_done:     int = 0
    temp_files: list = field(default_factory=list)
    stopped:    bool = False
    cancel_task: object = None
    # Live stats
    live_hits:       dict = field(default_factory=dict)
    live_lines:      int  = 0
    links_done:      int  = 0
    links_total:     int  = 0
    live_stats_mid:  int  = 0
    # Throttle
    _card_last_edit: float = 0.0
    _card_lock:      object = field(default=None)
    _card_phase:     str  = "…"
    _card_pct:       int  = 0
    _card_speed:     float = 0.0
    _card_eta:       str  = "—"
    # Crash safety — no longer used (streaming engine handles retries per-part)
    # kept as stubs so old pickled/serialised Job objects don't break on load
    _failed_ranges: dict = field(default_factory=dict)
    _retry_count:   int  = 0

# ─────────────────────────────────────────────────────────────────────
#  Global queue
# ─────────────────────────────────────────────────────────────────────
_job_queue:   asyncio.Queue = None
_queue_list:  list          = []
_queue_lock:  asyncio.Lock  = None
_current_job: Job | None    = None
_running_tasks: dict[int, asyncio.Task] = {}

user_state: dict[int, dict] = defaultdict(lambda: {
    "step": "idle", "links": [], "keywords": [],
})

# ─────────────────────────────────────────────────────────────────────
#  Telegram helpers
# ─────────────────────────────────────────────────────────────────────
async def safe_send(bot, chat_id: int, text: str, **kw):
    for i in range(3):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, **kw)
        except TelegramError as e:
            log.warning(f"send_message attempt {i+1}: {e}")
            await asyncio.sleep(2 ** i)

async def safe_edit(bot, chat_id: int, msg_id: int, text: str, **kw):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, **kw)
    except TelegramError as e:
        if "not modified" not in str(e).lower():
            log.warning(f"edit_message_text: {e}")

async def safe_send_doc(bot, chat_id: int, path: Path, caption: str = ""):
    import io
    for i in range(3):
        try:
            async with aiofiles.open(path, "rb") as af:
                data = await af.read()
            await bot.send_document(chat_id=chat_id,
                                    document=io.BytesIO(data),
                                    filename=path.name, caption=caption)
            return
        except TelegramError as e:
            log.warning(f"send_document attempt {i+1}: {e}")
            await asyncio.sleep(2 ** i)

def _fmt(mb: float) -> str:
    return f"{mb/1024:.2f} GB" if mb >= 1024 else f"{mb:.1f} MB"

def _kw_filename(keywords: list[str]) -> str:
    safe = "_".join(re.sub(r'[^\w]', '', k) for k in keywords if k)
    return (safe[:80] or "results") + ".txt"

def _strip_url(line: str) -> str:
    """
    Extract ONLY the credential (user:pass / email:pass) from any combo-list line.
    Zero URL leaks. Zero false-positive credential drops.

    Handles every real-world combo format:
      https://site.com:user:pass                → user:pass
      https://site.com:user@x.com:pass          → user@x.com:pass
      https://site.com:8080:john:secret         → john:secret
      https://cdn.site.com/files/2024/:user:pass→ user:pass
      https://a.com|https://b.com|user:pass     → user:pass
      https://site.com https://b.com user:pass  → user:pass
      site.com:443:user:pass                    → user:pass
      site.com:user:pass                        → user:pass
      user@gmail.com:pass                       → user@gmail.com:pass
      john:mypassword                           → john:mypassword
      john.doe:mypassword                       → john.doe:mypassword
      user TAB pass  /  url TAB user TAB pass   → user:pass
      {"login":"u","password":"p"}              → u:p
      LOGIN: u PASS: p                          → u:p
      user@x.com;pass  /  user,pass            → user@x.com:pass
      ftp://site.com:user:pass                  → user:pass
      socks5://proxy:1080:user:pass             → user:pass
      android://base64@com.package:             → (discarded — opaque token, not a credential)
      android://base64@com.package/:            → (discarded — opaque token, not a credential)
    """
    import json as _j

    # Only well-known schemes as URL prefixes — avoids false-positives on
    # passwords that happen to contain short strings like "pass://" or "go://"
    _IS_URL  = re.compile(
        r'^(?:https?|ftp|ftps|sftp|socks[45]?|ss|vmess|trojan|android|ios)://',
        re.IGNORECASE
    )
    _ANY_URL = re.compile(
        r'(?:https?|ftp|ftps|sftp|socks[45]?|ss|vmess|trojan|android|ios)://\S*',
        re.IGNORECASE
    )

    # App-scheme URLs with opaque base64/token user-info (android://, ios://, etc.)
    # The user-info portion is 20+ chars of base64/token — never a real credential.
    _APP_SCHEME_URL = re.compile(
        r'^(?:android|ios|[a-z][a-z0-9]{2,20})://[A-Za-z0-9+/=_\-]{20,}@[\w.\-]+[:/]',
        re.IGNORECASE
    )

    line = line.strip().rstrip('\r\n').strip()
    if not line:
        return ""

    # ── -1. Discard app-scheme token URLs (android://, ios://, etc.) ──────
    #   e.g. android://BIGBASE64TOKEN@com.company.app:
    #   The "user-info" is an opaque auth token, not a username.
    if _APP_SCHEME_URL.match(line):
        return ""

    # ── 0. JSON ───────────────────────────────────────────────────────────
    if line.startswith('{') and line.endswith('}'):
        try:
            obj = _j.loads(line)
            user = (obj.get("login") or obj.get("email") or obj.get("username")
                    or obj.get("user") or "")
            pw   = (obj.get("password") or obj.get("pass") or obj.get("passwd")
                    or obj.get("pwd") or "")
            if user and pw:
                return f"{user}:{pw}"
        except Exception:
            pass

    # ── 1. Space key-value: "LOGIN: user PASS: pass" ─────────────────────
    _kv = re.match(
        r'(?i)(?:login|email|user(?:name)?)\s*[=:]\s*(\S+)\s+'
        r'(?:pass(?:word)?|pwd)\s*[=:]\s*(\S+)', line)
    if _kv:
        return f"{_kv.group(1)}:{_kv.group(2)}"

    # ── 2. Tab-separated: drop URL tabs, keep cred tabs ───────────────────
    if '\t' in line:
        segs = [s.strip() for s in line.split('\t') if s.strip()]
        clean = [s for s in segs if not _IS_URL.match(s)]
        if len(clean) >= 2:
            return f"{clean[0]}:{clean[1]}"
        if len(clean) == 1 and ':' in clean[0]:
            return clean[0]

    # ── 3. Pipe-separated: drop URL/bare-domain segments ─────────────────
    if '|' in line:
        segs = [s.strip() for s in line.split('|') if s.strip()]
        clean = [s for s in segs if not _IS_URL.match(s)]
        def _is_bare_domain_seg(s):
            host = s.split(':')[0]
            return ('.' in host and '@' not in host and '/' not in host
                    and ' ' not in host and not host.isdigit())
        clean = [s for s in clean if not _is_bare_domain_seg(s)]
        if clean:
            line = clean[0]
        else:
            return ""

    # ── 4. Space-separated: strip ALL URLs from anywhere in the line ─────
    #   Remove URLs from any position (start, middle, end)
    #   BUT: only if there are multiple space-separated tokens
    #   (don't nuke single-token URL:cred lines like "https://site.com:user:pass")
    if ' ' in line and _ANY_URL.search(line):
        # Remove all URLs first
        tokens = line.split()
        cred_tokens = [t for t in tokens if not _IS_URL.match(t)]
        line = ' '.join(cred_tokens).strip() if cred_tokens else ""
        if not line:
            return ""
        # After removing space-separated URLs, continue processing
        # Don't return early - there may still be scheme-prefixed URLs to handle

    # ── 5. Protocol-relative //host ───────────────────────────────────────
    if line.startswith('//'):
        line = re.sub(r'^//[^\s:|]+(?::\d+)?(?:/[^\s:]*)?', '', line).lstrip(':').strip()
        if not line:
            return ""

    # ── 6. Scheme-prefixed URL at start: strip scheme://[host][:port][/path] ───────
    #   IMPORTANT: do NOT consume user-info (user@) here — it might be the
    #   credential itself (e.g. https://site.com:user@gmail.com:pass).
    #   Only strip the scheme + bare hostname + optional port + optional path.
    if _IS_URL.match(line):
        # Remove scheme + hostname only (no user-info stripping)
        after = re.sub(
            r'^(?:https?|ftp|ftps?|sftp|socks[45]?|ss|vmess|trojan|android|ios)'
            r'://[a-zA-Z0-9.\-]+'          # hostname (no @ allowed here)
            r'(?::\d+)?'                   # optional port
            r'(?:/[^:@\s]*)?',             # optional path (stop at : or @)
            '', line, flags=re.IGNORECASE
        )
        after = after.lstrip(':').strip()
        if after:
            line = after
        else:
            return ""

    # ── 7. Bare domain/IP prefix: site.com[:port]:user:pass ───────────────
    #   Only strip the domain prefix when what follows is clearly a full
    #   credential pair (user:pass with at least 2 colon-separated parts
    #   remaining after dropping an optional numeric port).
    #
    #   CRITICAL FIX: if the line is already just "username:password" with
    #   no URL prefix, do NOT strip it. We only apply this rule when the first
    #   segment looks like a real domain (contains a dot AND no @ sign AND
    #   is not already a credential by itself).
    parts = line.split(':')
    if len(parts) >= 3:   # Need at least domain:user:pass (3 parts)
        first = parts[0].strip()
        # Must look like a domain: has a dot, no @, no spaces, not pure digits
        is_real_domain = ('.' in first and '@' not in first and ' ' not in first
                          and not first.isdigit() and 2 <= len(first) <= 253)
        if is_real_domain:
            rest = parts[1:]
            if rest and rest[0].strip().isdigit():   # skip port segment
                rest = rest[1:]
            if len(rest) >= 2:    # must still have user + pass remaining
                cred = ':'.join(rest).strip()
                if cred:
                    return cred
    # 2-part lines (domain:pass) fall through — could be a plain user:pass too.
    # We keep them as-is and let later steps handle or return them verbatim.

    # ── 8. Email:pass — colon immediately after @ sign ────────────────────
    first_colon = line.find(':')
    if first_colon > 0 and '@' in line[:first_colon]:
        return line   # e.g. user@gmail.com:pass — return verbatim

    # ── 9. Plain username:password — return verbatim ──────────────────────
    #   At this point the line has no URL scheme and wasn't caught by domain
    #   stripping. If it has a colon it's almost certainly user:pass.
    if ':' in line:
        # Sanity: don't return pure junk. Must have non-empty parts.
        u, _, p = line.partition(':')
        if u.strip() and p.strip():
            return line   # ← FIXED: was being dropped before reaching here

    # ── 10. Alt separators: semicolon / comma ────────────────────────────
    for sep in (';', ','):
        if sep in line:
            u, _, p = line.partition(sep)
            u, p = u.strip(), p.strip()
            if u and p:
                return f"{u}:{p}"

    # ── 11. Final safety net: nuke any surviving scheme URL ───────────────
    #   Only match well-known schemes here — same list as _IS_URL — to avoid
    #   nuking passwords that happen to contain short letter sequences + "://".
    if re.search(
        r'(?:https?|ftp|ftps?|sftp|socks[45]?|ss|vmess|trojan|android|ios)://',
        line, re.IGNORECASE
    ):
        if _IS_URL.match(line.strip()):
            return ""   # entire value is a URL — discard
        line = _ANY_URL.sub('', line).strip().lstrip(':').strip()

    return line.strip()


# ─────────────────────────────────────────────────────────────────────
#  Channel gate
# ─────────────────────────────────────────────────────────────────────
async def check_joined_channels(bot, uid: int) -> list[str]:
    """Check if user has joined all required channels."""
    not_joined = []
    required_channels = _db.get("required_channels", [])

    for ch in required_channels:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=uid)
            if member.status in (ChatMember.LEFT, ChatMember.BANNED):
                not_joined.append(ch)
        except Exception as e:
            log.warning(f"Could not check channel {ch} for user {uid}: {e}")
            not_joined.append(ch)

    return not_joined

async def build_channel_join_keyboard(missing_channels: list[str]) -> InlineKeyboardMarkup:
    """Build a keyboard with join buttons for missing channels."""
    buttons = []
    for ch in missing_channels:
        ch_display = ch if ch.startswith('@') else f'@{ch}'
        ch_link = ch.replace('@', '')
        buttons.append([InlineKeyboardButton(
            f"📢 Join {ch_display}",
            url=f"https://t.me/{ch_link}"
        )])

    buttons.append([InlineKeyboardButton(
        "🔄 Check Membership",
        callback_data="check_membership"
    )])

    return InlineKeyboardMarkup(buttons)

# ─────────────────────────────────────────────────────────────────────
#  STREAMING DOWNLOAD+SCAN ENGINE
#  Single URL → N plan-based parts → semaphore-controlled parallel
#  Each part: stream download → early filter → parse → write results
#  No intermediate raw file stored on disk.
# ─────────────────────────────────────────────────────────────────────

async def _process_link_streaming(session, url: str, idx: int, job: Job) -> tuple[dict, Optional[Path]]:
    """
    ┌─────────────────────────────────────────────────────────────────┐
    │  PLAN-BASED SPLIT + STREAMING SCAN ENGINE                      │
    │                                                                 │
    │  1. HEAD  → get file size, check Accept-Ranges                 │
    │  2. SPLIT → N parts per plan (FREE=10 / VIP=20 / GOD=25)      │
    │  3. QUEUE → semaphore limits concurrent active parts           │
    │  4. EACH PART: stream → early filter → parse → write result   │
    │     No intermediate raw file stored on disk.                   │
    │  5. RETRY per part up to 3x with exponential backoff          │
    │  6. FALLBACK to single stream if range not supported          │
    └─────────────────────────────────────────────────────────────────┘
    """
    if job.stopped:
        return {k: 0 for k in job.keywords}, None

    # ── Plan parameters ───────────────────────────────────────────────
    if job.god:
        n_parts     = N_PARTS_GOD
        max_workers = MAX_WORKERS_GOD
        chunk_size  = CHUNK_SIZE_GOD
        max_retries = MAX_RETRIES_GOD
        conn_timeout = CONNECT_TIMEOUT_GOD
        read_timeout = SOCK_READ_TIMEOUT_GOD
    elif job.vip:
        n_parts     = N_PARTS_VIP
        max_workers = MAX_WORKERS_VIP
        chunk_size  = CHUNK_SIZE_VIP
        max_retries = MAX_RETRIES
        conn_timeout = CONNECT_TIMEOUT_VIP
        read_timeout = SOCK_READ_TIMEOUT_VIP
    else:
        n_parts     = N_PARTS_FREE
        max_workers = MAX_WORKERS_FREE
        chunk_size  = CHUNK_SIZE_FREE
        max_retries = MAX_RETRIES
        conn_timeout = CONNECT_TIMEOUT_FREE
        read_timeout = SOCK_READ_TIMEOUT_FREE

    base_headers = {"User-Agent": "Mozilla/5.0 (compatible; ZyScanBot/8.5)"}
    kw_lower    = [k.lower() for k in job.keywords]
    SEEN_CAP    = 5_000_000
    BUF_SIZE    = 20_000     # Large write buffer — flush less often

    # ── Result file setup — one file per keyword ─────────────────────
    result_paths: dict[str, Path] = {}
    for kw in job.keywords:
        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
        rp = RESULTS_DIR / f"link{idx}_{safe_kw[:80]}.txt"
        result_paths[kw] = rp
        job.temp_files.append(rp)
    result_path = list(result_paths.values())[0] if result_paths else \
        RESULTS_DIR / f"link{idx}_results.txt"

    # ── Shared state ──────────────────────────────────────────────────
    hits: dict[str, int]  = {k: 0 for k in job.keywords}
    seen: set[str]        = set()
    bytes_done            = [0]

    # Per-keyword write queues — large maxsize avoids put_nowait drops
    write_queues: dict[str, asyncio.Queue] = {
        k: asyncio.Queue(maxsize=0) for k in job.keywords  # 0 = unlimited
    }
    BUF_SENTINEL = None

    # ── Writer: batched async writes per keyword ──────────────────────
    async def _writer_for(kw: str):
        buf: list[str] = []
        async with aiofiles.open(result_paths[kw], "w", encoding="utf-8") as fout:
            while True:
                item = await write_queues[kw].get()
                if item is BUF_SENTINEL:
                    if buf:
                        await fout.write("".join(buf))
                    break
                buf.append(item)
                if len(buf) >= BUF_SIZE:
                    await fout.write("".join(buf))
                    buf.clear()

    async def _writer():
        await asyncio.gather(*[_writer_for(kw) for kw in job.keywords])

    # ── Step 1: HEAD → file size + range support ──────────────────────
    # Try HEAD first; if it fails, fire a GET with Range:bytes=0-0 as probe
    file_size       = 0
    range_supported = False
    try:
        head_timeout = aiohttp.ClientTimeout(total=15)
        async with session.head(url, headers=base_headers,
                                timeout=head_timeout, ssl=False,
                                allow_redirects=True) as resp:
            file_size = int(resp.headers.get("Content-Length", 0))
            ar = resp.headers.get("Accept-Ranges", "")
            range_supported = ar.lower() == "bytes" and file_size > 0
    except Exception:
        pass

    # Fallback probe: GET Range:bytes=0-0 to confirm range support
    if not range_supported:
        try:
            probe_timeout = aiohttp.ClientTimeout(total=15)
            async with session.get(
                url,
                headers={**base_headers, "Range": "bytes=0-0"},
                timeout=probe_timeout, ssl=False,
                allow_redirects=True,
            ) as resp:
                if resp.status == 206:
                    cr = resp.headers.get("Content-Range", "")
                    m  = re.search(r"/(\d+)$", cr)
                    if m:
                        file_size = int(m.group(1))
                    elif not file_size:
                        file_size = int(resp.headers.get("Content-Length", 0))
                    range_supported = file_size > 0
                elif resp.status == 200:
                    file_size = int(resp.headers.get("Content-Length", 0))
        except Exception as e:
            log.warning(f"[Link {idx}] Probe failed ({e}) — single stream")

    if file_size:
        job.total_bytes_expected += file_size

    log.info(f"[Link {idx}] size={file_size/1_048_576:.1f}MB range={range_supported} "
             f"parts={n_parts} workers={max_workers} chunk={chunk_size//1024}KB")

    # ─────────────────────────────────────────────────────────────────
    #  Progress reporter
    # ─────────────────────────────────────────────────────────────────
    reporter_stop = asyncio.Event()

    async def _progress_reporter():
        t0       = asyncio.get_event_loop().time()
        last_done  = 0
        last_speed = 0.0
        while not reporter_stop.is_set():
            await asyncio.sleep(3)          # update every 3s (was 5s) — snappier UI
            if reporter_stop.is_set():
                break
            done    = bytes_done[0]
            elapsed = asyncio.get_event_loop().time() - t0
            delta   = done - last_done
            last_done = done
            spd = (delta / 3) / 1_048_576  # MB/s over last 3s window
            if spd > 0:
                last_speed = spd
            elif last_speed > 0 and elapsed < 30:
                spd = last_speed

            if file_size and done > 0:
                pct   = min(int(done / file_size * 100), 99)
                sb    = done / elapsed if elapsed > 0 else 0
                eta_s = int((file_size - done) / sb) if sb > 0 and done < file_size else 0
                eta   = f"{eta_s // 60}m {eta_s % 60}s" if eta_s > 0 else "—"
            else:
                pct, eta = 0, "—"

            label = (f"⚡ {n_parts} parts · {hits and sum(hits.values()) or 0} hits"
                     if range_supported else f"🌊 Stream · {hits and sum(hits.values()) or 0} hits")
            await _update_card(job, pct, spd, eta,
                               job.total_bytes_done / 1_048_576,
                               job.total_bytes_expected / 1_048_576,
                               label)

    # ─────────────────────────────────────────────────────────────────
    #  Hot inner loop — runs in a ThreadPoolExecutor so it NEVER blocks
    #  the event loop. Downloads on all 32 parts can proceed freely
    #  while the CPU processes lines on a separate OS thread.
    # ─────────────────────────────────────────────────────────────────
    _kw_count  = len(job.keywords)
    _kw_orig   = job.keywords
    _kw_low    = kw_lower
    _write_qs  = write_queues
    _seen      = seen
    _hits      = hits
    _live_hits = job.live_hits
    _seen_cap  = SEEN_CAP

    # Threading lock: protects shared state (seen set, hits, live_hits).
    # KEY: We minimise time spent holding this lock so scan threads don't
    # block each other — expensive work (strip_url, keyword match) all
    # happens OUTSIDE the lock, lock is only taken for the dedup commit.
    import threading
    _state_lock = threading.Lock()

    def _process_chunk_lines(lines: list[str]) -> None:
        """
        CPU-bound line filter — lock-minimised design for GOD MODE.

        Phase 1 (NO lock): keyword match + strip_url for every line.
                           This is the slow CPU work — runs fully parallel.
        Phase 2 (local dedup): deduplicate within this batch using a local
                           set so we arrive at the global lock with minimal work.
        Phase 3 (brief lock): commit new entries to global seen set + hits.
                           Lock held for O(unique_hits) dict ops only.
        Phase 4 (NO lock): enqueue writes — put_nowait is GIL-safe.
        """
        # ── Phase 1: scan lines with no lock held ────────────────────
        candidates: list[tuple[int, str]] = []   # (kw_index, clean_cred)
        for raw_line in lines:
            if not raw_line:
                continue
            if ":" not in raw_line and "@" not in raw_line:
                continue
            low = raw_line.lower()
            for i in range(_kw_count):
                if _kw_low[i] not in low:
                    continue
                clean = _strip_url(raw_line)
                if clean:
                    candidates.append((i, clean))
                break   # first matching keyword wins per line

        if not candidates:
            return

        # ── Phase 2: local dedup (no lock — this set is thread-local) ─
        local_seen: set[str] = set()
        deduped: list[tuple[int, str]] = []
        for i, clean in candidates:
            if clean not in local_seen:
                local_seen.add(clean)
                deduped.append((i, clean))

        # ── Phase 3: global dedup + commit (lock held briefly) ────────
        committed: list[tuple[str, str]] = []   # (kw_name, line_out)
        with _state_lock:
            seen_len = len(_seen)
            for i, clean in deduped:
                if seen_len < _seen_cap:
                    if clean in _seen:
                        continue
                    _seen.add(clean)
                    seen_len += 1
                kw = _kw_orig[i]
                _hits[kw] += 1
                _live_hits[kw] = _live_hits.get(kw, 0) + 1
                committed.append((kw, clean + "\n"))

        # ── Phase 4: enqueue writes — put_nowait is GIL-safe ──────────
        for kw, line_out in committed:
            _write_qs[kw].put_nowait(line_out)

    # Thread pool sizing:
    #   GOD: 12 workers — one per 2-3 download coroutines at peak, preventing
    #        GIL contention while keeping all 32 parts fed continuously.
    #        6 workers was the bottleneck causing the "after-peak" speed drop.
    #   VIP: 6, FREE: 2 — proportional to part count.
    import concurrent.futures
    _tp_workers = 12 if job.god else (6 if job.vip else 2)
    _thread_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=_tp_workers, thread_name_prefix="scan"
    )

    # Pending scan futures — tracked so we can drain them before closing
    _pending_scans: list = []

    def _process_async(lines: list[str]) -> None:
        """
        Fire-and-forget submit: does NOT await. The download coroutine calls
        this and returns immediately to read the next network chunk — keeping
        the pipe saturated. This eliminates the stall where each part had to
        wait for the CPU scan to finish before it could read more data.
        Futures collected in _pending_scans; drained before exit.
        Completed futures are pruned every 200 submissions to prevent
        unbounded memory growth on large files.
        """
        fut = _thread_pool.submit(_process_chunk_lines, lines)
        _pending_scans.append(fut)
        # Prune done futures every 200 submissions — avoids list growing to
        # millions of entries on large files, which itself slows things down.
        if len(_pending_scans) % 200 == 0:
            done_indices = [i for i, f in enumerate(_pending_scans) if f.done()]
            for i in reversed(done_indices):
                _pending_scans.pop(i)

    async def _drain_scans() -> None:
        """Wait for all in-flight scan futures to finish (called before exiting)."""
        if not _pending_scans:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, lambda: [f.result() for f in _pending_scans]
        )

    # ─────────────────────────────────────────────────────────────────
    #  Shared decode helper — splits bytes on newlines, returns leftover
    #  Pure byte ops — no string allocation until we have complete lines
    # ─────────────────────────────────────────────────────────────────
    def _split_chunk(leftover: bytearray, raw_chunk: bytes) -> tuple[list[str], bytearray]:
        """Append chunk, extract complete lines as strings, return leftover bytes.
        Uses extend() instead of += to avoid copying the entire leftover buffer
        on every chunk — critical for sustained GOD MODE throughput."""
        leftover.extend(raw_chunk)
        last_nl = leftover.rfind(b"\n")
        if last_nl == -1:
            return [], leftover
        complete     = bytes(leftover[:last_nl + 1])
        new_leftover = bytearray(leftover[last_nl + 1:])
        text  = complete.decode("utf-8", errors="replace")
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines = lines[:-1]
        return lines, new_leftover

    # ─────────────────────────────────────────────────────────────────
    #  PATH A: Range-split parallel download
    #  Key change: await _process_async() so the event loop yields
    #  control to other download coroutines while lines are processed.
    # ─────────────────────────────────────────────────────────────────
    async def _download_part(part_id: int, start: int, end: int,
                             sem: asyncio.Semaphore) -> None:
        range_size   = end - start + 1
        part_headers = {**base_headers, "Range": f"bytes={start}-{end}"}
        timeout = aiohttp.ClientTimeout(total=None,
                                        connect=conn_timeout,
                                        sock_read=read_timeout)
        for attempt in range(1, max_retries + 1):
            if job.stopped:
                return
            try:
                # ⚡ GOD MODE: semaphore only gates the TCP connect() syscall.
                # We open the response object under the lock, then release
                # immediately — streaming happens at full concurrency with zero
                # semaphore contention. This is the fix for the "initial peak
                # then crawl to 0.2 MB/s" pattern: the old code held the sem
                # for the full stream duration, serialising all 32 parts.
                async with sem:
                    resp = await session.get(
                        url, headers=part_headers,
                        timeout=timeout, ssl=False,
                        allow_redirects=True,
                        raise_for_status=False,
                    )
                    # Verify status while still under sem so a bad status
                    # doesn't consume a slot for no reason
                    if resp.status not in (200, 206):
                        await resp.release()
                        log.warning(f"[Link {idx}] Part {part_id}: "
                                    f"HTTP {resp.status} (attempt {attempt})")
                        if attempt < max_retries:
                            await asyncio.sleep(calculate_backoff(attempt))
                        continue

                # Semaphore fully released — stream at full concurrency ⚡
                try:
                    leftover      = bytearray()
                    range_written = 0

                    async for raw_chunk in resp.content.iter_chunked(chunk_size):
                        if job.stopped:
                            return
                        remaining = range_size - range_written
                        if len(raw_chunk) > remaining:
                            raw_chunk = raw_chunk[:remaining]
                        if not raw_chunk:
                            break

                        clen = len(raw_chunk)
                        range_written        += clen
                        bytes_done[0]        += clen
                        job.total_bytes_done += clen

                        lines, leftover = _split_chunk(leftover, raw_chunk)
                        if lines:
                            _process_async(lines)

                        if range_written >= range_size:
                            break

                    if leftover:
                        tail = leftover.decode("utf-8", errors="replace").split("\n")
                        if tail:
                            _process_async(tail)
                finally:
                    await resp.release()
                return  # success

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[Link {idx}] Part {part_id} attempt {attempt}: "
                             f"{type(e).__name__}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(calculate_backoff(attempt))

        log.error(f"[Link {idx}] Part {part_id} FAILED after {max_retries} attempts — skipping")

    # ─────────────────────────────────────────────────────────────────
    #  PATH B: Single-stream fallback
    # ─────────────────────────────────────────────────────────────────
    async def _download_stream_fallback() -> None:
        timeout = aiohttp.ClientTimeout(total=None,
                                        connect=conn_timeout,
                                        sock_read=read_timeout)
        for attempt in range(1, max_retries + 1):
            if job.stopped:
                return
            try:
                async with session.get(url, headers=base_headers,
                                       timeout=timeout, ssl=False,
                                       allow_redirects=True) as resp:
                    cl = int(resp.headers.get("Content-Length", 0))
                    if cl and not job.total_bytes_expected:
                        job.total_bytes_expected += cl

                    leftover = bytearray()
                    async for raw_chunk in resp.content.iter_chunked(chunk_size):
                        if job.stopped:
                            return
                        clen = len(raw_chunk)
                        bytes_done[0]        += clen
                        job.total_bytes_done += clen
                        lines, leftover = _split_chunk(leftover, raw_chunk)
                        if lines:
                            _process_async(lines)

                    if leftover:
                        tail = leftover.decode("utf-8", errors="replace").split("\n")
                        if tail:
                            _process_async(tail)
                    return  # success

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[Link {idx}] Stream fallback attempt {attempt}: "
                             f"{type(e).__name__}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(calculate_backoff(attempt))

        log.error(f"[Link {idx}] Stream fallback FAILED after {max_retries} attempts")

    # ─────────────────────────────────────────────────────────────────
    #  Run everything
    # ─────────────────────────────────────────────────────────────────
    writer_task   = asyncio.create_task(_writer())
    reporter_task = asyncio.create_task(_progress_reporter())
    try:
        if range_supported:
            # Build parts list
            part_size = max(file_size // n_parts, 1)
            parts = []
            for i in range(n_parts):
                s = i * part_size
                e = (s + part_size - 1) if i < n_parts - 1 else (file_size - 1)
                parts.append((i, s, e))

            sem = asyncio.Semaphore(max_workers)
            # For GOD mode, all 32 parts connect simultaneously — semaphore
            # only gates the TCP handshake (fast), not the stream (slow).
            # Setting it to max_workers means all parts race to connect at once.
            await asyncio.gather(*[_download_part(pid, s, e, sem)
                                   for pid, s, e in parts])
        else:
            await _download_stream_fallback()

        # Drain all in-flight scan futures before signalling writers
        await _drain_scans()
        # Signal all per-keyword writers to flush + exit
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        await writer_task

        # Update global downloaded counter
        _db["stats"]["total_bytes_downloaded"] = (
            _db["stats"].get("total_bytes_downloaded", 0) + bytes_done[0]
        )
        await _asave()

        await _update_card(
            job, 100, 0.0, "✅ Done",
            job.total_bytes_done / 1_048_576,
            job.total_bytes_expected / 1_048_576,
            f"Link {idx} complete ✅",
            force=True,
        )
        log.info(f"[Link {idx}] Streaming scan done — "
                 f"{sum(hits.values())} hits, {bytes_done[0]/1_048_576:.1f} MB")
        return hits, result_paths

    except asyncio.CancelledError:
        await _drain_scans()
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        try:
            await writer_task
        except Exception:
            pass
        raise
    except Exception as e:
        log.error(f"[Link {idx}] Fatal streaming error: {e}\n{traceback.format_exc()}")
        await _drain_scans()
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        try:
            await writer_task
        except Exception:
            pass
        await _update_card(
            job, 0, 0.0, "—",
            job.total_bytes_done / 1_048_576,
            job.total_bytes_expected / 1_048_576,
            f"Link {idx}: error — {e}",
        )
        return hits, result_paths
    finally:
        reporter_stop.set()
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass
        _thread_pool.shutdown(wait=False)

# ─────────────────────────────────────────────────────────────────────
#  Cleanup
# ─────────────────────────────────────────────────────────────────────
def _sync_cleanup(temp_files: list) -> int:
    removed = 0
    for p in temp_files:
        try:
            pp = Path(p)
            if pp.exists():
                pp.unlink()
                removed += 1
        except Exception as e:
            log.warning(f"Cleanup {p}: {e}")
    return removed

async def cleanup_job(job: Job):
    removed = await asyncio.get_event_loop().run_in_executor(
        None, _sync_cleanup, list(job.temp_files)
    )
    log.info(f"Cleanup done — {removed} files for uid={job.uid}")

# ─────────────────────────────────────────────────────────────────────
#  Batch splitter & sender
# ─────────────────────────────────────────────────────────────────────
BATCH_SIZE_BYTES = 45 * 1024 * 1024

async def split_and_send(bot, chat_id: int, result_path: Path,
                         keywords: list[str], idx: int, tier: str,
                         hits: dict, job: Job):
    total_hits = sum(hits.values())
    kw_str = ", ".join(keywords)
    tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"
    file_size = result_path.stat().st_size if result_path.exists() else 0

    if file_size == 0:
        await safe_send(bot, chat_id,
            f"ℹ️ *Link {idx}:* No matches found.", parse_mode=ParseMode.MARKDOWN)
        return

    if file_size <= BATCH_SIZE_BYTES:
        cap = (
            f"📂 {tier_badge} RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keywords: `{kw_str}`\n"
            f"💥 Total Hits: `{total_hits:,}`\n"
            f"📦 Batch: 1/1"
        )
        await safe_send_doc(bot, chat_id, result_path, caption=cap)
        return

    loop = asyncio.get_event_loop()

    def _sync_split() -> list[Path]:
        paths: list[Path] = []
        buf: list[str] = []
        buf_size = 0
        bi = 0
        with open(result_path, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                buf.append(line)
                buf_size += len(line.encode("utf-8"))
                if buf_size >= BATCH_SIZE_BYTES:
                    bi += 1
                    bp = result_path.with_suffix(f".batch{bi}.txt")
                    with open(bp, "w", encoding="utf-8") as fout:
                        fout.writelines(buf)
                    paths.append(bp)
                    buf.clear()
                    buf_size = 0
            if buf:
                bi += 1
                bp = result_path.with_suffix(f".batch{bi}.txt")
                with open(bp, "w", encoding="utf-8") as fout:
                    fout.writelines(buf)
                paths.append(bp)
        return paths

    batch_paths = await loop.run_in_executor(None, _sync_split)
    for bp in batch_paths:
        job.temp_files.append(bp)

    total_batches = len(batch_paths)
    send_sem = asyncio.Semaphore(3)

    async def _send_one(bi: int, bp: Path):
        cap = (
            f"📂 {tier_badge} RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keywords: `{kw_str}`\n"
            f"💥 Total Hits: `{total_hits:,}`\n"
            f"📦 Batch: `{bi}/{total_batches}`"
        )
        async with send_sem:
            await safe_send_doc(bot, chat_id, bp, caption=cap)

    await asyncio.gather(*[_send_one(bi, bp) for bi, bp in enumerate(batch_paths, 1)])


async def merge_and_send(bot, chat_id: int, job: Job, tier: str):
    """
    Single-file mode: merge ALL links for ALL keywords into ONE file per keyword.
    Each keyword gets its own merged file sent separately.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"

    for kw in job.keywords:
        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
        merged_path = RESULTS_DIR / f"merged_{job.uid}_{safe_kw[:60]}_{ts}.txt"
        job.temp_files.append(merged_path)

        kw_hits = 0
        with open(merged_path, "w", encoding="utf-8") as fout:
            for i, info in sorted(job.results.items()):
                if info["status"] == "done":
                    # Try new per-keyword path first, fall back to legacy
                    rp_str = info.get("result_paths", {}).get(kw) or info.get("result_path")
                    if rp_str and Path(rp_str).exists():
                        kw_hits += info.get("hits", {}).get(kw, 0)
                        with open(rp_str, "r", encoding="utf-8", errors="replace") as fin:
                            for line in fin:
                                fout.write(line)

        file_size = merged_path.stat().st_size if merged_path.exists() else 0
        if file_size == 0:
            await safe_send(bot, chat_id,
                f"ℹ️ No matches found for keyword `{kw}` across all links.",
                parse_mode=ParseMode.MARKDOWN)
            continue

        cap = (
            f"📂 {tier_badge} MERGED RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keyword: `{kw}`\n"
            f"💥 Total Hits: `{kw_hits:,}`\n"
            f"📦 All Links Combined"
        )
        if file_size <= BATCH_SIZE_BYTES:
            await safe_send_doc(bot, chat_id, merged_path, caption=cap)
        else:
            dummy_hits = {kw: kw_hits}
            await split_and_send(bot, chat_id, merged_path, [kw], 0, tier, dummy_hits, job)


async def process_link(session, url: str, idx: int, job: Job):
    """
    Process a single URL: stream-download + scan in one pass.
    Calls _process_link_streaming which handles splitting, parallel parts,
    early filtering, and immediate result writing.
    """
    if job.stopped:
        return

    hits, result_paths = await _process_link_streaming(session, url, idx, job)

    job.results[idx] = {
        "status": "done",
        "hits": hits,
        # result_paths is now a dict {keyword: Path}
        "result_paths": {kw: str(p) for kw, p in result_paths.items()} if isinstance(result_paths, dict) else {},
        # keep legacy key for crash-resume compatibility
        "result_path": str(list(result_paths.values())[0]) if isinstance(result_paths, dict) and result_paths else None,
    }
    job.links_done += 1
    await _update_card(
        job, 100, 0.0, "—",
        job.total_bytes_done / 1_048_576,
        job.total_bytes_expected / 1_048_576,
        f"Link {idx} complete ✅ — {job.links_done}/{job.links_total} done",
        force=True,
    )


async def send_completed_results(job: Job):
    tier = "vip" if job.vip else "free"
    done_results = {i: info for i, info in job.results.items() if info["status"] == "done"}

    if not done_results:
        await safe_send(job.bot, job.chat_id,
            "ℹ️ No completed results to send.", parse_mode=ParseMode.MARKDOWN)
        return

    if job.single_file:
        await merge_and_send(job.bot, job.chat_id, job, tier)
    else:
        # Separate files mode: one merged file per keyword (all links combined per keyword)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"
        for kw in job.keywords:
            safe_kw = re.sub(r'[^\w]', '', kw) or "results"
            merged_kw_path = RESULTS_DIR / f"kw_{safe_kw[:60]}_{job.uid}_{ts}.txt"
            job.temp_files.append(merged_kw_path)

            kw_hits = 0
            with open(merged_kw_path, "w", encoding="utf-8") as fout:
                for i, info in sorted(done_results.items()):
                    rp_str = info.get("result_paths", {}).get(kw) or info.get("result_path")
                    if rp_str and Path(rp_str).exists():
                        kw_hits += info.get("hits", {}).get(kw, 0)
                        with open(rp_str, "r", encoding="utf-8", errors="replace") as fin:
                            for line in fin:
                                fout.write(line)

            file_size = merged_kw_path.stat().st_size if merged_kw_path.exists() else 0
            if file_size == 0:
                await safe_send(job.bot, job.chat_id,
                    f"ℹ️ No matches found for keyword `{kw}`.",
                    parse_mode=ParseMode.MARKDOWN)
                continue

            cap = (
                f"📂 {tier_badge} RESULTS\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Keyword: `{kw}`\n"
                f"💥 Total Hits: `{kw_hits:,}`\n"
                f"📦 All Links Combined"
            )
            if file_size <= BATCH_SIZE_BYTES:
                await safe_send_doc(job.bot, job.chat_id, merged_kw_path, caption=cap)
            else:
                dummy_hits = {kw: kw_hits}
                await split_and_send(job.bot, job.chat_id, merged_kw_path, [kw], 0, tier, dummy_hits, job)


async def run_job(job: Job):
    global _current_job
    _current_job = job
    _db["stats"]["total_scans"] += 1
    _db["stats"]["total_users"].add(job.uid)
    await _asave()

    # ── Persist job to disk so it can resume after a crash ───────────
    _save_pending_job(job)

    total_links = len([u for u in job.links if u.strip()])
    job.links_total = total_links
    job.live_hits = {k: 0 for k in job.keywords}
    job._card_lock = asyncio.Lock()

    init_card = _build_stats_card(job, 0, 0.0, "—", 0.0, 0.0, "Starting…")
    card_msg = await safe_send(job.bot, job.chat_id, init_card, parse_mode=ParseMode.MARKDOWN)
    if card_msg:
        job.live_stats_mid = card_msg.message_id

    # GOD MODE connector — tuned for sustained high throughput:
    #   • limit=128: 32 active streams + generous headroom for reconnects
    #   • limit_per_host=96: all 32 parts + retries open freely, no queuing
    #   • tcp_nodelay=True: disables Nagle — sends chunks immediately instead
    #     of buffering, eliminating the 40ms Nagle delay that causes speed dips
    #   • keepalive_timeout=180: keeps connections warm between retries
    #   • ttl_dns_cache=3600: DNS lookup cached for whole job — no repeated
    #     DNS latency adding jitter to reconnects
    if job.god:
        connector = aiohttp.TCPConnector(
            limit=128,
            limit_per_host=96,
            ttl_dns_cache=3600,
            force_close=False,
            enable_cleanup_closed=True,
            keepalive_timeout=180,
            # tcp_nodelay removed — deprecated in newer aiohttp (now always enabled by default)
        )
    elif job.vip:
        connector = aiohttp.TCPConnector(
            limit=32,
            limit_per_host=16,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
            keepalive_timeout=30,
        )
    else:
        connector = aiohttp.TCPConnector(
            limit=16,
            limit_per_host=8,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            read_bufsize=1024 * 1024 * 4,   # 4 MB kernel read buffer — matches OS TCP window
        ) as session:
            valid_links = [(i + 1, url.strip()) for i, url in enumerate(job.links) if url.strip()]

            for link_num, (idx, url) in enumerate(valid_links, 1):
                if job.stopped:
                    break
                # ── Skip links already completed before a crash ──────
                if idx in job.results and job.results[idx].get("status") == "done":
                    log.info(f"Resuming: skipping link {idx} (already done)")
                    continue
                if _db.get("locked", False):
                    await _update_card(job, 0, 0.0, "—",
                                       job.total_bytes_done / 1_048_576,
                                       job.total_bytes_expected / 1_048_576,
                                       "🔒 Bot locked by admin — scan stopped")
                    job.stopped = True
                    break
                if link_num > 1:
                    await _update_card(job, 0, 0.0, "—",
                                       job.total_bytes_done / 1_048_576,
                                       job.total_bytes_expected / 1_048_576,
                                       f"Starting link {idx} ({link_num}/{len(valid_links)})…")
                await process_link(session, url, idx, job)
                # ── Save progress after each link completes ──────────
                _save_pending_job(job)

        if job.stopped:
            done_count = sum(1 for i in job.results.values() if i["status"] == "done")
            await _update_card(job, 0, 0.0, "—",
                               job.total_bytes_done / 1_048_576,
                               job.total_bytes_expected / 1_048_576,
                               f"🛑 Stopped — {done_count}/{job.links_total} links done · sending results…")
            await send_completed_results(job)
            await safe_send(job.bot, job.chat_id,
                "✅ *Done. Results sent above.*\n\nSend /start to run another scan.",
                parse_mode=ParseMode.MARKDOWN)
        else:
            done_mb = job.total_bytes_done / 1_048_576
            exp_mb = job.total_bytes_expected / 1_048_576
            await _update_card(job, 100, 0.0, "✅ Complete",
                               done_mb, exp_mb,
                               f"All {job.links_total} links done · sending results…")

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            spath = RESULTS_DIR / f"summary_{job.uid}_{ts}.txt"
            job.temp_files.append(spath)
            with open(spath, "w", encoding="utf-8") as f:
                f.write("ZY Combo Scanner — Scan Report\n")
                f.write(f"Date     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Keywords : {', '.join(job.keywords)}\n")
                f.write(f"Total DL : {_fmt(done_mb)}\n")
                f.write(f"Tier     : {'VIP' if job.vip else 'Free'}\n\n")
                for i, info in sorted(job.results.items()):
                    f.write(f"Link {i} — {info['status'].upper()}\n")
                    for kw, cnt in info.get("hits", {}).items():
                        f.write(f"  {kw}: {cnt:,} hits\n")
                    f.write("\n")

            # Delegate to send_completed_results which handles both modes correctly
            await send_completed_results(job)

            await safe_send_doc(job.bot, job.chat_id, spath, caption="📋 Scan Summary")
            await safe_send(job.bot, job.chat_id,
                "✅ *All done! Results sent above.*\n\nSend /start to run another scan.",
                parse_mode=ParseMode.MARKDOWN)

    except asyncio.CancelledError:
        await _update_card(job, 0, 0.0, "—",
                           job.total_bytes_done / 1_048_576,
                           job.total_bytes_expected / 1_048_576,
                           "🛑 Scan cancelled — sending completed results…")
        await send_completed_results(job)
    except Exception as e:
        log.error(traceback.format_exc())
        await _update_card(job, 0, 0.0, "—",
                           job.total_bytes_done / 1_048_576,
                           job.total_bytes_expected / 1_048_576,
                           f"💥 Fatal error: {e}")
    finally:
        _clear_pending_job()
        await cleanup_job(job)
        _current_job = None
        _running_tasks.pop(job.uid, None)
        user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
        if not _job_queue.empty():
            try:
                nj = _queue_list[0] if _queue_list else None
                if nj:
                    await safe_send(nj.bot, nj.chat_id,
                        "🔔 *Queue moved — your scan is starting now!*",
                        parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass

# ─────────────────────────────────────────────────────────────────────
#  Queue worker
# ─────────────────────────────────────────────────────────────────────
async def _watchdog_queue_worker():
    while True:
        task = asyncio.create_task(queue_worker(), name="queue_worker")
        try:
            await task
            log.info("queue_worker exited cleanly — watchdog stopping.")
            return
        except asyncio.CancelledError:
            log.info("queue_watchdog cancelled — stopping.")
            task.cancel()
            return
        except Exception as e:
            log.error(f"queue_worker crashed: {e}\n{traceback.format_exc()}")
            log.warning("Restarting queue_worker in 3 seconds…")
            await asyncio.sleep(3)


async def queue_worker():
    while True:
        try:
            job = await _job_queue.get()
        except asyncio.CancelledError:
            log.warning("queue_worker: received shutdown CancelledError — exiting.")
            return
        except Exception as e:
            log.error(f"queue_worker: error waiting for job: {e}")
            await asyncio.sleep(1)
            continue

        if job in _queue_list:
            _queue_list.remove(job)

        try:
            task = asyncio.create_task(run_job(job))
            _running_tasks[job.uid] = task
            job.cancel_task = task
            await task
        except asyncio.CancelledError:
            log.warning(f"queue_worker: run_job for uid={job.uid} was cancelled — continuing queue.")
            _current_job = None
            _running_tasks.pop(job.uid, None)
            user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
            try:
                await safe_send(job.bot, job.chat_id,
                    "⚠️ *Scan was interrupted.* Use /start to try again.",
                    parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        except Exception as e:
            log.error(f"queue_worker: run_job unhandled error for uid={job.uid}: {e}\n{traceback.format_exc()}")
            try:
                await safe_send(job.bot, job.chat_id,
                    f"💥 *Scan crashed unexpectedly.*\n\n`{e}`\n\nUse /start to try again.",
                    parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
            _current_job = None
            _running_tasks.pop(job.uid, None)
            user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
        finally:
            _job_queue.task_done()

def _queue_pos(uid: int):
    for i, j in enumerate(_queue_list):
        if j.uid == uid:
            return i + 1
    return None

def _in_queue(uid: int) -> bool:
    return any(j.uid == uid for j in _queue_list)

# ─────────────────────────────────────────────────────────────────────
#  Keyboards
# ─────────────────────────────────────────────────────────────────────
def server_keyboard(uid: int) -> InlineKeyboardMarkup:
    buttons = []
    queue_len = len(_queue_list)
    if _db.get("free_enabled", True):
        buttons.append([InlineKeyboardButton(
            f"🟢 Free Server (Queue: {queue_len})",
            callback_data="server:free")])
    if is_vip(uid):
        buttons.append([InlineKeyboardButton(
            f"⭐ VIP Server 2 (Queue: {queue_len})",
            callback_data="server:vip")])
    if is_god(uid):
        buttons.append([InlineKeyboardButton(
            f"⚡ GOD MODE Server (Queue: {queue_len})",
            callback_data="server:god")])
    return InlineKeyboardMarkup(buttons)

def savemode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Single file (merged)", callback_data="save:single")],
        [InlineKeyboardButton("📂 Separate files per link", callback_data="save:separate")],
    ])

# ─────────────────────────────────────────────────────────────────────
#  Commands
# ─────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    _db["stats"]["total_users"].add(uid)
    await _asave()

    if _in_queue(uid):
        pos = _queue_pos(uid)
        await update.message.reply_text(
            f"⏳ You're queued at *#{pos}*. Use /cancel to leave.",
            parse_mode=ParseMode.MARKDOWN)
        return
    if _current_job and _current_job.uid == uid:
        await update.message.reply_text("⏳ Your scan is running. Use /stop to stop it.")
        return

    # ── Stale session guard (bot restarted mid-flow) ─────────────────
    current_step = user_state[uid].get("step", "idle")
    if current_step not in ("idle",) and not _in_queue(uid):
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}

    missing = await check_joined_channels(ctx.bot, uid)
    if missing:
        ch_lines = "\n".join(f"• `{c}`" for c in missing)
        keyboard = await build_channel_join_keyboard(missing)
        await update.message.reply_text(
            f"📢 *Join Required Channel(s)*\n\n"
            f"{ch_lines}\n\n"
            f"Join the channel(s) above, then click *Check Membership*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard)
        return

    user_state[uid] = {"step": "awaiting_links", "links": [], "keywords": []}
    await update.message.reply_text(
        "📎 Send your download link(s) (comma-separated):\n\n"
        "Example: `https://cdn1.example.xyz/file1.txt`",
        parse_mode=ParseMode.MARKDOWN)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _current_job and _current_job.uid == uid:
        _current_job.stopped = True
        await update.message.reply_text(
            "🛑 *Stop requested!*\n\n"
            "Current link will finish processing, then results will be sent.\n"
            "Please wait…",
            parse_mode=ParseMode.MARKDOWN)
        return
    if _in_queue(uid):
        for j in list(_queue_list):
            if j.uid == uid:
                _queue_list.remove(j)
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Removed from queue. No results to send (scan hadn't started).")
        return
    await update.message.reply_text("Nothing is running. Send /start to begin.")

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _in_queue(uid):
        for j in list(_queue_list):
            if j.uid == uid:
                _queue_list.remove(j)
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Removed from queue. Send /start to try again.")
        return
    if _current_job and _current_job.uid == uid:
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Cancellation requested. Use /stop if you want to receive completed results.")
        return
    await update.message.reply_text("Nothing to cancel. Send /start to begin.")

async def cmd_queue(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lines = ["📋 *Queue Status*\n"]
    if _current_job:
        you = " ← *you*" if _current_job.uid == uid else ""
        lines.append(f"▶️ Running: `{_current_job.uid}`{you}")
    else:
        lines.append("✅ Nothing running.")
    if _queue_list:
        lines.append(f"\n⏳ Waiting ({len(_queue_list)}):")
        for i, j in enumerate(_queue_list):
            you = " ← *you*" if j.uid == uid else ""
            lines.append(f"  {i+1}. `{j.uid}`{you}")
    else:
        lines.append("📭 Queue empty.")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_vipinfo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    entry = _db["vip"].get(str(uid))
    if not entry or not is_vip(uid):
        await update.message.reply_text(
            "❌ You don't have VIP access.\n\nContact admin to upgrade! 👑")
        return
    if entry.get("permanent"):
        label = "♾ Permanent"
    else:
        exp = datetime.fromisoformat(entry["expires"])
        label = f"⏰ Expires: {exp.strftime('%Y-%m-%d %H:%M')} UTC"
    link_limit = _db.get("link_limit")
    lbl = f"Up to {link_limit} links" if link_limit else "Unlimited links"
    await update.message.reply_text(
        f"👑 *VIP Status*\n\n"
        f"✅ Active\n{label}\n\n"
        f"Perks:\n"
        f"• {lbl}\n"
        f"• {N_PARTS_VIP} parallel download parts ({MAX_WORKERS_VIP} active at once)\n"
        f"• 4 MB chunks — ultra-fast streaming\n"
        f"• Real-time streaming scan — no intermediate files\n"
        f"• No size cap",
        parse_mode=ParseMode.MARKDOWN)

def _admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 Admin only.")
            return
        await func(update, ctx)
    return wrapper

@_admin_only
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    s = _db["stats"]
    vip_count = sum(1 for uid in _db["vip"] if is_vip(int(uid)))
    free_st = "✅ ON" if _db.get("free_enabled", True) else "🔴 OFF"
    lock_st = "🔒 LOCKED" if _db.get("locked", False) else "🔓 Unlocked"
    channels = _db.get("required_channels", [])
    channels_str = "\n    • ".join(channels) if channels else "None"
    link_limit = _db.get("link_limit") or "Unlimited"
    free_limit = _db.get("free_size_limit_mb", 500)
    total_gb = _fmt_gb_total()

    await update.message.reply_text(
        f"📊 *Bot Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Total users: `{len(s['total_users'])}`\n"
        f"🔍 Total scans: `{s['total_scans']}`\n"
        f"📥 Total downloaded: `{total_gb}`\n"
        f"👑 Active VIPs: `{vip_count}`\n\n"
        f"🔰 Free server: {free_st}\n"
        f"📏 Free size limit: `{free_limit} MB`\n"
        f"🔗 VIP link limit: `{link_limit}`\n\n"
        f"📢 Required channels:\n    • {channels_str}\n\n"
        f"🔒 Lock state: {lock_st}\n"
        f"⏳ Queue length: `{len(_queue_list)}`\n"
        f"▶️ Running: `{'Yes' if _current_job else 'No'}`",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = " ".join(ctx.args)
    if not msg:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    sent = 0
    for uid in list(_db["stats"]["total_users"]):
        try:
            await ctx.bot.send_message(chat_id=uid,
                text=f"📢 *Announcement*\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await update.message.reply_text(f"✅ Broadcast sent to {sent} user(s).")

@_admin_only
async def cmd_addvip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /addvip <uid> [days]")
        return
    try:
        target = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    days = int(ctx.args[1]) if len(ctx.args) > 1 else None
    if days:
        exp = (datetime.utcnow() + timedelta(days=days)).isoformat()
        _db["vip"][str(target)] = {"expires": exp, "permanent": False}
        label = f"{days} day(s)"
    else:
        _db["vip"][str(target)] = {"permanent": True}
        label = "permanent"
    await _asave()
    await update.message.reply_text(
        f"✅ VIP granted to `{target}` — {label}.", parse_mode=ParseMode.MARKDOWN)
    try:
        await ctx.bot.send_message(chat_id=target,
            text=f"🎉 *You've been upgraded to VIP!*\n\n"
                 f"Duration: {label}\n\nEnjoy unlimited scanning! 👑",
            parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

@_admin_only
async def cmd_rmvip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /rmvip <uid>")
        return
    uid = ctx.args[0]
    if uid in _db["vip"]:
        del _db["vip"][uid]
        await _asave()
        await update.message.reply_text(
            f"✅ VIP removed from `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(
            f"❌ User `{uid}` is not VIP.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_viptrial(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text("Usage: /viptrial <uid> <hours>")
        return
    try:
        target = int(ctx.args[0])
        hours = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return
    exp = (datetime.utcnow() + timedelta(hours=hours)).isoformat()
    _db["vip"][str(target)] = {"expires": exp, "permanent": False}
    await _asave()
    await update.message.reply_text(
        f"✅ VIP trial ({hours}h) granted to `{target}`.", parse_mode=ParseMode.MARKDOWN)
    try:
        await ctx.bot.send_message(chat_id=target,
            text=f"🎉 *VIP Trial activated!*\n\n⏰ Duration: {hours} hour(s)\n\nEnjoy! 👑",
            parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

@_admin_only
async def cmd_subscribers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    vips = [(uid, entry) for uid, entry in _db["vip"].items() if is_vip(int(uid))]
    if not vips:
        await update.message.reply_text("No active VIP users.")
        return
    lines = ["👑 *Active VIP Users*\n"]
    for uid, entry in vips:
        if entry.get("permanent"):
            label = "♾ Permanent"
        else:
            exp = datetime.fromisoformat(entry["expires"])
            label = f"expires {exp.strftime('%Y-%m-%d')}"
        lines.append(f"• `{uid}` — {label}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_addfreeserver(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["free_enabled"] = True
    await _asave()
    await update.message.reply_text("✅ Free server is now *ON*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_rmfreeserver(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["free_enabled"] = False
    await _asave()
    await update.message.reply_text("🔴 Free server is now *OFF*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_addrequiredchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /addrequiredchannel @channelname")
        return
    ch = ctx.args[0]
    if ch not in _db["required_channels"]:
        _db["required_channels"].append(ch)
        await _asave()
    channels_list = ", ".join(f"`{c}`" for c in _db["required_channels"])
    await update.message.reply_text(
        f"✅ Required channel added: `{ch}`\n\n"
        f"📋 Current required channels:\n{channels_list}",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_rmrequiredchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /rmrequiredchannel @channelname")
        return
    ch = ctx.args[0]
    if ch in _db["required_channels"]:
        _db["required_channels"].remove(ch)
        await _asave()
        channels_list = ", ".join(f"`{c}`" for c in _db["required_channels"]) if _db["required_channels"] else "None"
        await update.message.reply_text(
            f"✅ Removed: `{ch}`\n\n"
            f"📋 Remaining required channels:\n{channels_list}",
            parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Not found: `{ch}`", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_lockall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["locked"] = True
    await _asave()
    if _current_job:
        _current_job.stopped = True
        await safe_send(ctx.bot, _current_job.chat_id,
            "🔒 *Bot locked by admin.* Your scan has been stopped.\n"
            "Sending any completed results…",
            parse_mode=ParseMode.MARKDOWN)
    for j in list(_queue_list):
        _queue_list.remove(j)
        user_state[j.uid] = {"step": "idle", "links": [], "keywords": []}
        try:
            await safe_send(ctx.bot, j.chat_id,
                "🔒 *Bot locked by admin.* You've been removed from the queue.",
                parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
    await update.message.reply_text(
        "🔒 *Bot locked!*\n\n"
        "All running scans stopped. Queue cleared.\n"
        "Users cannot start new scans until /unlockall.",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_unlockall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["locked"] = False
    await _asave()
    await update.message.reply_text(
        "🔓 *Bot unlocked!*\n\nUsers can now start new scans.",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_setlinklimit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        current = _db.get("link_limit") or "Unlimited"
        await update.message.reply_text(
            f"📏 *Current VIP link limit:* `{current}`\n\n"
            "Usage: `/setlinklimit <number>` (0 = unlimited)",
            parse_mode=ParseMode.MARKDOWN)
        return
    try:
        n = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Usage: /setlinklimit <number>")
        return
    if n <= 0:
        _db["link_limit"] = None
        await _asave()
        await update.message.reply_text("✅ VIP link limit removed — *Unlimited*.", parse_mode=ParseMode.MARKDOWN)
    else:
        _db["link_limit"] = n
        await _asave()
        await update.message.reply_text(
            f"✅ VIP link limit set to *{n} links*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_setfreeserverlimit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        current = _db.get("free_size_limit_mb", 500)
        await update.message.reply_text(
            f"📏 *Current free server limit:* `{current} MB`\n\n"
            "Usage: `/setfreeserverlimit 2GB` or `/setfreeserverlimit 500MB`",
            parse_mode=ParseMode.MARKDOWN)
        return
    raw = ctx.args[0].upper().strip()
    try:
        if raw.endswith("GB"):
            mb = float(raw[:-2]) * 1024
        elif raw.endswith("MB"):
            mb = float(raw[:-2])
        else:
            mb = float(raw)
    except ValueError:
        await update.message.reply_text("❌ Invalid format. Examples: `2GB`, `500MB`, `1024`")
        return
    _db["free_size_limit_mb"] = mb
    await _asave()
    label = f"{mb/1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"
    await update.message.reply_text(
        f"✅ Free server size limit set to *{label}*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_genkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "❌ Usage: `/genkey <duration> [maxuser]`\n\n"
            "Duration examples: `30d` `12h` `45m` `permanent`\n"
            "MaxUser (optional): how many users can redeem (default: 1)",
            parse_mode=ParseMode.MARKDOWN)
        return
    duration_raw = ctx.args[0].lower().strip()
    max_users = 1
    if len(ctx.args) > 1:
        try:
            max_users = int(ctx.args[1])
            if max_users < 1:
                max_users = 1
        except ValueError:
            await update.message.reply_text("❌ maxuser must be a number.")
            return
    minutes = None
    hours = None
    days = None
    seconds_total = None
    label = ""
    if duration_raw in ("permanent", "perm", "∞"):
        label = "Permanent"
    elif duration_raw.endswith("d"):
        try:
            days = int(duration_raw[:-1])
            seconds_total = days * 86400
            label = f"{days} day(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    elif duration_raw.endswith("h"):
        try:
            hours = int(duration_raw[:-1])
            seconds_total = hours * 3600
            label = f"{hours} hour(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    elif duration_raw.endswith("m"):
        try:
            minutes = int(duration_raw[:-1])
            seconds_total = minutes * 60
            label = f"{minutes} minute(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    else:
        await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
        return
    if "vip_keys" not in _db:
        _db["vip_keys"] = {}
    chars = string.ascii_uppercase + string.digits
    raw_k = "".join(secrets.choice(chars) for _ in range(12))
    key = f"ZY-{raw_k[:4]}-{raw_k[4:8]}-{raw_k[8:12]}"
    _db["vip_keys"][key] = {
        "seconds": seconds_total,
        "permanent": seconds_total is None,
        "max_users": max_users,
        "used_count": 0,
        "used_by": [],
        "created": datetime.utcnow().isoformat(),
    }
    await _asave()
    max_lbl = f"{max_users} user(s)" if max_users > 1 else "1 user (one-time)"
    await update.message.reply_text(
        f"🔑 *VIP Key Generated!*\n\n"
        f"`{key}`\n\n"
        f"⏰ Duration: *{label}*\n"
        f"👥 Max redeems: *{max_lbl}*\n\n"
        f"Share with user(s). They redeem with:\n`/redeem {key}`",
        parse_mode=ParseMode.MARKDOWN)


async def cmd_redeem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args:
        await update.message.reply_text(
            "❌ Usage: `/redeem <KEY>`\n\nExample: `/redeem ZY-AB12-CD34-EF56`",
            parse_mode=ParseMode.MARKDOWN)
        return
    key = ctx.args[0].upper().strip()
    keys = _db.get("vip_keys", {})
    if key not in keys:
        await update.message.reply_text("❌ Invalid key. Check it and try again.")
        return
    entry = keys[key]
    max_users = entry.get("max_users", 1)
    used_count = entry.get("used_count", 0)
    used_by = entry.get("used_by", [])
    if used_count >= max_users:
        await update.message.reply_text("❌ This key has reached its maximum redemptions.")
        return
    if uid in used_by:
        await update.message.reply_text("❌ You have already redeemed this key.")
        return
    seconds = entry.get("seconds")
    permanent = entry.get("permanent", False)
    if permanent or seconds is None:
        _db["vip"][str(uid)] = {"permanent": True}
        label = "Permanent"
    else:
        exp = (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
        _db["vip"][str(uid)] = {"expires": exp, "permanent": False}
        if seconds >= 86400:
            label = f"{seconds // 86400} day(s)"
        elif seconds >= 3600:
            label = f"{seconds // 3600} hour(s)"
        else:
            label = f"{seconds // 60} minute(s)"
    entry["used_count"] = used_count + 1
    entry["used_by"] = used_by + [uid]
    entry[f"used_at_{uid}"] = datetime.utcnow().isoformat()
    if max_users == 1:
        entry["used"] = True
        entry["used_by_legacy"] = uid
    await _asave()
    await update.message.reply_text(
        f"🎉 *VIP Activated!*\n\n"
        f"✅ Key accepted\n"
        f"⏰ Duration: *{label}*\n\n"
        f"You now have:\n"
        f"• Unlimited links\n"
        f"• {N_PARTS_VIP} download parts, {MAX_WORKERS_VIP} active at once\n"
        f"• Real-time streaming scan — no intermediate files\n"
        f"• No size cap 👑",
        parse_mode=ParseMode.MARKDOWN)


@_admin_only
async def cmd_listkeys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keys = _db.get("vip_keys", {})
    active = [(k, v) for k, v in keys.items()
              if v.get("used_count", 0) < v.get("max_users", 1)]
    if not active:
        await update.message.reply_text("📭 No active (redeemable) VIP keys.")
        return
    lines = [f"🔑 *Active VIP Keys* ({len(active)})\n"]
    for k, v in active:
        seconds = v.get("seconds")
        permanent = v.get("permanent", False)
        if permanent or seconds is None:
            dur = "♾ Permanent"
        elif seconds >= 86400:
            dur = f"{seconds // 86400}d"
        elif seconds >= 3600:
            dur = f"{seconds // 3600}h"
        else:
            dur = f"{seconds // 60}m"
        used = v.get("used_count", 0)
        maxu = v.get("max_users", 1)
        lines.append(f"• `{k}` — {dur} | {used}/{maxu} used")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@_admin_only
async def cmd_rmkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: `/rmkey <KEY>` or `/rmkey all` or `/rmkey <userid>`",
            parse_mode=ParseMode.MARKDOWN)
        return
    arg = ctx.args[0]
    keys = _db.get("vip_keys", {})
    if arg.lower() == "all":
        count = len(keys)
        _db["vip_keys"] = {}
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted *{count}* VIP key(s).", parse_mode=ParseMode.MARKDOWN)
        return
    key = arg.upper()
    if key in keys:
        del _db["vip_keys"][key]
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted key: `{key}`", parse_mode=ParseMode.MARKDOWN)
        return
    found = []
    for k, v in list(keys.items()):
        if arg in v.get("used_by", []):
            found.append(k)
    if found:
        for k in found:
            del _db["vip_keys"][k]
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted {len(found)} key(s) used by `{arg}`.",
            parse_mode=ParseMode.MARKDOWN)
        return
    await update.message.reply_text(
        f"❌ Key/user not found: `{arg}`", parse_mode=ParseMode.MARKDOWN)


# ─────────────────────────────────────────────────────────────────────
#  Message handler
# ─────────────────────────────────────────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()
    data = user_state[uid]
    step = data.get("step", "idle")

    if step == "idle":
        await update.message.reply_text("Send /start to begin.")
        return

    if step == "awaiting_links":
        # Split on commas/newlines first so comma-joined URLs are each extracted separately
        parts = re.split(r'[,\n]+', text)
        links = []
        seen_links = set()
        for part in parts:
            part = part.strip()
            for url in URL_RE.findall(part):
                url = url.rstrip('.,;)')  # strip trailing punctuation
                if url and url not in seen_links:
                    seen_links.add(url)
                    links.append(url)
        if not links:
            await update.message.reply_text(
                "⚠️ No valid links found. Please send URL(s).")
            return
        data["links"] = links
        data["step"] = "awaiting_keywords"
        await update.message.reply_text(
            "🔗 *Link saved!*\n\n"
            "Enter Target Sites or Keywords (comma-separated).\n"
            "Example: `crunchyroll.com, netflix.com`",
            parse_mode=ParseMode.MARKDOWN)
        return

    if step == "awaiting_keywords":
        keywords = [k.strip() for k in text.split(",") if k.strip()]
        if not keywords:
            await update.message.reply_text(
                "⚠️ No keywords found. Please send at least one.")
            return
        data["keywords"] = keywords
        data["single_file"] = True  # default to merged for multi-link
        await _show_server_selection(update, ctx, uid, data, keywords)

    elif step in ("queued", "processing", "awaiting_server", "awaiting_savemode"):
        pos = _queue_pos(uid)
        if pos:
            await update.message.reply_text(
                f"⏳ Queued at *#{pos}*. Please wait. /cancel to leave.",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(
                "⏳ Scan is running. Please wait. /stop to stop it.")
    else:
        await update.message.reply_text("Send /start to begin.")


async def _show_server_selection(update, ctx, uid, data, keywords):
    free_on = _db.get("free_enabled", True)
    vip_user = is_vip(uid)
    god_user = is_god(uid)
    if not free_on and not vip_user and not god_user:
        await update.message.reply_text(
            "🔴 *Free server is currently offline.*\n\n"
            "Contact admin for VIP access. 👑",
            parse_mode=ParseMode.MARKDOWN)
        data["step"] = "idle"
        return
    data["step"] = "awaiting_server"
    kb = server_keyboard(uid)
    await update.message.reply_text(
        "🖥 *Select a Processing Server:*\n\n"
        "Choose a server with a low queue for the fastest results.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb)

# ─────────────────────────────────────────────────────────────────────
#  Callback handler
# ─────────────────────────────────────────────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = user_state[uid]

    # ── Stale session guard ──────────────────────────────────────────
    step = data.get("step", "idle")
    if step == "idle" and query.data not in ("check_membership",):
        await query.edit_message_text(
            "⚠️ *Session expired* (bot was restarted).\n\n"
            "Send /start to begin a new scan.",
            parse_mode=ParseMode.MARKDOWN)
        return

    if query.data == "check_membership":
        missing = await check_joined_channels(ctx.bot, uid)
        if missing:
            ch_lines = "\n".join(f"• `{c}`" for c in missing)
            keyboard = await build_channel_join_keyboard(missing)
            await query.edit_message_text(
                f"📢 *Still missing required channel(s)*\n\n"
                f"{ch_lines}\n\n"
                f"Join the channel(s) above, then click *Check Membership* again.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard)
        else:
            data["step"] = "awaiting_links"
            data["links"] = []
            data["keywords"] = []
            await query.edit_message_text(
                "✅ *Membership verified!*\n\n"
                "📎 Send your download link(s) (comma-separated):\n\n"
                "Example: `https://cdn1.example.xyz/file1.txt`",
                parse_mode=ParseMode.MARKDOWN)
        return

    if query.data.startswith("save:"):
        if data["step"] != "awaiting_savemode":
            await query.edit_message_text("⚠️ Session expired. Send /start to begin.")
            return
        choice = query.data.split(":")[1]
        data["single_file"] = (choice == "single")
        keywords = data["keywords"]
        free_on = _db.get("free_enabled", True)
        vip_user = is_vip(uid)
        god_user = is_god(uid)
        if not free_on and not vip_user and not god_user:
            await query.edit_message_text(
                "🔴 *Free server is currently offline.*\n\nContact admin for VIP access. 👑",
                parse_mode=ParseMode.MARKDOWN)
            data["step"] = "idle"
            return
        total_gb = _fmt_gb_total()
        data["step"] = "awaiting_server"
        kb = server_keyboard(uid)
        await query.edit_message_text(
            "🖥 *Select a Processing Server:*\n\n"
            "Choose a server with a low queue for the fastest results.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb)
        return

    if not query.data.startswith("server:"):
        return
    if data["step"] != "awaiting_server":
        await query.edit_message_text("⚠️ Session expired. Send /start to begin.")
        return

    if _db.get("locked", False):
        await query.edit_message_text(
            "🔒 *Bot is locked by admin.* Please try again later.",
            parse_mode=ParseMode.MARKDOWN)
        data["step"] = "idle"
        return

    choice = query.data.split(":")[1]
    links = data["links"]
    keywords = data["keywords"]
    single_file = data.get("single_file", False)
    god_mode = (choice == "god")
    vip_mode = (choice == "vip") or god_mode

    if god_mode and not is_god(uid):
        await query.edit_message_text("🚫 God Mode is for admins only.")
        return
    if choice == "vip" and not is_vip(uid):
        await query.edit_message_text("🚫 You don't have VIP access.")
        return
    if not vip_mode and not _db.get("free_enabled", True):
        await query.edit_message_text("🔴 Free server is currently offline.")
        return

    # Determine server label for the confirmation message
    if god_mode:
        server_label = "GOD MODE Server"
    elif vip_mode:
        server_label = "VIP Server 2"
    else:
        server_label = "Free Server"

    # Replace the server selection message with the "Job Sent" confirmation
    await query.edit_message_text(
        f"✅ *Job Sent to {server_label}!*\n\n"
        f"The server will download your file and send you the results directly when finished.\n"
        f"(Type /cancel at any time to abort)",
        parse_mode=ParseMode.MARKDOWN)

    job = Job(uid=uid, chat_id=query.message.chat_id,
              links=links, keywords=keywords, bot=ctx.bot,
              vip=vip_mode, god=god_mode, single_file=single_file)
    data["step"] = "queued"

    async with _queue_lock:
        _queue_list.append(job)
        await _job_queue.put(job)

# ─────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────
def main():
    global _job_queue, _queue_lock
    _job_queue = asyncio.Queue()
    _queue_lock = asyncio.Lock()

    log.info("Starting ZY Combo Scanner Bot v8.5 ULTRA — GOD 50-part/32-worker engine…")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(120)
        .connect_timeout(30)
        .pool_timeout(60)
        .build()
    )

    # User commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("vipinfo", cmd_vipinfo))
    app.add_handler(CommandHandler("redeem", cmd_redeem))

    # Admin commands
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("addvip", cmd_addvip))
    app.add_handler(CommandHandler("rmvip", cmd_rmvip))
    app.add_handler(CommandHandler("viptrial", cmd_viptrial))
    app.add_handler(CommandHandler("subscribers", cmd_subscribers))
    app.add_handler(CommandHandler("addfreeserver", cmd_addfreeserver))
    app.add_handler(CommandHandler("rmfreeserver", cmd_rmfreeserver))
    app.add_handler(CommandHandler("addrequiredchannel", cmd_addrequiredchannel))
    app.add_handler(CommandHandler("rmrequiredchannel", cmd_rmrequiredchannel))
    app.add_handler(CommandHandler("lockall", cmd_lockall))
    app.add_handler(CommandHandler("unlockall", cmd_unlockall))
    app.add_handler(CommandHandler("setlinklimit", cmd_setlinklimit))
    app.add_handler(CommandHandler("setfreeserverlimit", cmd_setfreeserverlimit))
    app.add_handler(CommandHandler("genkey", cmd_genkey))
    app.add_handler(CommandHandler("listkeys", cmd_listkeys))
    app.add_handler(CommandHandler("rmkey", cmd_rmkey))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def post_init(application: Application):
        from telegram import BotCommandScopeChat

        # ── Resume any job that was running before a crash/restart ───
        pending = _load_pending_job()
        if pending:
            log.info(f"Found pending job for uid={pending['uid']} — resuming…")
            try:
                resume_job = Job(
                    uid        = pending["uid"],
                    chat_id    = pending["chat_id"],
                    links      = pending["links"],
                    keywords   = pending["keywords"],
                    bot        = application.bot,
                    vip        = pending.get("vip", False),
                    god        = pending.get("god", False),
                    single_file= pending.get("single_file", True),
                )
                # Mark already-done links so they're skipped
                for done_idx in pending.get("done_indices", []):
                    # Rebuild per-keyword result paths from disk
                    kw_paths = {}
                    for kw in resume_job.keywords:
                        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
                        p = RESULTS_DIR / f"link{done_idx}_{safe_kw[:80]}.txt"
                        if p.exists():
                            kw_paths[kw] = str(p)
                    resume_job.results[done_idx] = {
                        "status": "done",
                        "hits": {},
                        "result_paths": kw_paths,
                        "result_path": list(kw_paths.values())[0] if kw_paths else None,
                    }
                    resume_job.links_done += 1

                await application.bot.send_message(
                    chat_id=pending["chat_id"],
                    text=(
                        "♻️ *Bot restarted — resuming your scan!*\n\n"
                        f"Skipping {len(pending.get('done_indices', []))} already-completed link(s).\n"
                        "Continuing from where it left off…"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                )
                user_state[pending["uid"]]["step"] = "queued"
                async with _queue_lock:
                    _queue_list.append(resume_job)
                    await _job_queue.put(resume_job)
            except Exception as e:
                log.error(f"Failed to resume pending job: {e}\n{traceback.format_exc()}")
                _clear_pending_job()
                try:
                    await application.bot.send_message(
                        chat_id=pending["chat_id"],
                        text=(
                            "⚠️ *Bot restarted* but could not resume your scan automatically.\n\n"
                            "Please send /start to run again. Sorry for the interruption!"
                        ),
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass

        public_commands = [
            BotCommand("start", "Start a new scan"),
            BotCommand("stop", "Stop scan (sends completed results)"),
            BotCommand("cancel", "Cancel / leave queue"),
            BotCommand("queue", "Check queue status"),
            BotCommand("vipinfo", "Check your VIP status"),
            BotCommand("redeem", "Redeem a VIP key"),
        ]
        await application.bot.set_my_commands(public_commands)

        admin_commands = public_commands + [
            BotCommand("stats", "📊 Bot statistics"),
            BotCommand("broadcast", "📢 Broadcast message"),
            BotCommand("addvip", "➕ Add VIP user"),
            BotCommand("rmvip", "➖ Remove VIP user"),
            BotCommand("viptrial", "⏱ Give VIP trial"),
            BotCommand("subscribers", "👑 List VIP users"),
            BotCommand("genkey", "🔑 Generate VIP key"),
            BotCommand("listkeys", "📋 List active keys"),
            BotCommand("rmkey", "🗑 Delete key"),
            BotCommand("addfreeserver", "✅ Enable free server"),
            BotCommand("rmfreeserver", "🔴 Disable free server"),
            BotCommand("lockall", "🔒 Lock bot"),
            BotCommand("unlockall", "🔓 Unlock bot"),
            BotCommand("setlinklimit", "📏 Set VIP link limit"),
            BotCommand("setfreeserverlimit", "📏 Set free server size limit"),
            BotCommand("addrequiredchannel", "📢 Add required channel"),
            BotCommand("rmrequiredchannel", "🗑 Remove required channel"),
        ]
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeChat(chat_id=admin_id)
                )
            except Exception as e:
                log.warning(f"Could not set admin commands for {admin_id}: {e}")

        asyncio.create_task(_watchdog_queue_worker(), name="queue_watchdog")
        log.info("Queue worker started.")

    app.post_init = post_init
    log.info("Bot is running…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

+++ bot.py (修改后)
"""
╔══════════════════════════════════════════════════════════════╗
║      𝙕𝙔 𝘾𝙤𝙢𝙗𝙤 𝙎𝙘𝙖𝙣𝙣𝙚𝙧 𝘽𝙤𝙩  ·  𝙑𝟴.𝟱 𝙐𝙇𝙏𝙍𝘼         ║
╚══════════════════════════════════════════════════════════════╝

Features:
  • Free tier  — 1 link at a time, 10-part split, 500 MB cap (default)
  • VIP tier   — unlimited links, 30-part split, no cap, max speed
  • GOD MODE   — 50-part split, 32 concurrent workers, 8 MB chunks, ULTRA speed
  • Plan-based split download engine:
      FREE → 10 parts (4 active)  VIP → 30 parts (15 active)
      GOD  → 50 parts (32 active)  ⚡ ULTRA
  • Streaming scan — download chunk → filter → write results immediately
    No intermediate raw file stored on disk.
  • Early filter (colon/at check) before full keyword parse
  • Single-writer queue — zero concurrent file-write contention
  • Range-fallback — auto single-stream if server doesn't support ranges
  • Per-part retry (15× GOD / 3× others) with exponential backoff + jitter
  • Live download progress (edited in-place, no spam)
  • /stop command — stop current scan, sends already-processed results
  • /lockall /unlockall — admin: freeze all running scans
  • Admin panel: /stats /broadcast /addvip /rmvip /viptrial
                 /subscribers /addfreeserver /rmfreeserver
                 /addrequiredchannel /rmrequiredchannel
                 /lockall /unlockall /setlinklimit /setfreeserverlimit
                 /genkey (days/hours/minutes) maxuser
                 /rmkey all | /rmkey (userid)
  • Required channel join gate before any scan
  • Keyword-named result files, URL-stripped + deduplicated
  • Ask user before processing: merge into single file or separate?
  • Crash-safe cleanup on Railway / any PaaS
  • Global total GB downloaded counter
  • Railway-optimised: persistent JSON, crash-resume, enlarged Telegram timeouts
"""

import os, re, json, asyncio, aiohttp, aiofiles, logging, traceback, secrets, string
import random
import signal
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

# ─────────────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
#  Config - GOD MODE ULTRA — Railway-Optimised
# ─────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "8661120242:AAEO3UUHVKrcQha_XCGsVA0tJJq3qy5_8YQ")
ADMIN_IDS = {5028065177}
RESULTS_DIR      = Path(os.getenv("RESULTS_DIR",      "results"))
DATA_FILE        = Path(os.getenv("DATA_FILE",        "botdata.json"))
PENDING_JOB_FILE = Path(os.getenv("PENDING_JOB_FILE", "pending_job.json"))

# ── Chunk sizes ─────────────────────────────────────────────────────────
# GOD: 512 KB — semaphore is now released before streaming (not after),
# so all 32 parts stream freely. Larger chunks = fewer syscalls = higher
# sustained throughput. The old 256 KB comment was a workaround for the
# semaphore bug; with that fixed, bigger chunks win.
CHUNK_SIZE_FREE  = 1024 * 128        # 128 KB
CHUNK_SIZE_VIP   = 1024 * 384        # 384 KB
CHUNK_SIZE_GOD   = 1024 * 512        # 512 KB ⚡ max throughput

# Connection pool limits
MAX_PAR_FREE     = 3
MAX_PAR_GOD      = 96               # 32 active + 64 queued headroom

# ── Plan-based split parts ──────────────────────────────────────────────
N_PARTS_FREE = 8    # FREE  → 8 parts
N_PARTS_VIP  = 16   # VIP   → 16 parts
N_PARTS_GOD  = 32   # GOD   → 32 parts ⚡ all run concurrently

# Max concurrent parts
MAX_WORKERS_FREE = 4    # 8 parts, 4 active
MAX_WORKERS_VIP  = 12   # 16 parts, 12 active
MAX_WORKERS_GOD  = 32   # 32 parts, all 32 active simultaneously ⚡

# Legacy thread-count names (kept so vipinfo display still works)
N_THREADS_FREE = N_PARTS_FREE
N_THREADS_VIP  = N_PARTS_VIP
N_THREADS_GOD  = N_PARTS_GOD

FREE_SIZE_LIMIT  = 500 * 1024 * 1024  # 500 MB (overridable via /setfreeserverlimit)

# Retry settings - CRASH SAFE
MAX_RETRIES      = 3
MAX_RETRIES_GOD  = 15               # GOD MODE: Maximum retries for ultra reliability
PROGRESS_STEP    = 5               # % step for live bar update

# Timeout settings (seconds)
CONNECT_TIMEOUT_FREE = 60
CONNECT_TIMEOUT_VIP  = 20
CONNECT_TIMEOUT_GOD  = 8            # Fast fail → fast retry → no long idle gaps

SOCK_READ_TIMEOUT_FREE = 300
SOCK_READ_TIMEOUT_VIP  = 120
# KEY FIX: 15s read timeout for GOD MODE.
# The old 60s value meant a stalled part sat silent for a full minute
# before retrying — that's the "drop to 0.5 MB/s" window you see.
# At 15s, a stalled part fails fast, retries immediately, and the other
# 31 parts keep running at full speed the whole time.
SOCK_READ_TIMEOUT_GOD  = 15

# Exponential backoff settings
BASE_RETRY_DELAY = 0.2              # Start retrying almost immediately
MAX_RETRY_DELAY  = 5.0             # Low cap — don't wait long in GOD MODE
JITTER_RANGE     = 0.2             # Tight jitter to avoid thundering herd

RESULTS_DIR.mkdir(exist_ok=True)

URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────────
#  Persistent data  (JSON — survives Railway restarts)
# ─────────────────────────────────────────────────────────────────────
def _load_data() -> dict:
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            pass
    return {
        "vip": {},
        "vip_keys": {},
        "free_enabled": True,
        "required_channels": [],
        "stats": {"total_scans": 0, "total_users": [], "total_bytes_downloaded": 0},
        "link_limit": None,          # None = unlimited for VIP
        "free_size_limit_mb": 500,   # MB, configurable via /setfreeserverlimit
        "locked": False,             # /lockall state
    }

def _save_data() -> None:
    d = dict(_db)
    d["stats"] = {**d["stats"], "total_users": list(d["stats"]["total_users"])}
    DATA_FILE.write_text(json.dumps(d, indent=2))

async def _asave() -> None:
    """Non-blocking wrapper — runs _save_data in a thread so the event loop never stalls."""
    await asyncio.get_event_loop().run_in_executor(None, _save_data)

# ─────────────────────────────────────────────────────────────────────
#  Pending-job persistence  (crash-resume)
# ─────────────────────────────────────────────────────────────────────
def _save_pending_job(job) -> None:
    """Persist the active job to disk so it can be resumed after a restart."""
    try:
        data = {
            "uid":         job.uid,
            "chat_id":     job.chat_id,
            "links":       job.links,
            "keywords":    job.keywords,
            "vip":         job.vip,
            "god":         job.god,
            "single_file": job.single_file,
            # Track which link indices are already done so we skip them on resume
            "done_indices": [
                idx for idx, info in job.results.items()
                if info.get("status") == "done"
            ],
        }
        PENDING_JOB_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        log.warning(f"_save_pending_job failed: {e}")

def _load_pending_job() -> dict | None:
    """Load a previously-saved pending job, or return None if none exists."""
    if not PENDING_JOB_FILE.exists():
        return None
    try:
        return json.loads(PENDING_JOB_FILE.read_text())
    except Exception as e:
        log.warning(f"_load_pending_job failed: {e}")
        return None

def _clear_pending_job() -> None:
    """Delete the pending-job file once the job completes or is cancelled."""
    try:
        if PENDING_JOB_FILE.exists():
            PENDING_JOB_FILE.unlink()
    except Exception as e:
        log.warning(f"_clear_pending_job failed: {e}")

_db = _load_data()
_db["stats"]["total_users"] = set(_db["stats"].get("total_users", []))
if "total_bytes_downloaded" not in _db["stats"]:
    _db["stats"]["total_bytes_downloaded"] = 0
if "link_limit" not in _db:
    _db["link_limit"] = None
if "free_size_limit_mb" not in _db:
    _db["free_size_limit_mb"] = 500
if "locked" not in _db:
    _db["locked"] = False

def is_vip(uid: int) -> bool:
    entry = _db["vip"].get(str(uid))
    if not entry:
        return False
    if entry.get("permanent"):
        return True
    exp = entry.get("expires")
    if exp and datetime.fromisoformat(exp) > datetime.utcnow():
        return True
    return False

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def is_god(uid: int) -> bool:
    """God Mode — admin-only tier with 50x threads and maximum crash safety."""
    return uid in ADMIN_IDS

def get_free_size_limit() -> int:
    """Returns free size limit in bytes."""
    mb = _db.get("free_size_limit_mb", 500)
    return int(mb * 1024 * 1024)

def _fmt_gb_total() -> str:
    total_bytes = _db["stats"].get("total_bytes_downloaded", 0)
    gb = total_bytes / (1024 ** 3)
    return f"{gb:.2f} GB"

# ─────────────────────────────────────────────────────────────────────
#  Exponential Backoff with Jitter - CRASH SAFE RETRY
# ─────────────────────────────────────────────────────────────────────
def calculate_backoff(attempt: int, base_delay: float = BASE_RETRY_DELAY,
                      max_delay: float = MAX_RETRY_DELAY,
                      jitter: float = JITTER_RANGE) -> float:
    """
    Calculate exponential backoff with jitter for retry delays.
    This prevents thundering herd problems and improves crash safety.
    """
    # Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s (capped)
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    # Add random jitter (0% to jitter% of delay)
    jitter_amount = delay * jitter * random.random()
    return delay + jitter_amount

# ─────────────────────────────────────────────────────────────────────
#  Stats Card Builder
# ─────────────────────────────────────────────────────────────────────
def _build_stats_card(job, pct: int, speed_mb: float, eta: str,
                      done_mb: float, total_mb: float, phase: str) -> str:
    """Render the single persistent live stats card with beautiful formatting."""
    # ── progress bar (20 blocks) ──
    filled = round(pct / 5)
    bar    = "█" * filled + "▒" * (20 - filled)

    # ── size display — cap done at total so we never display e.g. "3.60 GB / 3.40 GB" ──
    if total_mb > 0:
        display_done_mb = min(done_mb, total_mb)
    else:
        display_done_mb = done_mb
    if total_mb >= 1024:
        size_str = f"`{display_done_mb/1024:.2f} GB` / `{total_mb/1024:.2f} GB`"
    elif total_mb > 0:
        size_str = f"`{display_done_mb:.1f} MB` / `{total_mb:.1f} MB`"
    else:
        size_str = f"`{display_done_mb:.1f} MB`"

    speed_str = f"`{speed_mb:.1f} MB/s`" if speed_mb > 0 else "`—`"
    eta_str   = f"`{eta}`"

    return (
        f"`{bar}` *{pct}%*\n"
        f"📦 {size_str}\n"
        f"🚀 Speed: {speed_str} | ⏱ ETA: {eta_str}\n\n"
        f"📡 *Phase:* `{phase}`"
    )

CARD_MIN_INTERVAL = 5.0   # seconds — Telegram allows ~1 edit/msg/5s before flood

async def _update_card(job, pct: int = 0, speed_mb: float = 0.0,
                       eta: str = "—", done_mb: float = 0.0,
                       total_mb: float = 0.0, phase: str = "…",
                       force: bool = False):
    """
    Edit the single persistent card in-place.
    Throttled to CARD_MIN_INTERVAL seconds to avoid Telegram 429 flood errors.
    Pass force=True for milestone events (link complete, scan done) to bypass throttle.
    """
    if not job.live_stats_mid:
        return

    if job._card_lock is None:
        job._card_lock = asyncio.Lock()

    job._card_pct   = pct
    job._card_speed = speed_mb
    job._card_eta   = eta
    job._card_phase = phase

    now = asyncio.get_event_loop().time()
    if not force and now - job._card_last_edit < CARD_MIN_INTERVAL:
        return

    async with job._card_lock:
        now = asyncio.get_event_loop().time()
        if not force and now - job._card_last_edit < CARD_MIN_INTERVAL:
            return

        card = _build_stats_card(
            job,
            job._card_pct, job._card_speed, job._card_eta,
            done_mb, total_mb, job._card_phase
        )
        try:
            await job.bot.edit_message_text(
                chat_id=job.chat_id,
                message_id=job.live_stats_mid,
                text=card,
                parse_mode=ParseMode.MARKDOWN,
            )
            job._card_last_edit = asyncio.get_event_loop().time()
        except TelegramError as e:
            err = str(e).lower()
            if "not modified" in err:
                job._card_last_edit = asyncio.get_event_loop().time()
            elif "429" in err or "flood" in err:
                log.debug(f"Card flood-control — will retry next cycle")
            else:
                log.warning(f"card edit error: {e}")

# ─────────────────────────────────────────────────────────────────────
#  Job dataclass
# ─────────────────────────────────────────────────────────────────────
@dataclass
class Job:
    uid:        int
    chat_id:    int
    links:      list
    keywords:   list
    bot:        object
    vip:        bool = False
    god:        bool = False
    single_file: bool = False
    results:    dict = field(default_factory=dict)
    total_bytes_expected: int = 0
    total_bytes_done:     int = 0
    temp_files: list = field(default_factory=list)
    stopped:    bool = False
    cancel_task: object = None
    # Live stats
    live_hits:       dict = field(default_factory=dict)
    live_lines:      int  = 0
    links_done:      int  = 0
    links_total:     int  = 0
    live_stats_mid:  int  = 0
    # Throttle
    _card_last_edit: float = 0.0
    _card_lock:      object = field(default=None)
    _card_phase:     str  = "…"
    _card_pct:       int  = 0
    _card_speed:     float = 0.0
    _card_eta:       str  = "—"
    # Crash safety — no longer used (streaming engine handles retries per-part)
    # kept as stubs so old pickled/serialised Job objects don't break on load
    _failed_ranges: dict = field(default_factory=dict)
    _retry_count:   int  = 0

# ─────────────────────────────────────────────────────────────────────
#  Global queue
# ─────────────────────────────────────────────────────────────────────
_job_queue:   asyncio.Queue = None
_queue_list:  list          = []
_queue_lock:  asyncio.Lock  = None
_current_job: Job | None    = None
_running_tasks: dict[int, asyncio.Task] = {}
_watchdog_task: asyncio.Task = None
_shutdown_event: asyncio.Event = None

user_state: dict[int, dict] = defaultdict(lambda: {
    "step": "idle", "links": [], "keywords": [],
})

# ─────────────────────────────────────────────────────────────────────
#  Telegram helpers
# ─────────────────────────────────────────────────────────────────────
async def safe_send(bot, chat_id: int, text: str, **kw):
    for i in range(3):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, **kw)
        except TelegramError as e:
            log.warning(f"send_message attempt {i+1}: {e}")
            await asyncio.sleep(2 ** i)

async def safe_edit(bot, chat_id: int, msg_id: int, text: str, **kw):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, **kw)
    except TelegramError as e:
        if "not modified" not in str(e).lower():
            log.warning(f"edit_message_text: {e}")

async def safe_send_doc(bot, chat_id: int, path: Path, caption: str = ""):
    import io
    for i in range(3):
        try:
            async with aiofiles.open(path, "rb") as af:
                data = await af.read()
            await bot.send_document(chat_id=chat_id,
                                    document=io.BytesIO(data),
                                    filename=path.name, caption=caption)
            return
        except TelegramError as e:
            log.warning(f"send_document attempt {i+1}: {e}")
            await asyncio.sleep(2 ** i)

def _fmt(mb: float) -> str:
    return f"{mb/1024:.2f} GB" if mb >= 1024 else f"{mb:.1f} MB"

def _kw_filename(keywords: list[str]) -> str:
    safe = "_".join(re.sub(r'[^\w]', '', k) for k in keywords if k)
    return (safe[:80] or "results") + ".txt"

def _strip_url(line: str) -> str:
    """
    Extract ONLY the credential (user:pass / email:pass) from any combo-list line.
    Zero URL leaks. Zero false-positive credential drops.

    Handles every real-world combo format:
      https://site.com:user:pass                → user:pass
      https://site.com:user@x.com:pass          → user@x.com:pass
      https://site.com:8080:john:secret         → john:secret
      https://cdn.site.com/files/2024/:user:pass→ user:pass
      https://a.com|https://b.com|user:pass     → user:pass
      https://site.com https://b.com user:pass  → user:pass
      site.com:443:user:pass                    → user:pass
      site.com:user:pass                        → user:pass
      user@gmail.com:pass                       → user@gmail.com:pass
      john:mypassword                           → john:mypassword
      john.doe:mypassword                       → john.doe:mypassword
      user TAB pass  /  url TAB user TAB pass   → user:pass
      {"login":"u","password":"p"}              → u:p
      LOGIN: u PASS: p                          → u:p
      user@x.com;pass  /  user,pass            → user@x.com:pass
      ftp://site.com:user:pass                  → user:pass
      socks5://proxy:1080:user:pass             → user:pass
      android://base64@com.package:             → (discarded — opaque token, not a credential)
      android://base64@com.package/:            → (discarded — opaque token, not a credential)
    """
    import json as _j

    # Only well-known schemes as URL prefixes — avoids false-positives on
    # passwords that happen to contain short strings like "pass://" or "go://"
    _IS_URL  = re.compile(
        r'^(?:https?|ftp|ftps|sftp|socks[45]?|ss|vmess|trojan|android|ios)://',
        re.IGNORECASE
    )
    _ANY_URL = re.compile(
        r'(?:https?|ftp|ftps|sftp|socks[45]?|ss|vmess|trojan|android|ios)://\S*',
        re.IGNORECASE
    )

    # App-scheme URLs with opaque base64/token user-info (android://, ios://, etc.)
    # The user-info portion is 20+ chars of base64/token — never a real credential.
    _APP_SCHEME_URL = re.compile(
        r'^(?:android|ios|[a-z][a-z0-9]{2,20})://[A-Za-z0-9+/=_\-]{20,}@[\w.\-]+[:/]',
        re.IGNORECASE
    )

    line = line.strip().rstrip('\r\n').strip()
    if not line:
        return ""

    # ── -1. Discard app-scheme token URLs (android://, ios://, etc.) ──────
    #   e.g. android://BIGBASE64TOKEN@com.company.app:
    #   The "user-info" is an opaque auth token, not a username.
    if _APP_SCHEME_URL.match(line):
        return ""

    # ── 0. JSON ───────────────────────────────────────────────────────────
    if line.startswith('{') and line.endswith('}'):
        try:
            obj = _j.loads(line)
            user = (obj.get("login") or obj.get("email") or obj.get("username")
                    or obj.get("user") or "")
            pw   = (obj.get("password") or obj.get("pass") or obj.get("passwd")
                    or obj.get("pwd") or "")
            if user and pw:
                return f"{user}:{pw}"
        except Exception:
            pass

    # ── 1. Space key-value: "LOGIN: user PASS: pass" ─────────────────────
    _kv = re.match(
        r'(?i)(?:login|email|user(?:name)?)\s*[=:]\s*(\S+)\s+'
        r'(?:pass(?:word)?|pwd)\s*[=:]\s*(\S+)', line)
    if _kv:
        return f"{_kv.group(1)}:{_kv.group(2)}"

    # ── 2. Tab-separated: drop URL tabs, keep cred tabs ───────────────────
    if '\t' in line:
        segs = [s.strip() for s in line.split('\t') if s.strip()]
        clean = [s for s in segs if not _IS_URL.match(s)]
        if len(clean) >= 2:
            return f"{clean[0]}:{clean[1]}"
        if len(clean) == 1 and ':' in clean[0]:
            return clean[0]

    # ── 3. Pipe-separated: drop URL/bare-domain segments ─────────────────
    if '|' in line:
        segs = [s.strip() for s in line.split('|') if s.strip()]
        clean = [s for s in segs if not _IS_URL.match(s)]
        def _is_bare_domain_seg(s):
            host = s.split(':')[0]
            return ('.' in host and '@' not in host and '/' not in host
                    and ' ' not in host and not host.isdigit())
        clean = [s for s in clean if not _is_bare_domain_seg(s)]
        if clean:
            line = clean[0]
        else:
            return ""

    # ── 4. Space-separated: strip ALL URLs from anywhere in the line ─────
    #   Remove URLs from any position (start, middle, end)
    #   BUT: only if there are multiple space-separated tokens
    #   (don't nuke single-token URL:cred lines like "https://site.com:user:pass")
    if ' ' in line and _ANY_URL.search(line):
        # Remove all URLs first
        tokens = line.split()
        cred_tokens = [t for t in tokens if not _IS_URL.match(t)]
        line = ' '.join(cred_tokens).strip() if cred_tokens else ""
        if not line:
            return ""
        # After removing space-separated URLs, continue processing
        # Don't return early - there may still be scheme-prefixed URLs to handle

    # ── 5. Protocol-relative //host ───────────────────────────────────────
    if line.startswith('//'):
        line = re.sub(r'^//[^\s:|]+(?::\d+)?(?:/[^\s:]*)?', '', line).lstrip(':').strip()
        if not line:
            return ""

    # ── 6. Scheme-prefixed URL at start: strip scheme://[host][:port][/path] ───────
    #   IMPORTANT: do NOT consume user-info (user@) here — it might be the
    #   credential itself (e.g. https://site.com:user@gmail.com:pass).
    #   Only strip the scheme + bare hostname + optional port + optional path.
    if _IS_URL.match(line):
        # Remove scheme + hostname only (no user-info stripping)
        after = re.sub(
            r'^(?:https?|ftp|ftps?|sftp|socks[45]?|ss|vmess|trojan|android|ios)'
            r'://[a-zA-Z0-9.\-]+'          # hostname (no @ allowed here)
            r'(?::\d+)?'                   # optional port
            r'(?:/[^:@\s]*)?',             # optional path (stop at : or @)
            '', line, flags=re.IGNORECASE
        )
        after = after.lstrip(':').strip()
        if after:
            line = after
        else:
            return ""

    # ── 7. Bare domain/IP prefix: site.com[:port]:user:pass ───────────────
    #   Only strip the domain prefix when what follows is clearly a full
    #   credential pair (user:pass with at least 2 colon-separated parts
    #   remaining after dropping an optional numeric port).
    #
    #   CRITICAL FIX: if the line is already just "username:password" with
    #   no URL prefix, do NOT strip it. We only apply this rule when the first
    #   segment looks like a real domain (contains a dot AND no @ sign AND
    #   is not already a credential by itself).
    parts = line.split(':')
    if len(parts) >= 3:   # Need at least domain:user:pass (3 parts)
        first = parts[0].strip()
        # Must look like a domain: has a dot, no @, no spaces, not pure digits
        is_real_domain = ('.' in first and '@' not in first and ' ' not in first
                          and not first.isdigit() and 2 <= len(first) <= 253)
        if is_real_domain:
            rest = parts[1:]
            if rest and rest[0].strip().isdigit():   # skip port segment
                rest = rest[1:]
            if len(rest) >= 2:    # must still have user + pass remaining
                cred = ':'.join(rest).strip()
                if cred:
                    return cred
    # 2-part lines (domain:pass) fall through — could be a plain user:pass too.
    # We keep them as-is and let later steps handle or return them verbatim.

    # ── 8. Email:pass — colon immediately after @ sign ────────────────────
    first_colon = line.find(':')
    if first_colon > 0 and '@' in line[:first_colon]:
        return line   # e.g. user@gmail.com:pass — return verbatim

    # ── 9. Plain username:password — return verbatim ──────────────────────
    #   At this point the line has no URL scheme and wasn't caught by domain
    #   stripping. If it has a colon it's almost certainly user:pass.
    if ':' in line:
        # Sanity: don't return pure junk. Must have non-empty parts.
        u, _, p = line.partition(':')
        if u.strip() and p.strip():
            return line   # ← FIXED: was being dropped before reaching here

    # ── 10. Alt separators: semicolon / comma ────────────────────────────
    for sep in (';', ','):
        if sep in line:
            u, _, p = line.partition(sep)
            u, p = u.strip(), p.strip()
            if u and p:
                return f"{u}:{p}"

    # ── 11. Final safety net: nuke any surviving scheme URL ───────────────
    #   Only match well-known schemes here — same list as _IS_URL — to avoid
    #   nuking passwords that happen to contain short letter sequences + "://".
    if re.search(
        r'(?:https?|ftp|ftps?|sftp|socks[45]?|ss|vmess|trojan|android|ios)://',
        line, re.IGNORECASE
    ):
        if _IS_URL.match(line.strip()):
            return ""   # entire value is a URL — discard
        line = _ANY_URL.sub('', line).strip().lstrip(':').strip()

    return line.strip()


# ─────────────────────────────────────────────────────────────────────
#  Channel gate
# ─────────────────────────────────────────────────────────────────────
async def check_joined_channels(bot, uid: int) -> list[str]:
    """Check if user has joined all required channels."""
    not_joined = []
    required_channels = _db.get("required_channels", [])

    for ch in required_channels:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=uid)
            if member.status in (ChatMember.LEFT, ChatMember.BANNED):
                not_joined.append(ch)
        except Exception as e:
            log.warning(f"Could not check channel {ch} for user {uid}: {e}")
            not_joined.append(ch)

    return not_joined

async def build_channel_join_keyboard(missing_channels: list[str]) -> InlineKeyboardMarkup:
    """Build a keyboard with join buttons for missing channels."""
    buttons = []
    for ch in missing_channels:
        ch_display = ch if ch.startswith('@') else f'@{ch}'
        ch_link = ch.replace('@', '')
        buttons.append([InlineKeyboardButton(
            f"📢 Join {ch_display}",
            url=f"https://t.me/{ch_link}"
        )])

    buttons.append([InlineKeyboardButton(
        "🔄 Check Membership",
        callback_data="check_membership"
    )])

    return InlineKeyboardMarkup(buttons)

# ─────────────────────────────────────────────────────────────────────
#  STREAMING DOWNLOAD+SCAN ENGINE
#  Single URL → N plan-based parts → semaphore-controlled parallel
#  Each part: stream download → early filter → parse → write results
#  No intermediate raw file stored on disk.
# ─────────────────────────────────────────────────────────────────────

async def _process_link_streaming(session, url: str, idx: int, job: Job) -> tuple[dict, Optional[Path]]:
    """
    ┌─────────────────────────────────────────────────────────────────┐
    │  PLAN-BASED SPLIT + STREAMING SCAN ENGINE                      │
    │                                                                 │
    │  1. HEAD  → get file size, check Accept-Ranges                 │
    │  2. SPLIT → N parts per plan (FREE=10 / VIP=20 / GOD=25)      │
    │  3. QUEUE → semaphore limits concurrent active parts           │
    │  4. EACH PART: stream → early filter → parse → write result   │
    │     No intermediate raw file stored on disk.                   │
    │  5. RETRY per part up to 3x with exponential backoff          │
    │  6. FALLBACK to single stream if range not supported          │
    └─────────────────────────────────────────────────────────────────┘
    """
    if job.stopped:
        return {k: 0 for k in job.keywords}, None

    # ── Plan parameters ───────────────────────────────────────────────
    if job.god:
        n_parts     = N_PARTS_GOD
        max_workers = MAX_WORKERS_GOD
        chunk_size  = CHUNK_SIZE_GOD
        max_retries = MAX_RETRIES_GOD
        conn_timeout = CONNECT_TIMEOUT_GOD
        read_timeout = SOCK_READ_TIMEOUT_GOD
    elif job.vip:
        n_parts     = N_PARTS_VIP
        max_workers = MAX_WORKERS_VIP
        chunk_size  = CHUNK_SIZE_VIP
        max_retries = MAX_RETRIES
        conn_timeout = CONNECT_TIMEOUT_VIP
        read_timeout = SOCK_READ_TIMEOUT_VIP
    else:
        n_parts     = N_PARTS_FREE
        max_workers = MAX_WORKERS_FREE
        chunk_size  = CHUNK_SIZE_FREE
        max_retries = MAX_RETRIES
        conn_timeout = CONNECT_TIMEOUT_FREE
        read_timeout = SOCK_READ_TIMEOUT_FREE

    base_headers = {"User-Agent": "Mozilla/5.0 (compatible; ZyScanBot/8.5)"}
    kw_lower    = [k.lower() for k in job.keywords]
    SEEN_CAP    = 5_000_000
    BUF_SIZE    = 20_000     # Large write buffer — flush less often

    # ── Result file setup — one file per keyword ─────────────────────
    result_paths: dict[str, Path] = {}
    for kw in job.keywords:
        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
        rp = RESULTS_DIR / f"link{idx}_{safe_kw[:80]}.txt"
        result_paths[kw] = rp
        job.temp_files.append(rp)
    result_path = list(result_paths.values())[0] if result_paths else \
        RESULTS_DIR / f"link{idx}_results.txt"

    # ── Shared state ──────────────────────────────────────────────────
    hits: dict[str, int]  = {k: 0 for k in job.keywords}
    seen: set[str]        = set()
    bytes_done            = [0]

    # Per-keyword write queues — large maxsize avoids put_nowait drops
    write_queues: dict[str, asyncio.Queue] = {
        k: asyncio.Queue(maxsize=0) for k in job.keywords  # 0 = unlimited
    }
    BUF_SENTINEL = None

    # ── Writer: batched async writes per keyword ──────────────────────
    async def _writer_for(kw: str):
        buf: list[str] = []
        async with aiofiles.open(result_paths[kw], "w", encoding="utf-8") as fout:
            while True:
                item = await write_queues[kw].get()
                if item is BUF_SENTINEL:
                    if buf:
                        await fout.write("".join(buf))
                    break
                buf.append(item)
                if len(buf) >= BUF_SIZE:
                    await fout.write("".join(buf))
                    buf.clear()

    async def _writer():
        await asyncio.gather(*[_writer_for(kw) for kw in job.keywords])

    # ── Step 1: HEAD → file size + range support ──────────────────────
    # Try HEAD first; if it fails, fire a GET with Range:bytes=0-0 as probe
    file_size       = 0
    range_supported = False
    try:
        head_timeout = aiohttp.ClientTimeout(total=15)
        async with session.head(url, headers=base_headers,
                                timeout=head_timeout, ssl=False,
                                allow_redirects=True) as resp:
            file_size = int(resp.headers.get("Content-Length", 0))
            ar = resp.headers.get("Accept-Ranges", "")
            range_supported = ar.lower() == "bytes" and file_size > 0
    except Exception:
        pass

    # Fallback probe: GET Range:bytes=0-0 to confirm range support
    if not range_supported:
        try:
            probe_timeout = aiohttp.ClientTimeout(total=15)
            async with session.get(
                url,
                headers={**base_headers, "Range": "bytes=0-0"},
                timeout=probe_timeout, ssl=False,
                allow_redirects=True,
            ) as resp:
                if resp.status == 206:
                    cr = resp.headers.get("Content-Range", "")
                    m  = re.search(r"/(\d+)$", cr)
                    if m:
                        file_size = int(m.group(1))
                    elif not file_size:
                        file_size = int(resp.headers.get("Content-Length", 0))
                    range_supported = file_size > 0
                elif resp.status == 200:
                    file_size = int(resp.headers.get("Content-Length", 0))
        except Exception as e:
            log.warning(f"[Link {idx}] Probe failed ({e}) — single stream")

    if file_size:
        job.total_bytes_expected += file_size

    log.info(f"[Link {idx}] size={file_size/1_048_576:.1f}MB range={range_supported} "
             f"parts={n_parts} workers={max_workers} chunk={chunk_size//1024}KB")

    # ─────────────────────────────────────────────────────────────────
    #  Progress reporter
    # ─────────────────────────────────────────────────────────────────
    reporter_stop = asyncio.Event()

    async def _progress_reporter():
        t0       = asyncio.get_event_loop().time()
        last_done  = 0
        last_speed = 0.0
        while not reporter_stop.is_set():
            await asyncio.sleep(3)          # update every 3s (was 5s) — snappier UI
            if reporter_stop.is_set():
                break
            done    = bytes_done[0]
            elapsed = asyncio.get_event_loop().time() - t0
            delta   = done - last_done
            last_done = done
            spd = (delta / 3) / 1_048_576  # MB/s over last 3s window
            if spd > 0:
                last_speed = spd
            elif last_speed > 0 and elapsed < 30:
                spd = last_speed

            if file_size and done > 0:
                pct   = min(int(done / file_size * 100), 99)
                sb    = done / elapsed if elapsed > 0 else 0
                eta_s = int((file_size - done) / sb) if sb > 0 and done < file_size else 0
                eta   = f"{eta_s // 60}m {eta_s % 60}s" if eta_s > 0 else "—"
            else:
                pct, eta = 0, "—"

            label = (f"⚡ {n_parts} parts · {hits and sum(hits.values()) or 0} hits"
                     if range_supported else f"🌊 Stream · {hits and sum(hits.values()) or 0} hits")
            await _update_card(job, pct, spd, eta,
                               job.total_bytes_done / 1_048_576,
                               job.total_bytes_expected / 1_048_576,
                               label)

    # ─────────────────────────────────────────────────────────────────
    #  Hot inner loop — runs in a ThreadPoolExecutor so it NEVER blocks
    #  the event loop. Downloads on all 32 parts can proceed freely
    #  while the CPU processes lines on a separate OS thread.
    # ─────────────────────────────────────────────────────────────────
    _kw_count  = len(job.keywords)
    _kw_orig   = job.keywords
    _kw_low    = kw_lower
    _write_qs  = write_queues
    _seen      = seen
    _hits      = hits
    _live_hits = job.live_hits
    _seen_cap  = SEEN_CAP

    # Threading lock: protects shared state (seen set, hits, live_hits).
    # KEY: We minimise time spent holding this lock so scan threads don't
    # block each other — expensive work (strip_url, keyword match) all
    # happens OUTSIDE the lock, lock is only taken for the dedup commit.
    import threading
    _state_lock = threading.Lock()

    def _process_chunk_lines(lines: list[str]) -> None:
        """
        CPU-bound line filter — lock-minimised design for GOD MODE.

        Phase 1 (NO lock): keyword match + strip_url for every line.
                           This is the slow CPU work — runs fully parallel.
        Phase 2 (local dedup): deduplicate within this batch using a local
                           set so we arrive at the global lock with minimal work.
        Phase 3 (brief lock): commit new entries to global seen set + hits.
                           Lock held for O(unique_hits) dict ops only.
        Phase 4 (NO lock): enqueue writes — put_nowait is GIL-safe.
        """
        # ── Phase 1: scan lines with no lock held ────────────────────
        candidates: list[tuple[int, str]] = []   # (kw_index, clean_cred)
        for raw_line in lines:
            if not raw_line:
                continue
            if ":" not in raw_line and "@" not in raw_line:
                continue
            low = raw_line.lower()
            for i in range(_kw_count):
                if _kw_low[i] not in low:
                    continue
                clean = _strip_url(raw_line)
                if clean:
                    candidates.append((i, clean))
                break   # first matching keyword wins per line

        if not candidates:
            return

        # ── Phase 2: local dedup (no lock — this set is thread-local) ─
        local_seen: set[str] = set()
        deduped: list[tuple[int, str]] = []
        for i, clean in candidates:
            if clean not in local_seen:
                local_seen.add(clean)
                deduped.append((i, clean))

        # ── Phase 3: global dedup + commit (lock held briefly) ────────
        committed: list[tuple[str, str]] = []   # (kw_name, line_out)
        with _state_lock:
            seen_len = len(_seen)
            for i, clean in deduped:
                if seen_len < _seen_cap:
                    if clean in _seen:
                        continue
                    _seen.add(clean)
                    seen_len += 1
                kw = _kw_orig[i]
                _hits[kw] += 1
                _live_hits[kw] = _live_hits.get(kw, 0) + 1
                committed.append((kw, clean + "\n"))

        # ── Phase 4: enqueue writes — put_nowait is GIL-safe ──────────
        for kw, line_out in committed:
            _write_qs[kw].put_nowait(line_out)

    # Thread pool sizing:
    #   GOD: 12 workers — one per 2-3 download coroutines at peak, preventing
    #        GIL contention while keeping all 32 parts fed continuously.
    #        6 workers was the bottleneck causing the "after-peak" speed drop.
    #   VIP: 6, FREE: 2 — proportional to part count.
    import concurrent.futures
    _tp_workers = 12 if job.god else (6 if job.vip else 2)
    _thread_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=_tp_workers, thread_name_prefix="scan"
    )

    # Pending scan futures — tracked so we can drain them before closing
    _pending_scans: list = []

    def _process_async(lines: list[str]) -> None:
        """
        Fire-and-forget submit: does NOT await. The download coroutine calls
        this and returns immediately to read the next network chunk — keeping
        the pipe saturated. This eliminates the stall where each part had to
        wait for the CPU scan to finish before it could read more data.
        Futures collected in _pending_scans; drained before exit.
        Completed futures are pruned every 200 submissions to prevent
        unbounded memory growth on large files.
        """
        fut = _thread_pool.submit(_process_chunk_lines, lines)
        _pending_scans.append(fut)
        # Prune done futures every 200 submissions — avoids list growing to
        # millions of entries on large files, which itself slows things down.
        if len(_pending_scans) % 200 == 0:
            done_indices = [i for i, f in enumerate(_pending_scans) if f.done()]
            for i in reversed(done_indices):
                _pending_scans.pop(i)

    async def _drain_scans() -> None:
        """Wait for all in-flight scan futures to finish (called before exiting)."""
        if not _pending_scans:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, lambda: [f.result() for f in _pending_scans]
        )

    # ─────────────────────────────────────────────────────────────────
    #  Shared decode helper — splits bytes on newlines, returns leftover
    #  Pure byte ops — no string allocation until we have complete lines
    # ─────────────────────────────────────────────────────────────────
    def _split_chunk(leftover: bytearray, raw_chunk: bytes) -> tuple[list[str], bytearray]:
        """Append chunk, extract complete lines as strings, return leftover bytes.
        Uses extend() instead of += to avoid copying the entire leftover buffer
        on every chunk — critical for sustained GOD MODE throughput."""
        leftover.extend(raw_chunk)
        last_nl = leftover.rfind(b"\n")
        if last_nl == -1:
            return [], leftover
        complete     = bytes(leftover[:last_nl + 1])
        new_leftover = bytearray(leftover[last_nl + 1:])
        text  = complete.decode("utf-8", errors="replace")
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines = lines[:-1]
        return lines, new_leftover

    # ─────────────────────────────────────────────────────────────────
    #  PATH A: Range-split parallel download
    #  Key change: await _process_async() so the event loop yields
    #  control to other download coroutines while lines are processed.
    # ─────────────────────────────────────────────────────────────────
    async def _download_part(part_id: int, start: int, end: int,
                             sem: asyncio.Semaphore) -> None:
        range_size   = end - start + 1
        part_headers = {**base_headers, "Range": f"bytes={start}-{end}"}
        timeout = aiohttp.ClientTimeout(total=None,
                                        connect=conn_timeout,
                                        sock_read=read_timeout)
        for attempt in range(1, max_retries + 1):
            if job.stopped:
                return
            try:
                # ⚡ GOD MODE: semaphore only gates the TCP connect() syscall.
                # We open the response object under the lock, then release
                # immediately — streaming happens at full concurrency with zero
                # semaphore contention. This is the fix for the "initial peak
                # then crawl to 0.2 MB/s" pattern: the old code held the sem
                # for the full stream duration, serialising all 32 parts.
                async with sem:
                    resp = await session.get(
                        url, headers=part_headers,
                        timeout=timeout, ssl=False,
                        allow_redirects=True,
                        raise_for_status=False,
                    )
                    # Verify status while still under sem so a bad status
                    # doesn't consume a slot for no reason
                    if resp.status not in (200, 206):
                        await resp.release()
                        log.warning(f"[Link {idx}] Part {part_id}: "
                                    f"HTTP {resp.status} (attempt {attempt})")
                        if attempt < max_retries:
                            await asyncio.sleep(calculate_backoff(attempt))
                        continue

                # Semaphore fully released — stream at full concurrency ⚡
                try:
                    leftover      = bytearray()
                    range_written = 0

                    async for raw_chunk in resp.content.iter_chunked(chunk_size):
                        if job.stopped:
                            return
                        remaining = range_size - range_written
                        if len(raw_chunk) > remaining:
                            raw_chunk = raw_chunk[:remaining]
                        if not raw_chunk:
                            break

                        clen = len(raw_chunk)
                        range_written        += clen
                        bytes_done[0]        += clen
                        job.total_bytes_done += clen

                        lines, leftover = _split_chunk(leftover, raw_chunk)
                        if lines:
                            _process_async(lines)

                        if range_written >= range_size:
                            break

                    if leftover:
                        tail = leftover.decode("utf-8", errors="replace").split("\n")
                        if tail:
                            _process_async(tail)
                finally:
                    await resp.release()
                return  # success

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[Link {idx}] Part {part_id} attempt {attempt}: "
                             f"{type(e).__name__}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(calculate_backoff(attempt))

        log.error(f"[Link {idx}] Part {part_id} FAILED after {max_retries} attempts — skipping")

    # ─────────────────────────────────────────────────────────────────
    #  PATH B: Single-stream fallback
    # ─────────────────────────────────────────────────────────────────
    async def _download_stream_fallback() -> None:
        timeout = aiohttp.ClientTimeout(total=None,
                                        connect=conn_timeout,
                                        sock_read=read_timeout)
        for attempt in range(1, max_retries + 1):
            if job.stopped:
                return
            try:
                async with session.get(url, headers=base_headers,
                                       timeout=timeout, ssl=False,
                                       allow_redirects=True) as resp:
                    cl = int(resp.headers.get("Content-Length", 0))
                    if cl and not job.total_bytes_expected:
                        job.total_bytes_expected += cl

                    leftover = bytearray()
                    async for raw_chunk in resp.content.iter_chunked(chunk_size):
                        if job.stopped:
                            return
                        clen = len(raw_chunk)
                        bytes_done[0]        += clen
                        job.total_bytes_done += clen
                        lines, leftover = _split_chunk(leftover, raw_chunk)
                        if lines:
                            _process_async(lines)

                    if leftover:
                        tail = leftover.decode("utf-8", errors="replace").split("\n")
                        if tail:
                            _process_async(tail)
                    return  # success

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[Link {idx}] Stream fallback attempt {attempt}: "
                             f"{type(e).__name__}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(calculate_backoff(attempt))

        log.error(f"[Link {idx}] Stream fallback FAILED after {max_retries} attempts")

    # ─────────────────────────────────────────────────────────────────
    #  Run everything
    # ─────────────────────────────────────────────────────────────────
    writer_task   = asyncio.create_task(_writer())
    reporter_task = asyncio.create_task(_progress_reporter())
    try:
        if range_supported:
            # Build parts list
            part_size = max(file_size // n_parts, 1)
            parts = []
            for i in range(n_parts):
                s = i * part_size
                e = (s + part_size - 1) if i < n_parts - 1 else (file_size - 1)
                parts.append((i, s, e))

            sem = asyncio.Semaphore(max_workers)
            # For GOD mode, all 32 parts connect simultaneously — semaphore
            # only gates the TCP handshake (fast), not the stream (slow).
            # Setting it to max_workers means all parts race to connect at once.
            await asyncio.gather(*[_download_part(pid, s, e, sem)
                                   for pid, s, e in parts])
        else:
            await _download_stream_fallback()

        # Drain all in-flight scan futures before signalling writers
        await _drain_scans()
        # Signal all per-keyword writers to flush + exit
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        await writer_task

        # Update global downloaded counter
        _db["stats"]["total_bytes_downloaded"] = (
            _db["stats"].get("total_bytes_downloaded", 0) + bytes_done[0]
        )
        await _asave()

        await _update_card(
            job, 100, 0.0, "✅ Done",
            job.total_bytes_done / 1_048_576,
            job.total_bytes_expected / 1_048_576,
            f"Link {idx} complete ✅",
            force=True,
        )
        log.info(f"[Link {idx}] Streaming scan done — "
                 f"{sum(hits.values())} hits, {bytes_done[0]/1_048_576:.1f} MB")
        return hits, result_paths

    except asyncio.CancelledError:
        await _drain_scans()
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        try:
            await writer_task
        except Exception:
            pass
        raise
    except Exception as e:
        log.error(f"[Link {idx}] Fatal streaming error: {e}\n{traceback.format_exc()}")
        await _drain_scans()
        for wq in write_queues.values():
            await wq.put(BUF_SENTINEL)
        try:
            await writer_task
        except Exception:
            pass
        await _update_card(
            job, 0, 0.0, "—",
            job.total_bytes_done / 1_048_576,
            job.total_bytes_expected / 1_048_576,
            f"Link {idx}: error — {e}",
        )
        return hits, result_paths
    finally:
        reporter_stop.set()
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass
        _thread_pool.shutdown(wait=False)

# ─────────────────────────────────────────────────────────────────────
#  Cleanup
# ─────────────────────────────────────────────────────────────────────
def _sync_cleanup(temp_files: list) -> int:
    removed = 0
    for p in temp_files:
        try:
            pp = Path(p)
            if pp.exists():
                pp.unlink()
                removed += 1
        except Exception as e:
            log.warning(f"Cleanup {p}: {e}")
    return removed

async def cleanup_job(job: Job):
    removed = await asyncio.get_event_loop().run_in_executor(
        None, _sync_cleanup, list(job.temp_files)
    )
    log.info(f"Cleanup done — {removed} files for uid={job.uid}")

# ─────────────────────────────────────────────────────────────────────
#  Batch splitter & sender
# ─────────────────────────────────────────────────────────────────────
BATCH_SIZE_BYTES = 45 * 1024 * 1024

async def split_and_send(bot, chat_id: int, result_path: Path,
                         keywords: list[str], idx: int, tier: str,
                         hits: dict, job: Job):
    total_hits = sum(hits.values())
    kw_str = ", ".join(keywords)
    tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"
    file_size = result_path.stat().st_size if result_path.exists() else 0

    if file_size == 0:
        await safe_send(bot, chat_id,
            f"ℹ️ *Link {idx}:* No matches found.", parse_mode=ParseMode.MARKDOWN)
        return

    if file_size <= BATCH_SIZE_BYTES:
        cap = (
            f"📂 {tier_badge} RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keywords: `{kw_str}`\n"
            f"💥 Total Hits: `{total_hits:,}`\n"
            f"📦 Batch: 1/1"
        )
        await safe_send_doc(bot, chat_id, result_path, caption=cap)
        return

    loop = asyncio.get_event_loop()

    def _sync_split() -> list[Path]:
        paths: list[Path] = []
        buf: list[str] = []
        buf_size = 0
        bi = 0
        with open(result_path, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                buf.append(line)
                buf_size += len(line.encode("utf-8"))
                if buf_size >= BATCH_SIZE_BYTES:
                    bi += 1
                    bp = result_path.with_suffix(f".batch{bi}.txt")
                    with open(bp, "w", encoding="utf-8") as fout:
                        fout.writelines(buf)
                    paths.append(bp)
                    buf.clear()
                    buf_size = 0
            if buf:
                bi += 1
                bp = result_path.with_suffix(f".batch{bi}.txt")
                with open(bp, "w", encoding="utf-8") as fout:
                    fout.writelines(buf)
                paths.append(bp)
        return paths

    batch_paths = await loop.run_in_executor(None, _sync_split)
    for bp in batch_paths:
        job.temp_files.append(bp)

    total_batches = len(batch_paths)
    send_sem = asyncio.Semaphore(3)

    async def _send_one(bi: int, bp: Path):
        cap = (
            f"📂 {tier_badge} RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keywords: `{kw_str}`\n"
            f"💥 Total Hits: `{total_hits:,}`\n"
            f"📦 Batch: `{bi}/{total_batches}`"
        )
        async with send_sem:
            await safe_send_doc(bot, chat_id, bp, caption=cap)

    await asyncio.gather(*[_send_one(bi, bp) for bi, bp in enumerate(batch_paths, 1)])


async def merge_and_send(bot, chat_id: int, job: Job, tier: str):
    """
    Single-file mode: merge ALL links for ALL keywords into ONE file per keyword.
    Each keyword gets its own merged file sent separately.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"

    for kw in job.keywords:
        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
        merged_path = RESULTS_DIR / f"merged_{job.uid}_{safe_kw[:60]}_{ts}.txt"
        job.temp_files.append(merged_path)

        kw_hits = 0
        with open(merged_path, "w", encoding="utf-8") as fout:
            for i, info in sorted(job.results.items()):
                if info["status"] == "done":
                    # Try new per-keyword path first, fall back to legacy
                    rp_str = info.get("result_paths", {}).get(kw) or info.get("result_path")
                    if rp_str and Path(rp_str).exists():
                        kw_hits += info.get("hits", {}).get(kw, 0)
                        with open(rp_str, "r", encoding="utf-8", errors="replace") as fin:
                            for line in fin:
                                fout.write(line)

        file_size = merged_path.stat().st_size if merged_path.exists() else 0
        if file_size == 0:
            await safe_send(bot, chat_id,
                f"ℹ️ No matches found for keyword `{kw}` across all links.",
                parse_mode=ParseMode.MARKDOWN)
            continue

        cap = (
            f"📂 {tier_badge} MERGED RESULTS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Keyword: `{kw}`\n"
            f"💥 Total Hits: `{kw_hits:,}`\n"
            f"📦 All Links Combined"
        )
        if file_size <= BATCH_SIZE_BYTES:
            await safe_send_doc(bot, chat_id, merged_path, caption=cap)
        else:
            dummy_hits = {kw: kw_hits}
            await split_and_send(bot, chat_id, merged_path, [kw], 0, tier, dummy_hits, job)


async def process_link(session, url: str, idx: int, job: Job):
    """
    Process a single URL: stream-download + scan in one pass.
    Calls _process_link_streaming which handles splitting, parallel parts,
    early filtering, and immediate result writing.
    """
    if job.stopped:
        return

    hits, result_paths = await _process_link_streaming(session, url, idx, job)

    job.results[idx] = {
        "status": "done",
        "hits": hits,
        # result_paths is now a dict {keyword: Path}
        "result_paths": {kw: str(p) for kw, p in result_paths.items()} if isinstance(result_paths, dict) else {},
        # keep legacy key for crash-resume compatibility
        "result_path": str(list(result_paths.values())[0]) if isinstance(result_paths, dict) and result_paths else None,
    }
    job.links_done += 1
    await _update_card(
        job, 100, 0.0, "—",
        job.total_bytes_done / 1_048_576,
        job.total_bytes_expected / 1_048_576,
        f"Link {idx} complete ✅ — {job.links_done}/{job.links_total} done",
        force=True,
    )


async def send_completed_results(job: Job):
    tier = "vip" if job.vip else "free"
    done_results = {i: info for i, info in job.results.items() if info["status"] == "done"}

    if not done_results:
        await safe_send(job.bot, job.chat_id,
            "ℹ️ No completed results to send.", parse_mode=ParseMode.MARKDOWN)
        return

    if job.single_file:
        await merge_and_send(job.bot, job.chat_id, job, tier)
    else:
        # Separate files mode: one merged file per keyword (all links combined per keyword)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        tier_badge = "👑 VIP" if tier == "vip" else "🔰 FREE"
        for kw in job.keywords:
            safe_kw = re.sub(r'[^\w]', '', kw) or "results"
            merged_kw_path = RESULTS_DIR / f"kw_{safe_kw[:60]}_{job.uid}_{ts}.txt"
            job.temp_files.append(merged_kw_path)

            kw_hits = 0
            with open(merged_kw_path, "w", encoding="utf-8") as fout:
                for i, info in sorted(done_results.items()):
                    rp_str = info.get("result_paths", {}).get(kw) or info.get("result_path")
                    if rp_str and Path(rp_str).exists():
                        kw_hits += info.get("hits", {}).get(kw, 0)
                        with open(rp_str, "r", encoding="utf-8", errors="replace") as fin:
                            for line in fin:
                                fout.write(line)

            file_size = merged_kw_path.stat().st_size if merged_kw_path.exists() else 0
            if file_size == 0:
                await safe_send(job.bot, job.chat_id,
                    f"ℹ️ No matches found for keyword `{kw}`.",
                    parse_mode=ParseMode.MARKDOWN)
                continue

            cap = (
                f"📂 {tier_badge} RESULTS\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Keyword: `{kw}`\n"
                f"💥 Total Hits: `{kw_hits:,}`\n"
                f"📦 All Links Combined"
            )
            if file_size <= BATCH_SIZE_BYTES:
                await safe_send_doc(job.bot, job.chat_id, merged_kw_path, caption=cap)
            else:
                dummy_hits = {kw: kw_hits}
                await split_and_send(job.bot, job.chat_id, merged_kw_path, [kw], 0, tier, dummy_hits, job)


async def run_job(job: Job):
    global _current_job
    _current_job = job
    _db["stats"]["total_scans"] += 1
    _db["stats"]["total_users"].add(job.uid)
    await _asave()

    # ── Persist job to disk so it can resume after a crash ───────────
    _save_pending_job(job)

    total_links = len([u for u in job.links if u.strip()])
    job.links_total = total_links
    job.live_hits = {k: 0 for k in job.keywords}
    job._card_lock = asyncio.Lock()

    init_card = _build_stats_card(job, 0, 0.0, "—", 0.0, 0.0, "Starting…")
    card_msg = await safe_send(job.bot, job.chat_id, init_card, parse_mode=ParseMode.MARKDOWN)
    if card_msg:
        job.live_stats_mid = card_msg.message_id

    # GOD MODE connector — tuned for sustained high throughput:
    #   • limit=128: 32 active streams + generous headroom for reconnects
    #   • limit_per_host=96: all 32 parts + retries open freely, no queuing
    #   • tcp_nodelay=True: disables Nagle — sends chunks immediately instead
    #     of buffering, eliminating the 40ms Nagle delay that causes speed dips
    #   • keepalive_timeout=180: keeps connections warm between retries
    #   • ttl_dns_cache=3600: DNS lookup cached for whole job — no repeated
    #     DNS latency adding jitter to reconnects
    if job.god:
        connector = aiohttp.TCPConnector(
            limit=128,
            limit_per_host=96,
            ttl_dns_cache=3600,
            force_close=False,
            enable_cleanup_closed=True,
            keepalive_timeout=180,
            # tcp_nodelay removed — deprecated in newer aiohttp (now always enabled by default)
        )
    elif job.vip:
        connector = aiohttp.TCPConnector(
            limit=32,
            limit_per_host=16,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
            keepalive_timeout=30,
        )
    else:
        connector = aiohttp.TCPConnector(
            limit=16,
            limit_per_host=8,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            read_bufsize=1024 * 1024 * 4,   # 4 MB kernel read buffer — matches OS TCP window
        ) as session:
            valid_links = [(i + 1, url.strip()) for i, url in enumerate(job.links) if url.strip()]

            for link_num, (idx, url) in enumerate(valid_links, 1):
                if job.stopped:
                    break
                # ── Skip links already completed before a crash ──────
                if idx in job.results and job.results[idx].get("status") == "done":
                    log.info(f"Resuming: skipping link {idx} (already done)")
                    continue
                if _db.get("locked", False):
                    await _update_card(job, 0, 0.0, "—",
                                       job.total_bytes_done / 1_048_576,
                                       job.total_bytes_expected / 1_048_576,
                                       "🔒 Bot locked by admin — scan stopped")
                    job.stopped = True
                    break
                if link_num > 1:
                    await _update_card(job, 0, 0.0, "—",
                                       job.total_bytes_done / 1_048_576,
                                       job.total_bytes_expected / 1_048_576,
                                       f"Starting link {idx} ({link_num}/{len(valid_links)})…")
                await process_link(session, url, idx, job)
                # ── Save progress after each link completes ──────────
                _save_pending_job(job)

        if job.stopped:
            done_count = sum(1 for i in job.results.values() if i["status"] == "done")
            await _update_card(job, 0, 0.0, "—",
                               job.total_bytes_done / 1_048_576,
                               job.total_bytes_expected / 1_048_576,
                               f"🛑 Stopped — {done_count}/{job.links_total} links done · sending results…")
            await send_completed_results(job)
            await safe_send(job.bot, job.chat_id,
                "✅ *Done. Results sent above.*\n\nSend /start to run another scan.",
                parse_mode=ParseMode.MARKDOWN)
        else:
            done_mb = job.total_bytes_done / 1_048_576
            exp_mb = job.total_bytes_expected / 1_048_576
            await _update_card(job, 100, 0.0, "✅ Complete",
                               done_mb, exp_mb,
                               f"All {job.links_total} links done · sending results…")

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            spath = RESULTS_DIR / f"summary_{job.uid}_{ts}.txt"
            job.temp_files.append(spath)
            with open(spath, "w", encoding="utf-8") as f:
                f.write("ZY Combo Scanner — Scan Report\n")
                f.write(f"Date     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Keywords : {', '.join(job.keywords)}\n")
                f.write(f"Total DL : {_fmt(done_mb)}\n")
                f.write(f"Tier     : {'VIP' if job.vip else 'Free'}\n\n")
                for i, info in sorted(job.results.items()):
                    f.write(f"Link {i} — {info['status'].upper()}\n")
                    for kw, cnt in info.get("hits", {}).items():
                        f.write(f"  {kw}: {cnt:,} hits\n")
                    f.write("\n")

            # Delegate to send_completed_results which handles both modes correctly
            await send_completed_results(job)

            await safe_send_doc(job.bot, job.chat_id, spath, caption="📋 Scan Summary")
            await safe_send(job.bot, job.chat_id,
                "✅ *All done! Results sent above.*\n\nSend /start to run another scan.",
                parse_mode=ParseMode.MARKDOWN)

    except asyncio.CancelledError:
        await _update_card(job, 0, 0.0, "—",
                           job.total_bytes_done / 1_048_576,
                           job.total_bytes_expected / 1_048_576,
                           "🛑 Scan cancelled — sending completed results…")
        await send_completed_results(job)
    except Exception as e:
        log.error(traceback.format_exc())
        await _update_card(job, 0, 0.0, "—",
                           job.total_bytes_done / 1_048_576,
                           job.total_bytes_expected / 1_048_576,
                           f"💥 Fatal error: {e}")
    finally:
        _clear_pending_job()
        await cleanup_job(job)
        _current_job = None
        _running_tasks.pop(job.uid, None)
        user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
        if not _job_queue.empty():
            try:
                nj = _queue_list[0] if _queue_list else None
                if nj:
                    await safe_send(nj.bot, nj.chat_id,
                        "🔔 *Queue moved — your scan is starting now!*",
                        parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass

# ─────────────────────────────────────────────────────────────────────
#  Queue worker
# ─────────────────────────────────────────────────────────────────────
async def _watchdog_queue_worker():
    while True:
        if _shutdown_event and _shutdown_event.is_set():
            log.info("queue_watchdog: shutdown requested — exiting cleanly.")
            return
        task = asyncio.create_task(queue_worker(), name="queue_worker")
        try:
            await task
            log.info("queue_worker exited cleanly — watchdog stopping.")
            return
        except asyncio.CancelledError:
            log.info("queue_watchdog cancelled — stopping.")
            task.cancel()
            return
        except Exception as e:
            log.error(f"queue_worker crashed: {e}\n{traceback.format_exc()}")
            log.warning("Restarting queue_worker in 3 seconds…")
            await asyncio.sleep(3)


async def queue_worker():
    while True:
        if _shutdown_event and _shutdown_event.is_set():
            log.info("queue_worker: shutdown requested — exiting cleanly.")
            return
        try:
            job = await _job_queue.get()
        except asyncio.CancelledError:
            log.warning("queue_worker: received shutdown CancelledError — exiting.")
            return
        except Exception as e:
            if "Event loop is closed" in str(e):
                log.info("queue_worker: event loop closed — exiting cleanly.")
                return
            log.error(f"queue_worker: error waiting for job: {e}")
            await asyncio.sleep(1)
            continue

        if job in _queue_list:
            _queue_list.remove(job)

        try:
            task = asyncio.create_task(run_job(job))
            _running_tasks[job.uid] = task
            job.cancel_task = task
            await task
        except asyncio.CancelledError:
            log.warning(f"queue_worker: run_job for uid={job.uid} was cancelled — continuing queue.")
            _current_job = None
            _running_tasks.pop(job.uid, None)
            user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
            try:
                await safe_send(job.bot, job.chat_id,
                    "⚠️ *Scan was interrupted.* Use /start to try again.",
                    parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        except Exception as e:
            log.error(f"queue_worker: run_job unhandled error for uid={job.uid}: {e}\n{traceback.format_exc()}")
            try:
                await safe_send(job.bot, job.chat_id,
                    f"💥 *Scan crashed unexpectedly.*\n\n`{e}`\n\nUse /start to try again.",
                    parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
            _current_job = None
            _running_tasks.pop(job.uid, None)
            user_state[job.uid] = {"step": "idle", "links": [], "keywords": []}
        finally:
            _job_queue.task_done()

def _queue_pos(uid: int):
    for i, j in enumerate(_queue_list):
        if j.uid == uid:
            return i + 1
    return None

def _in_queue(uid: int) -> bool:
    return any(j.uid == uid for j in _queue_list)

# ─────────────────────────────────────────────────────────────────────
#  Keyboards
# ─────────────────────────────────────────────────────────────────────
def server_keyboard(uid: int) -> InlineKeyboardMarkup:
    buttons = []
    queue_len = len(_queue_list)
    if _db.get("free_enabled", True):
        buttons.append([InlineKeyboardButton(
            f"🟢 Free Server (Queue: {queue_len})",
            callback_data="server:free")])
    if is_vip(uid):
        buttons.append([InlineKeyboardButton(
            f"⭐ VIP Server 2 (Queue: {queue_len})",
            callback_data="server:vip")])
    if is_god(uid):
        buttons.append([InlineKeyboardButton(
            f"⚡ GOD MODE Server (Queue: {queue_len})",
            callback_data="server:god")])
    return InlineKeyboardMarkup(buttons)

def savemode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Single file (merged)", callback_data="save:single")],
        [InlineKeyboardButton("📂 Separate files per link", callback_data="save:separate")],
    ])

# ─────────────────────────────────────────────────────────────────────
#  Commands
# ─────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    _db["stats"]["total_users"].add(uid)
    await _asave()

    if _in_queue(uid):
        pos = _queue_pos(uid)
        await update.message.reply_text(
            f"⏳ You're queued at *#{pos}*. Use /cancel to leave.",
            parse_mode=ParseMode.MARKDOWN)
        return
    if _current_job and _current_job.uid == uid:
        await update.message.reply_text("⏳ Your scan is running. Use /stop to stop it.")
        return

    # ── Stale session guard (bot restarted mid-flow) ─────────────────
    current_step = user_state[uid].get("step", "idle")
    if current_step not in ("idle",) and not _in_queue(uid):
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}

    missing = await check_joined_channels(ctx.bot, uid)
    if missing:
        ch_lines = "\n".join(f"• `{c}`" for c in missing)
        keyboard = await build_channel_join_keyboard(missing)
        await update.message.reply_text(
            f"📢 *Join Required Channel(s)*\n\n"
            f"{ch_lines}\n\n"
            f"Join the channel(s) above, then click *Check Membership*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard)
        return

    user_state[uid] = {"step": "awaiting_links", "links": [], "keywords": []}
    await update.message.reply_text(
        "📎 Send your download link(s) (comma-separated):\n\n"
        "Example: `https://cdn1.example.xyz/file1.txt`",
        parse_mode=ParseMode.MARKDOWN)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _current_job and _current_job.uid == uid:
        _current_job.stopped = True
        await update.message.reply_text(
            "🛑 *Stop requested!*\n\n"
            "Current link will finish processing, then results will be sent.\n"
            "Please wait…",
            parse_mode=ParseMode.MARKDOWN)
        return
    if _in_queue(uid):
        for j in list(_queue_list):
            if j.uid == uid:
                _queue_list.remove(j)
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Removed from queue. No results to send (scan hadn't started).")
        return
    await update.message.reply_text("Nothing is running. Send /start to begin.")

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if _in_queue(uid):
        for j in list(_queue_list):
            if j.uid == uid:
                _queue_list.remove(j)
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Removed from queue. Send /start to try again.")
        return
    if _current_job and _current_job.uid == uid:
        user_state[uid] = {"step": "idle", "links": [], "keywords": []}
        await update.message.reply_text("🛑 Cancellation requested. Use /stop if you want to receive completed results.")
        return
    await update.message.reply_text("Nothing to cancel. Send /start to begin.")

async def cmd_queue(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    lines = ["📋 *Queue Status*\n"]
    if _current_job:
        you = " ← *you*" if _current_job.uid == uid else ""
        lines.append(f"▶️ Running: `{_current_job.uid}`{you}")
    else:
        lines.append("✅ Nothing running.")
    if _queue_list:
        lines.append(f"\n⏳ Waiting ({len(_queue_list)}):")
        for i, j in enumerate(_queue_list):
            you = " ← *you*" if j.uid == uid else ""
            lines.append(f"  {i+1}. `{j.uid}`{you}")
    else:
        lines.append("📭 Queue empty.")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_vipinfo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    entry = _db["vip"].get(str(uid))
    if not entry or not is_vip(uid):
        await update.message.reply_text(
            "❌ You don't have VIP access.\n\nContact admin to upgrade! 👑")
        return
    if entry.get("permanent"):
        label = "♾ Permanent"
    else:
        exp = datetime.fromisoformat(entry["expires"])
        label = f"⏰ Expires: {exp.strftime('%Y-%m-%d %H:%M')} UTC"
    link_limit = _db.get("link_limit")
    lbl = f"Up to {link_limit} links" if link_limit else "Unlimited links"
    await update.message.reply_text(
        f"👑 *VIP Status*\n\n"
        f"✅ Active\n{label}\n\n"
        f"Perks:\n"
        f"• {lbl}\n"
        f"• {N_PARTS_VIP} parallel download parts ({MAX_WORKERS_VIP} active at once)\n"
        f"• 4 MB chunks — ultra-fast streaming\n"
        f"• Real-time streaming scan — no intermediate files\n"
        f"• No size cap",
        parse_mode=ParseMode.MARKDOWN)

def _admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 Admin only.")
            return
        await func(update, ctx)
    return wrapper

@_admin_only
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    s = _db["stats"]
    vip_count = sum(1 for uid in _db["vip"] if is_vip(int(uid)))
    free_st = "✅ ON" if _db.get("free_enabled", True) else "🔴 OFF"
    lock_st = "🔒 LOCKED" if _db.get("locked", False) else "🔓 Unlocked"
    channels = _db.get("required_channels", [])
    channels_str = "\n    • ".join(channels) if channels else "None"
    link_limit = _db.get("link_limit") or "Unlimited"
    free_limit = _db.get("free_size_limit_mb", 500)
    total_gb = _fmt_gb_total()

    await update.message.reply_text(
        f"📊 *Bot Statistics*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Total users: `{len(s['total_users'])}`\n"
        f"🔍 Total scans: `{s['total_scans']}`\n"
        f"📥 Total downloaded: `{total_gb}`\n"
        f"👑 Active VIPs: `{vip_count}`\n\n"
        f"🔰 Free server: {free_st}\n"
        f"📏 Free size limit: `{free_limit} MB`\n"
        f"🔗 VIP link limit: `{link_limit}`\n\n"
        f"📢 Required channels:\n    • {channels_str}\n\n"
        f"🔒 Lock state: {lock_st}\n"
        f"⏳ Queue length: `{len(_queue_list)}`\n"
        f"▶️ Running: `{'Yes' if _current_job else 'No'}`",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = " ".join(ctx.args)
    if not msg:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    sent = 0
    for uid in list(_db["stats"]["total_users"]):
        try:
            await ctx.bot.send_message(chat_id=uid,
                text=f"📢 *Announcement*\n\n{msg}", parse_mode=ParseMode.MARKDOWN)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await update.message.reply_text(f"✅ Broadcast sent to {sent} user(s).")

@_admin_only
async def cmd_addvip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /addvip <uid> [days]")
        return
    try:
        target = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    days = int(ctx.args[1]) if len(ctx.args) > 1 else None
    if days:
        exp = (datetime.utcnow() + timedelta(days=days)).isoformat()
        _db["vip"][str(target)] = {"expires": exp, "permanent": False}
        label = f"{days} day(s)"
    else:
        _db["vip"][str(target)] = {"permanent": True}
        label = "permanent"
    await _asave()
    await update.message.reply_text(
        f"✅ VIP granted to `{target}` — {label}.", parse_mode=ParseMode.MARKDOWN)
    try:
        await ctx.bot.send_message(chat_id=target,
            text=f"🎉 *You've been upgraded to VIP!*\n\n"
                 f"Duration: {label}\n\nEnjoy unlimited scanning! 👑",
            parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

@_admin_only
async def cmd_rmvip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /rmvip <uid>")
        return
    uid = ctx.args[0]
    if uid in _db["vip"]:
        del _db["vip"][uid]
        await _asave()
        await update.message.reply_text(
            f"✅ VIP removed from `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(
            f"❌ User `{uid}` is not VIP.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_viptrial(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text("Usage: /viptrial <uid> <hours>")
        return
    try:
        target = int(ctx.args[0])
        hours = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return
    exp = (datetime.utcnow() + timedelta(hours=hours)).isoformat()
    _db["vip"][str(target)] = {"expires": exp, "permanent": False}
    await _asave()
    await update.message.reply_text(
        f"✅ VIP trial ({hours}h) granted to `{target}`.", parse_mode=ParseMode.MARKDOWN)
    try:
        await ctx.bot.send_message(chat_id=target,
            text=f"🎉 *VIP Trial activated!*\n\n⏰ Duration: {hours} hour(s)\n\nEnjoy! 👑",
            parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

@_admin_only
async def cmd_subscribers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    vips = [(uid, entry) for uid, entry in _db["vip"].items() if is_vip(int(uid))]
    if not vips:
        await update.message.reply_text("No active VIP users.")
        return
    lines = ["👑 *Active VIP Users*\n"]
    for uid, entry in vips:
        if entry.get("permanent"):
            label = "♾ Permanent"
        else:
            exp = datetime.fromisoformat(entry["expires"])
            label = f"expires {exp.strftime('%Y-%m-%d')}"
        lines.append(f"• `{uid}` — {label}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_addfreeserver(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["free_enabled"] = True
    await _asave()
    await update.message.reply_text("✅ Free server is now *ON*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_rmfreeserver(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["free_enabled"] = False
    await _asave()
    await update.message.reply_text("🔴 Free server is now *OFF*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_addrequiredchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /addrequiredchannel @channelname")
        return
    ch = ctx.args[0]
    if ch not in _db["required_channels"]:
        _db["required_channels"].append(ch)
        await _asave()
    channels_list = ", ".join(f"`{c}`" for c in _db["required_channels"])
    await update.message.reply_text(
        f"✅ Required channel added: `{ch}`\n\n"
        f"📋 Current required channels:\n{channels_list}",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_rmrequiredchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /rmrequiredchannel @channelname")
        return
    ch = ctx.args[0]
    if ch in _db["required_channels"]:
        _db["required_channels"].remove(ch)
        await _asave()
        channels_list = ", ".join(f"`{c}`" for c in _db["required_channels"]) if _db["required_channels"] else "None"
        await update.message.reply_text(
            f"✅ Removed: `{ch}`\n\n"
            f"📋 Remaining required channels:\n{channels_list}",
            parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"❌ Not found: `{ch}`", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_lockall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["locked"] = True
    await _asave()
    if _current_job:
        _current_job.stopped = True
        await safe_send(ctx.bot, _current_job.chat_id,
            "🔒 *Bot locked by admin.* Your scan has been stopped.\n"
            "Sending any completed results…",
            parse_mode=ParseMode.MARKDOWN)
    for j in list(_queue_list):
        _queue_list.remove(j)
        user_state[j.uid] = {"step": "idle", "links": [], "keywords": []}
        try:
            await safe_send(ctx.bot, j.chat_id,
                "🔒 *Bot locked by admin.* You've been removed from the queue.",
                parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
    await update.message.reply_text(
        "🔒 *Bot locked!*\n\n"
        "All running scans stopped. Queue cleared.\n"
        "Users cannot start new scans until /unlockall.",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_unlockall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    _db["locked"] = False
    await _asave()
    await update.message.reply_text(
        "🔓 *Bot unlocked!*\n\nUsers can now start new scans.",
        parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_setlinklimit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        current = _db.get("link_limit") or "Unlimited"
        await update.message.reply_text(
            f"📏 *Current VIP link limit:* `{current}`\n\n"
            "Usage: `/setlinklimit <number>` (0 = unlimited)",
            parse_mode=ParseMode.MARKDOWN)
        return
    try:
        n = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Usage: /setlinklimit <number>")
        return
    if n <= 0:
        _db["link_limit"] = None
        await _asave()
        await update.message.reply_text("✅ VIP link limit removed — *Unlimited*.", parse_mode=ParseMode.MARKDOWN)
    else:
        _db["link_limit"] = n
        await _asave()
        await update.message.reply_text(
            f"✅ VIP link limit set to *{n} links*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_setfreeserverlimit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        current = _db.get("free_size_limit_mb", 500)
        await update.message.reply_text(
            f"📏 *Current free server limit:* `{current} MB`\n\n"
            "Usage: `/setfreeserverlimit 2GB` or `/setfreeserverlimit 500MB`",
            parse_mode=ParseMode.MARKDOWN)
        return
    raw = ctx.args[0].upper().strip()
    try:
        if raw.endswith("GB"):
            mb = float(raw[:-2]) * 1024
        elif raw.endswith("MB"):
            mb = float(raw[:-2])
        else:
            mb = float(raw)
    except ValueError:
        await update.message.reply_text("❌ Invalid format. Examples: `2GB`, `500MB`, `1024`")
        return
    _db["free_size_limit_mb"] = mb
    await _asave()
    label = f"{mb/1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"
    await update.message.reply_text(
        f"✅ Free server size limit set to *{label}*.", parse_mode=ParseMode.MARKDOWN)

@_admin_only
async def cmd_genkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "❌ Usage: `/genkey <duration> [maxuser]`\n\n"
            "Duration examples: `30d` `12h` `45m` `permanent`\n"
            "MaxUser (optional): how many users can redeem (default: 1)",
            parse_mode=ParseMode.MARKDOWN)
        return
    duration_raw = ctx.args[0].lower().strip()
    max_users = 1
    if len(ctx.args) > 1:
        try:
            max_users = int(ctx.args[1])
            if max_users < 1:
                max_users = 1
        except ValueError:
            await update.message.reply_text("❌ maxuser must be a number.")
            return
    minutes = None
    hours = None
    days = None
    seconds_total = None
    label = ""
    if duration_raw in ("permanent", "perm", "∞"):
        label = "Permanent"
    elif duration_raw.endswith("d"):
        try:
            days = int(duration_raw[:-1])
            seconds_total = days * 86400
            label = f"{days} day(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    elif duration_raw.endswith("h"):
        try:
            hours = int(duration_raw[:-1])
            seconds_total = hours * 3600
            label = f"{hours} hour(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    elif duration_raw.endswith("m"):
        try:
            minutes = int(duration_raw[:-1])
            seconds_total = minutes * 60
            label = f"{minutes} minute(s)"
        except ValueError:
            await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
            return
    else:
        await update.message.reply_text("❌ Invalid duration. Use: `30d` `12h` `45m` `permanent`")
        return
    if "vip_keys" not in _db:
        _db["vip_keys"] = {}
    chars = string.ascii_uppercase + string.digits
    raw_k = "".join(secrets.choice(chars) for _ in range(12))
    key = f"ZY-{raw_k[:4]}-{raw_k[4:8]}-{raw_k[8:12]}"
    _db["vip_keys"][key] = {
        "seconds": seconds_total,
        "permanent": seconds_total is None,
        "max_users": max_users,
        "used_count": 0,
        "used_by": [],
        "created": datetime.utcnow().isoformat(),
    }
    await _asave()
    max_lbl = f"{max_users} user(s)" if max_users > 1 else "1 user (one-time)"
    await update.message.reply_text(
        f"🔑 *VIP Key Generated!*\n\n"
        f"`{key}`\n\n"
        f"⏰ Duration: *{label}*\n"
        f"👥 Max redeems: *{max_lbl}*\n\n"
        f"Share with user(s). They redeem with:\n`/redeem {key}`",
        parse_mode=ParseMode.MARKDOWN)


async def cmd_redeem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args:
        await update.message.reply_text(
            "❌ Usage: `/redeem <KEY>`\n\nExample: `/redeem ZY-AB12-CD34-EF56`",
            parse_mode=ParseMode.MARKDOWN)
        return
    key = ctx.args[0].upper().strip()
    keys = _db.get("vip_keys", {})
    if key not in keys:
        await update.message.reply_text("❌ Invalid key. Check it and try again.")
        return
    entry = keys[key]
    max_users = entry.get("max_users", 1)
    used_count = entry.get("used_count", 0)
    used_by = entry.get("used_by", [])
    if used_count >= max_users:
        await update.message.reply_text("❌ This key has reached its maximum redemptions.")
        return
    if uid in used_by:
        await update.message.reply_text("❌ You have already redeemed this key.")
        return
    seconds = entry.get("seconds")
    permanent = entry.get("permanent", False)
    if permanent or seconds is None:
        _db["vip"][str(uid)] = {"permanent": True}
        label = "Permanent"
    else:
        exp = (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
        _db["vip"][str(uid)] = {"expires": exp, "permanent": False}
        if seconds >= 86400:
            label = f"{seconds // 86400} day(s)"
        elif seconds >= 3600:
            label = f"{seconds // 3600} hour(s)"
        else:
            label = f"{seconds // 60} minute(s)"
    entry["used_count"] = used_count + 1
    entry["used_by"] = used_by + [uid]
    entry[f"used_at_{uid}"] = datetime.utcnow().isoformat()
    if max_users == 1:
        entry["used"] = True
        entry["used_by_legacy"] = uid
    await _asave()
    await update.message.reply_text(
        f"🎉 *VIP Activated!*\n\n"
        f"✅ Key accepted\n"
        f"⏰ Duration: *{label}*\n\n"
        f"You now have:\n"
        f"• Unlimited links\n"
        f"• {N_PARTS_VIP} download parts, {MAX_WORKERS_VIP} active at once\n"
        f"• Real-time streaming scan — no intermediate files\n"
        f"• No size cap 👑",
        parse_mode=ParseMode.MARKDOWN)


@_admin_only
async def cmd_listkeys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keys = _db.get("vip_keys", {})
    active = [(k, v) for k, v in keys.items()
              if v.get("used_count", 0) < v.get("max_users", 1)]
    if not active:
        await update.message.reply_text("📭 No active (redeemable) VIP keys.")
        return
    lines = [f"🔑 *Active VIP Keys* ({len(active)})\n"]
    for k, v in active:
        seconds = v.get("seconds")
        permanent = v.get("permanent", False)
        if permanent or seconds is None:
            dur = "♾ Permanent"
        elif seconds >= 86400:
            dur = f"{seconds // 86400}d"
        elif seconds >= 3600:
            dur = f"{seconds // 3600}h"
        else:
            dur = f"{seconds // 60}m"
        used = v.get("used_count", 0)
        maxu = v.get("max_users", 1)
        lines.append(f"• `{k}` — {dur} | {used}/{maxu} used")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@_admin_only
async def cmd_rmkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: `/rmkey <KEY>` or `/rmkey all` or `/rmkey <userid>`",
            parse_mode=ParseMode.MARKDOWN)
        return
    arg = ctx.args[0]
    keys = _db.get("vip_keys", {})
    if arg.lower() == "all":
        count = len(keys)
        _db["vip_keys"] = {}
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted *{count}* VIP key(s).", parse_mode=ParseMode.MARKDOWN)
        return
    key = arg.upper()
    if key in keys:
        del _db["vip_keys"][key]
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted key: `{key}`", parse_mode=ParseMode.MARKDOWN)
        return
    found = []
    for k, v in list(keys.items()):
        if arg in v.get("used_by", []):
            found.append(k)
    if found:
        for k in found:
            del _db["vip_keys"][k]
        await _asave()
        await update.message.reply_text(
            f"🗑 Deleted {len(found)} key(s) used by `{arg}`.",
            parse_mode=ParseMode.MARKDOWN)
        return
    await update.message.reply_text(
        f"❌ Key/user not found: `{arg}`", parse_mode=ParseMode.MARKDOWN)


# ─────────────────────────────────────────────────────────────────────
#  Message handler
# ─────────────────────────────────────────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()
    data = user_state[uid]
    step = data.get("step", "idle")

    if step == "idle":
        await update.message.reply_text("Send /start to begin.")
        return

    if step == "awaiting_links":
        # Split on commas/newlines first so comma-joined URLs are each extracted separately
        parts = re.split(r'[,\n]+', text)
        links = []
        seen_links = set()
        for part in parts:
            part = part.strip()
            for url in URL_RE.findall(part):
                url = url.rstrip('.,;)')  # strip trailing punctuation
                if url and url not in seen_links:
                    seen_links.add(url)
                    links.append(url)
        if not links:
            await update.message.reply_text(
                "⚠️ No valid links found. Please send URL(s).")
            return
        data["links"] = links
        data["step"] = "awaiting_keywords"
        await update.message.reply_text(
            "🔗 *Link saved!*\n\n"
            "Enter Target Sites or Keywords (comma-separated).\n"
            "Example: `crunchyroll.com, netflix.com`",
            parse_mode=ParseMode.MARKDOWN)
        return

    if step == "awaiting_keywords":
        keywords = [k.strip() for k in text.split(",") if k.strip()]
        if not keywords:
            await update.message.reply_text(
                "⚠️ No keywords found. Please send at least one.")
            return
        data["keywords"] = keywords
        data["single_file"] = True  # default to merged for multi-link
        await _show_server_selection(update, ctx, uid, data, keywords)

    elif step in ("queued", "processing", "awaiting_server", "awaiting_savemode"):
        pos = _queue_pos(uid)
        if pos:
            await update.message.reply_text(
                f"⏳ Queued at *#{pos}*. Please wait. /cancel to leave.",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(
                "⏳ Scan is running. Please wait. /stop to stop it.")
    else:
        await update.message.reply_text("Send /start to begin.")


async def _show_server_selection(update, ctx, uid, data, keywords):
    free_on = _db.get("free_enabled", True)
    vip_user = is_vip(uid)
    god_user = is_god(uid)
    if not free_on and not vip_user and not god_user:
        await update.message.reply_text(
            "🔴 *Free server is currently offline.*\n\n"
            "Contact admin for VIP access. 👑",
            parse_mode=ParseMode.MARKDOWN)
        data["step"] = "idle"
        return
    data["step"] = "awaiting_server"
    kb = server_keyboard(uid)
    await update.message.reply_text(
        "🖥 *Select a Processing Server:*\n\n"
        "Choose a server with a low queue for the fastest results.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb)

# ─────────────────────────────────────────────────────────────────────
#  Callback handler
# ─────────────────────────────────────────────────────────────────────
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = user_state[uid]

    # ── Stale session guard ──────────────────────────────────────────
    step = data.get("step", "idle")
    if step == "idle" and query.data not in ("check_membership",):
        await query.edit_message_text(
            "⚠️ *Session expired* (bot was restarted).\n\n"
            "Send /start to begin a new scan.",
            parse_mode=ParseMode.MARKDOWN)
        return

    if query.data == "check_membership":
        missing = await check_joined_channels(ctx.bot, uid)
        if missing:
            ch_lines = "\n".join(f"• `{c}`" for c in missing)
            keyboard = await build_channel_join_keyboard(missing)
            await query.edit_message_text(
                f"📢 *Still missing required channel(s)*\n\n"
                f"{ch_lines}\n\n"
                f"Join the channel(s) above, then click *Check Membership* again.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard)
        else:
            data["step"] = "awaiting_links"
            data["links"] = []
            data["keywords"] = []
            await query.edit_message_text(
                "✅ *Membership verified!*\n\n"
                "📎 Send your download link(s) (comma-separated):\n\n"
                "Example: `https://cdn1.example.xyz/file1.txt`",
                parse_mode=ParseMode.MARKDOWN)
        return

    if query.data.startswith("save:"):
        if data["step"] != "awaiting_savemode":
            await query.edit_message_text("⚠️ Session expired. Send /start to begin.")
            return
        choice = query.data.split(":")[1]
        data["single_file"] = (choice == "single")
        keywords = data["keywords"]
        free_on = _db.get("free_enabled", True)
        vip_user = is_vip(uid)
        god_user = is_god(uid)
        if not free_on and not vip_user and not god_user:
            await query.edit_message_text(
                "🔴 *Free server is currently offline.*\n\nContact admin for VIP access. 👑",
                parse_mode=ParseMode.MARKDOWN)
            data["step"] = "idle"
            return
        total_gb = _fmt_gb_total()
        data["step"] = "awaiting_server"
        kb = server_keyboard(uid)
        await query.edit_message_text(
            "🖥 *Select a Processing Server:*\n\n"
            "Choose a server with a low queue for the fastest results.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb)
        return

    if not query.data.startswith("server:"):
        return
    if data["step"] != "awaiting_server":
        await query.edit_message_text("⚠️ Session expired. Send /start to begin.")
        return

    if _db.get("locked", False):
        await query.edit_message_text(
            "🔒 *Bot is locked by admin.* Please try again later.",
            parse_mode=ParseMode.MARKDOWN)
        data["step"] = "idle"
        return

    choice = query.data.split(":")[1]
    links = data["links"]
    keywords = data["keywords"]
    single_file = data.get("single_file", False)
    god_mode = (choice == "god")
    vip_mode = (choice == "vip") or god_mode

    if god_mode and not is_god(uid):
        await query.edit_message_text("🚫 God Mode is for admins only.")
        return
    if choice == "vip" and not is_vip(uid):
        await query.edit_message_text("🚫 You don't have VIP access.")
        return
    if not vip_mode and not _db.get("free_enabled", True):
        await query.edit_message_text("🔴 Free server is currently offline.")
        return

    # Determine server label for the confirmation message
    if god_mode:
        server_label = "GOD MODE Server"
    elif vip_mode:
        server_label = "VIP Server 2"
    else:
        server_label = "Free Server"

    # Replace the server selection message with the "Job Sent" confirmation
    await query.edit_message_text(
        f"✅ *Job Sent to {server_label}!*\n\n"
        f"The server will download your file and send you the results directly when finished.\n"
        f"(Type /cancel at any time to abort)",
        parse_mode=ParseMode.MARKDOWN)

    job = Job(uid=uid, chat_id=query.message.chat_id,
              links=links, keywords=keywords, bot=ctx.bot,
              vip=vip_mode, god=god_mode, single_file=single_file)
    data["step"] = "queued"

    async with _queue_lock:
        _queue_list.append(job)
        await _job_queue.put(job)

# ─────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────
def main():
    global _job_queue, _queue_lock
    _job_queue = asyncio.Queue()
    _queue_lock = asyncio.Lock()

    log.info("Starting ZY Combo Scanner Bot v8.5 ULTRA — GOD 50-part/32-worker engine…")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(120)
        .connect_timeout(30)
        .pool_timeout(60)
        .build()
    )

    # User commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("vipinfo", cmd_vipinfo))
    app.add_handler(CommandHandler("redeem", cmd_redeem))

    # Admin commands
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("addvip", cmd_addvip))
    app.add_handler(CommandHandler("rmvip", cmd_rmvip))
    app.add_handler(CommandHandler("viptrial", cmd_viptrial))
    app.add_handler(CommandHandler("subscribers", cmd_subscribers))
    app.add_handler(CommandHandler("addfreeserver", cmd_addfreeserver))
    app.add_handler(CommandHandler("rmfreeserver", cmd_rmfreeserver))
    app.add_handler(CommandHandler("addrequiredchannel", cmd_addrequiredchannel))
    app.add_handler(CommandHandler("rmrequiredchannel", cmd_rmrequiredchannel))
    app.add_handler(CommandHandler("lockall", cmd_lockall))
    app.add_handler(CommandHandler("unlockall", cmd_unlockall))
    app.add_handler(CommandHandler("setlinklimit", cmd_setlinklimit))
    app.add_handler(CommandHandler("setfreeserverlimit", cmd_setfreeserverlimit))
    app.add_handler(CommandHandler("genkey", cmd_genkey))
    app.add_handler(CommandHandler("listkeys", cmd_listkeys))
    app.add_handler(CommandHandler("rmkey", cmd_rmkey))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def post_init(application: Application):
        from telegram import BotCommandScopeChat

        # ── Resume any job that was running before a crash/restart ───
        pending = _load_pending_job()
        if pending:
            log.info(f"Found pending job for uid={pending['uid']} — resuming…")
            try:
                resume_job = Job(
                    uid        = pending["uid"],
                    chat_id    = pending["chat_id"],
                    links      = pending["links"],
                    keywords   = pending["keywords"],
                    bot        = application.bot,
                    vip        = pending.get("vip", False),
                    god        = pending.get("god", False),
                    single_file= pending.get("single_file", True),
                )
                # Mark already-done links so they're skipped
                for done_idx in pending.get("done_indices", []):
                    # Rebuild per-keyword result paths from disk
                    kw_paths = {}
                    for kw in resume_job.keywords:
                        safe_kw = re.sub(r'[^\w]', '', kw) or "results"
                        p = RESULTS_DIR / f"link{done_idx}_{safe_kw[:80]}.txt"
                        if p.exists():
                            kw_paths[kw] = str(p)
                    resume_job.results[done_idx] = {
                        "status": "done",
                        "hits": {},
                        "result_paths": kw_paths,
                        "result_path": list(kw_paths.values())[0] if kw_paths else None,
                    }
                    resume_job.links_done += 1

                await application.bot.send_message(
                    chat_id=pending["chat_id"],
                    text=(
                        "♻️ *Bot restarted — resuming your scan!*\n\n"
                        f"Skipping {len(pending.get('done_indices', []))} already-completed link(s).\n"
                        "Continuing from where it left off…"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                )
                user_state[pending["uid"]]["step"] = "queued"
                async with _queue_lock:
                    _queue_list.append(resume_job)
                    await _job_queue.put(resume_job)
            except Exception as e:
                log.error(f"Failed to resume pending job: {e}\n{traceback.format_exc()}")
                _clear_pending_job()
                try:
                    await application.bot.send_message(
                        chat_id=pending["chat_id"],
                        text=(
                            "⚠️ *Bot restarted* but could not resume your scan automatically.\n\n"
                            "Please send /start to run again. Sorry for the interruption!"
                        ),
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass

        public_commands = [
            BotCommand("start", "Start a new scan"),
            BotCommand("stop", "Stop scan (sends completed results)"),
            BotCommand("cancel", "Cancel / leave queue"),
            BotCommand("queue", "Check queue status"),
            BotCommand("vipinfo", "Check your VIP status"),
            BotCommand("redeem", "Redeem a VIP key"),
        ]
        await application.bot.set_my_commands(public_commands)

        admin_commands = public_commands + [
            BotCommand("stats", "📊 Bot statistics"),
            BotCommand("broadcast", "📢 Broadcast message"),
            BotCommand("addvip", "➕ Add VIP user"),
            BotCommand("rmvip", "➖ Remove VIP user"),
            BotCommand("viptrial", "⏱ Give VIP trial"),
            BotCommand("subscribers", "👑 List VIP users"),
            BotCommand("genkey", "🔑 Generate VIP key"),
            BotCommand("listkeys", "📋 List active keys"),
            BotCommand("rmkey", "🗑 Delete key"),
            BotCommand("addfreeserver", "✅ Enable free server"),
            BotCommand("rmfreeserver", "🔴 Disable free server"),
            BotCommand("lockall", "🔒 Lock bot"),
            BotCommand("unlockall", "🔓 Unlock bot"),
            BotCommand("setlinklimit", "📏 Set VIP link limit"),
            BotCommand("setfreeserverlimit", "📏 Set free server size limit"),
            BotCommand("addrequiredchannel", "📢 Add required channel"),
            BotCommand("rmrequiredchannel", "🗑 Remove required channel"),
        ]
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeChat(chat_id=admin_id)
                )
            except Exception as e:
                log.warning(f"Could not set admin commands for {admin_id}: {e}")

        _shutdown_event = asyncio.Event()
        _watchdog_task = asyncio.create_task(_watchdog_queue_worker(), name="queue_watchdog")
        log.info("Queue worker started.")

    async def post_stop(self, app: Application):
        """Called after stopping the application - give tasks time to exit cleanly."""
        log.info("Post-stop: signaling shutdown...")
        if _shutdown_event:
            _shutdown_event.set()
        # Give tasks a moment to notice the shutdown flag and exit cleanly
        await asyncio.sleep(0.5)

    async def post_shutdown(self, app: Application):
        """Graceful shutdown handler - cancel background tasks cleanly."""
        log.info("Shutting down gracefully...")
        if _watchdog_task and not _watchdog_task.done():
            _watchdog_task.cancel()
            try:
                await asyncio.wait_for(_watchdog_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        log.info("Shutdown complete.")

    app.post_init = post_init
    app.post_stop = post_stop
    app.post_shutdown = post_shutdown
    log.info("Bot is running…")
    # Use close_loop=False to prevent the event loop from closing before our tasks can finish
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, close_loop=False)


if __name__ == "__main__":
    main()
