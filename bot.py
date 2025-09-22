#!/usr/bin/env python3
# bot.py
# Single-file Telegram bot implementing upload sessions, delivery, DB backups, forced channels,
# start/help messages, broadcasts, admin panel, persistent auto-delete jobs, and healthcheck.
# Minimal comments, production-oriented error handling and logging.

import os
import sys
import json
import time
import sqlite3
import logging
import asyncio
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.exceptions import ChatNotFound, BotBlocked, RetryAfter, TelegramAPIError

# Environment
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID", "0") or 0)
DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID", "0") or 0)
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

if not BOT_TOKEN or OWNER_ID == 0 or UPLOAD_CHANNEL_ID == 0 or DB_CHANNEL_ID == 0:
    print("Missing required environment variables: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID")
    sys.exit(1)

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s'
)
logger = logging.getLogger("uploader_bot")

# Bot and dispatcher
bot = Bot(token=BOT_TOKEN, parse_mode=None)  # we'll send plain text
dp = Dispatcher(bot)

# Scheduler with SQLAlchemyJobStore
jobstore_url = f"sqlite:///{JOB_DB_PATH}"
scheduler = AsyncIOScheduler()
scheduler.add_jobstore(SQLAlchemyJobStore(url=jobstore_url), 'default')

# In-memory session collector for owner during /upload
_upload_sessions: Dict[int, Dict[str, Any]] = {}

# Utility helpers
def now_ts() -> int:
    return int(time.time())

def iso_now() -> str:
    return datetime.utcnow().isoformat()

def ensure_dir_for_path(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

ensure_dir_for_path(DB_PATH)
ensure_dir_for_path(JOB_DB_PATH)

# SQLite DB helper (application DB)
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_id INTEGER,
        created_at TEXT,
        protect INTEGER DEFAULT 0,
        auto_delete_hours INTEGER DEFAULT 0,
        vault_msg_id INTEGER,
        deep_link TEXT,
        revoked INTEGER DEFAULT 0
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS session_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER,
        file_type TEXT,
        file_id TEXT,
        caption TEXT,
        original_chat_id INTEGER,
        original_msg_id INTEGER,
        vault_msg_id INTEGER,
        created_at TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        last_seen INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS delete_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        chat_id INTEGER,
        message_ids TEXT,
        run_at INTEGER,
        session_id INTEGER,
        created_at TEXT
    )
    """)
    conn.commit()
    conn.close()

init_db()

# Settings accessor
def get_setting(key: str, default: Optional[str]=None) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()
    conn.close()

# Channel storage functions (JSON arrays)
def get_channels(key: str) -> List[Dict[str, str]]:
    raw = get_setting(key, "[]")
    try:
        return json.loads(raw)
    except Exception:
        return []

def set_channels(key: str, channels: List[Dict[str, str]]):
    set_setting(key, json.dumps(channels))

# DB Backup helpers
async def upload_and_pin_db():
    try:
        # Upload DB file to DB_CHANNEL_ID and pin it.
        if not os.path.exists(DB_PATH):
            logger.error("Local DB file not found for backup.")
            return None
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(DB_CHANNEL_ID, f, caption=f"DB backup {iso_now()}")
        try:
            await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
        except ChatNotFound:
            logger.error("Bot not found in DB channel when pinning backup.")
        except Exception as e:
            logger.exception("Failed to pin DB backup: %s", e)
        return sent
    except ChatNotFound:
        logger.error("DB_CHANNEL_ID not found or bot not a member (ChatNotFound).")
        return None
    except Exception as e:
        logger.exception("upload_and_pin_db failed: %s", e)
        return None

async def restore_db_from_pinned():
    # If local DB missing, attempt to restore from pinned message in DB_CHANNEL_ID
    if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0:
        return False
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned = getattr(chat, "pinned_message", None)
        if pinned and pinned.document:
            file = await bot.get_file(pinned.document.file_id)
            ensure_dir_for_path(DB_PATH)
            await file.download(destination=DB_PATH)
            logger.info("Restored DB from pinned backup.")
            return True
        else:
            # fallback: search last messages for a document
            async for msg in bot.iter_history(DB_CHANNEL_ID, limit=20):
                if msg.document:
                    file = await bot.get_file(msg.document.file_id)
                    ensure_dir_for_path(DB_PATH)
                    await file.download(destination=DB_PATH)
                    logger.info("Restored DB from recent backup message.")
                    return True
    except ChatNotFound:
        logger.error("DB channel not found when attempting restore.")
    except Exception as e:
        logger.exception("Failed to restore DB: %s", e)
    return False

# Startup restoration and job reload
async def restore_state_and_jobs():
    try:
        await restore_db_from_pinned()
    except Exception:
        logger.exception("Error during DB restore.")
    # Load pending delete_jobs from DB table and schedule them
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM delete_jobs")
    rows = cur.fetchall()
    conn.close()
    for row in rows:
        job_id = row["job_id"]
        chat_id = row["chat_id"]
        message_ids = json.loads(row["message_ids"])
        run_at = int(row["run_at"])
        session_id = row["session_id"]
        if run_at <= now_ts():
            # run immediately
            asyncio.create_task(execute_delete_job_internal(chat_id, message_ids, session_id, remove_db=True))
        else:
            try:
                scheduler.add_job(
                    execute_delete_job_internal,
                    trigger=DateTrigger(run_date=datetime.utcfromtimestamp(run_at)),
                    args=[chat_id, message_ids, session_id, True],
                    id=str(job_id),
                    replace_existing=True
                )
                logger.info("Restored scheduled delete job %s", job_id)
            except Exception:
                logger.exception("Failed to schedule restored job %s", job_id)

# Execution of delete jobs
async def execute_delete_job_internal(chat_id: int, message_ids: List[int], session_id: Optional[int], remove_db: bool=False):
    # Attempt to delete messages in chat
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except ChatNotFound:
            logger.warning("ChatNotFound when deleting msg %s in chat %s", mid, chat_id)
        except BotBlocked:
            logger.warning("BotBlocked when deleting msg %s in chat %s", mid, chat_id)
        except TelegramAPIError as e:
            logger.warning("TelegramAPIError deleting message %s in chat %s: %s", mid, chat_id, e)
        except Exception:
            logger.exception("Unexpected error deleting message %s in chat %s", mid, chat_id)
    if remove_db:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM delete_jobs WHERE job_id = ?", (str(session_id),))
            conn.commit()
            conn.close()
            logger.info("Removed delete job DB entry for session %s", session_id)
        except Exception:
            logger.exception("Failed to remove delete job DB entry.")

# User registration/updating
def update_user_record(user: types.User):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO users (user_id, username, first_name, last_name, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            last_seen=excluded.last_seen
        """, (user.id, user.username or "", user.first_name or "", user.last_name or "", now_ts()))
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Failed to update user record.")

# Admin check decorator
def owner_only(func):
    async def wrapper(message: types.Message):
        if message.from_user.id != OWNER_ID:
            await message.reply("Unauthorized.")
            return
        return await func(message)
    return wrapper

# Helper to escape text (we will send plain text)
def safe_text(text: Optional[str]) -> str:
    return text or ""

# Upload session handlers
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Only owner can start upload.")
        return
    args = message.get_args().strip()
    exclude_text = False
    if args.lower() == "exclude_text" or args.lower() == "exclude_text=true":
        exclude_text = True
    _upload_sessions[message.from_user.id] = {
        "started_at": now_ts(),
        "exclude_text": exclude_text,
        "collected": [],  # list of (chat_id, message_id)
        "owner": message.from_user.id
    }
    await message.reply(f"Upload session started. exclude_text={exclude_text}. Send files/text. Use /d to finish, /e to cancel.")

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    sess = _upload_sessions.pop(message.from_user.id, None)
    if sess:
        await message.reply("Upload session canceled and cleared.")
    else:
        await message.reply("No active upload session.")

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    sess = _upload_sessions.get(message.from_user.id)
    if not sess:
        await message.reply("No active upload session.")
        return
    # Ask for protect ON/OFF via InlineKeyboard
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("Protect: ON", callback_data=f"final_protect:1"),
        types.InlineKeyboardButton("Protect: OFF", callback_data=f"final_protect:0")
    )
    await message.reply("Choose Protect option (prevents forwarding/downloading for non-owner):", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("final_protect:"))
async def on_protect_choice(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Unauthorized", show_alert=True)
        return
    _, val = callback.data.split(":", 1)
    protect = int(val)
    # ask for auto-delete hours
    kb = types.InlineKeyboardMarkup()
    for h in [0, 1, 6, 24, 72, 168]:
        kb.add(types.InlineKeyboardButton(f"{h}h", callback_data=f"final_autodel:{h}:{protect}"))
    await callback.message.reply("Choose auto-delete timer in hours (0 = no auto-delete):", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("final_autodel:"))
async def on_autodel_choice(callback: types.CallbackQuery):
    if callback.from_user.id != OWNER_ID:
        await callback.answer("Unauthorized", show_alert=True)
        return
    try:
        _, hours_str, protect_str = callback.data.split(":")
        hours = int(hours_str)
        protect = int(protect_str)
    except Exception:
        await callback.answer("Invalid selection", show_alert=True)
        return
    sess = _upload_sessions.pop(callback.from_user.id, None)
    if not sess:
        await callback.answer("Session expired or missing.", show_alert=True)
        return
    collected = sess.get("collected", [])
    exclude_text = sess.get("exclude_text", False)
    # Create session DB row
    conn = get_conn()
    cur = conn.cursor()
    created_at = iso_now()
    cur.execute("INSERT INTO sessions (owner_id, created_at, protect, auto_delete_hours, revoked) VALUES (?, ?, ?, ?, ?)",
                (callback.from_user.id, created_at, protect, hours, 0))
    session_id = cur.lastrowid
    conn.commit()
    # Create deep link
    try:
        me = await bot.get_me()
        bot_username = me.username
    except Exception:
        bot_username = None
    deep_link = f"https://t.me/{bot_username}?start={session_id}" if bot_username else f"tg://bot_start?start={session_id}"
    cur.execute("UPDATE sessions SET deep_link = ? WHERE id = ?", (deep_link, session_id))
    conn.commit()
    # Post header placeholder to UPLOAD_CHANNEL_ID
    header_text = f"Session #{session_id}\nOwner: {callback.from_user.id}\nProtect: {protect}\nAuto-delete hours: {hours}\nLink: {deep_link}\n\nUploading content..."
    try:
        header_msg = await bot.send_message(UPLOAD_CHANNEL_ID, header_text)
        header_msg_id = header_msg.message_id
    except ChatNotFound:
        header_msg = None
        header_msg_id = None
        logger.error("UPLOAD_CHANNEL_ID not found when posting header.")
    # Copy messages into upload channel and save file metadata
    saved_files = []
    for chat_id, msg_id in collected:
        try:
            # Fetch original to see type and caption
            # Use forward? We'll copy to preserve file without reupload where possible
            copied = await bot.copy_message(UPLOAD_CHANNEL_ID, chat_id, msg_id)
            vault_msg_id = copied.message_id
            # Determine file info
            original = await bot.get_chat(chat_id)
            # Try to get original message content by fetching message via get_messages? Not available; we have message id only.
            # Instead, attempt to get message from owner chat if original chat is owner's chat
            caption = ""
            file_type = "unknown"
            file_id = ""
            # Inspect copied message object to derive file info
            for attr in ("document", "photo", "video", "voice", "audio", "sticker", "animation"):
                obj = getattr(copied, attr, None)
                if obj:
                    file_type = attr
                    if attr == "photo":
                        # photo is list
                        if isinstance(obj, list):
                            file_id = obj[-1].file_id
                        else:
                            file_id = obj.file_id
                    elif hasattr(obj, "file_id"):
                        file_id = obj.file_id
                    break
            if hasattr(copied, "caption") and copied.caption:
                caption = copied.caption
            cur.execute("""
            INSERT INTO session_files
            (session_id, file_type, file_id, caption, original_chat_id, original_msg_id, vault_msg_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, file_type, file_id, caption, chat_id, msg_id, vault_msg_id, created_at))
            conn.commit()
            saved_files.append((file_type, file_id))
        except ChatNotFound:
            logger.error("ChatNotFound when copying msg %s from chat %s", msg_id, chat_id)
        except Exception:
            logger.exception("Error copying message %s from chat %s", msg_id, chat_id)
    # Edit header with final link/reference
    if header_msg_id:
        try:
            final_header = f"Session #{session_id}\nOwner: {callback.from_user.id}\nProtect: {protect}\nAuto-delete hours: {hours}\nLink: {deep_link}\nFiles: {len(saved_files)}"
            await bot.edit_message_text(final_header, UPLOAD_CHANNEL_ID, header_msg_id)
        except Exception:
            logger.exception("Failed editing header message.")
    # Update session vault_msg_id with header message id if any
    cur.execute("UPDATE sessions SET vault_msg_id = ? WHERE id = ?", (header_msg_id or 0, session_id))
    conn.commit()
    conn.close()
    await callback.message.reply(f"Session #{session_id} finalized with {len(saved_files)} files. Link: {deep_link}")
    # Upload DB backup and pin
    await upload_and_pin_db()
    await callback.answer()

# Message collector: collect owner messages during upload session
@dp.message_handler(lambda message: message.from_user and message.from_user.id == OWNER_ID, content_types=types.ContentTypes.ANY)
async def collect_owner_messages(message: types.Message):
    sess = _upload_sessions.get(message.from_user.id)
    if not sess:
        # Not in upload mode; ignore for collection
        return
    # Ignore commands
    if message.text and message.text.strip().startswith("/"):
        return
    # Ignore service messages
    if message.content_type == "service":
        return
    # If exclude_text and message is plain text, skip
    if sess.get("exclude_text") and message.content_type == "text":
        return
    # Save a tuple of chat_id and message_id for copying later
    sess["collected"].append((message.chat.id, message.message_id))
    try:
        await message.react("✅")
    except Exception:
        pass

# /start handler
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    update_user_record(message.from_user)
    args = message.get_args().strip()
    start_text = get_setting("start_text", "Hello {first_name}! Use /help.")
    help_text = get_setting("help_text", "Help text here.")
    start_text_formatted = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
    kb = types.InlineKeyboardMarkup(row_width=2)
    # Add help button
    kb.add(types.InlineKeyboardButton("Help", callback_data="show_help"))
    # Forced channels
    forced = get_channels("force_channels")
    optional = get_channels("optional_channels")
    for ch in forced:
        kb.add(types.InlineKeyboardButton(ch.get("name", "Channel"), url=ch.get("link", "")))
    for ch in optional:
        kb.add(types.InlineKeyboardButton(ch.get("name", "Channel"), url=ch.get("link", "")))
    if not args:
        await message.reply(start_text_formatted, reply_markup=kb)
        return
    # Payload provided: expected to be session_id
    try:
        session_id = int(args.split()[0])
    except Exception:
        await message.reply("Invalid link payload.")
        return
    # Check session exists and not revoked
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
    srow = cur.fetchone()
    if not srow:
        await message.reply("Session not found or expired.")
        conn.close()
        return
    if srow["revoked"]:
        await message.reply("This session has been revoked.")
        conn.close()
        return
    # Get force channels and check memberships where possible
    force_channels = get_channels("force_channels")
    need_check = []
    unverified = []
    for ch in force_channels:
        link = ch.get("link")
        name = ch.get("name", "Channel")
        resolved = await resolve_channel_to_id(link)
        if resolved is None:
            unverified.append((name, link))
        else:
            need_check.append((name, link, resolved))
    # Check membership for resolvable ones
    not_member = []
    for name, link, ch_id in need_check:
        try:
            member = await bot.get_chat_member(ch_id, message.from_user.id)
            if member.status in ("creator", "administrator", "member"):
                continue
            else:
                not_member.append((name, link))
        except ChatNotFound:
            logger.warning("Bot can't access channel %s (%s) to verify membership.", name, link)
            unverified.append((name, link))
        except Exception:
            logger.exception("Error checking membership for channel %s", link)
            unverified.append((name, link))
    if not_member or unverified:
        kb2 = types.InlineKeyboardMarkup(row_width=1)
        for name, link in not_member + unverified:
            kb2.add(types.InlineKeyboardButton(name, url=link))
        kb2.add(types.InlineKeyboardButton("Retry", callback_data=f"retry:{session_id}"))
        await message.reply("You must join the following channels first:", reply_markup=kb2)
        conn.close()
        return
    # All checks passed; deliver session files
    cur.execute("SELECT * FROM session_files WHERE session_id = ? ORDER BY id ASC", (session_id,))
    files = cur.fetchall()
    delivered_message_ids = []
    protect_flag = bool(srow["protect"])
    auto_delete_hours = int(srow["auto_delete_hours"] or 0)
    for f in files:
        try:
            # Copy from UPLOAD_CHANNEL_ID using vault_msg_id saved when uploading
            vault_id = f["vault_msg_id"]
            copied = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, vault_id, protect_content=protect_flag and message.from_user.id != OWNER_ID)
            if copied:
                delivered_message_ids.append(copied.message_id)
        except ChatNotFound:
            logger.warning("Upload channel message not found when delivering session.")
        except RetryAfter as e:
            logger.warning("RetryAfter when delivering; sleeping %s", e.timeout)
            await asyncio.sleep(e.timeout)
            try:
                copied = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, vault_id, protect_content=protect_flag and message.from_user.id != OWNER_ID)
                if copied:
                    delivered_message_ids.append(copied.message_id)
            except Exception:
                logger.exception("Failed after retry.")
        except BotBlocked:
            logger.warning("Bot blocked by user %s", message.from_user.id)
            break
        except Exception:
            logger.exception("Failed to deliver file %s", f["id"])
    # Schedule auto-delete if needed
    if auto_delete_hours > 0 and delivered_message_ids:
        run_at = now_ts() + auto_delete_hours * 3600
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO delete_jobs (job_id, chat_id, message_ids, run_at, session_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (str(session_id), message.chat.id, json.dumps(delivered_message_ids), run_at, session_id, iso_now()))
        job_row_id = cur.lastrowid
        conn.commit()
        conn.close()
        try:
            scheduler.add_job(
                execute_delete_job_internal,
                trigger=DateTrigger(run_date=datetime.utcfromtimestamp(run_at)),
                args=[message.chat.id, delivered_message_ids, session_id, True],
                id=str(job_row_id),
                replace_existing=True
            )
        except Exception:
            logger.exception("Failed to schedule auto-delete job.")
    await message.reply("Delivery complete.")
    conn.close()

# Resolve channel link to numeric id if possible
async def resolve_channel_to_id(link: str) -> Optional[int]:
    if not link:
        return None
    link = link.strip()
    # Numeric -100 prefix
    try:
        if link.startswith("-100") or (link.startswith("-") and link[1:].isdigit()):
            return int(link)
        if link.startswith("@"):
            chat = await bot.get_chat(link)
            return chat.id
        if link.startswith("https://t.me/") or link.startswith("http://t.me/"):
            username = link.rsplit("/", 1)[-1]
            if username:
                try:
                    chat = await bot.get_chat(username)
                    return chat.id
                except Exception:
                    return None
    except Exception:
        return None
    return None

# Retry callback for forced channels
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("retry:"))
async def on_retry(callback: types.CallbackQuery):
    await callback.answer("Re-checking channels... Please re-open the deep link if needed.")
    # Try to re-check membership and deliver if possible
    try:
        _, sid = callback.data.split(":")
        fake_msg = types.Message(
            message_id=callback.message.message_id,
            date=callback.message.date,
            chat=callback.message.chat,
            from_user=callback.from_user,
            content_type="text",
            text=f"/start {sid}"
        )
        # Can't call handler directly easily; instruct user to re-open link
        await callback.message.reply("Please re-open the deep link (click the original session link) to retry delivery.")
    except Exception:
        await callback.message.reply("Retry failed.")

# /setmessage handler
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    args = message.get_args()
    if not args and not message.reply_to_message:
        await message.reply("Usage: /setmessage <start|help> Your message OR reply to a message with /setmessage <start|help>")
        return
    parts = args.split(None, 1)
    key = parts[0].lower() if parts else ""
    if key not in ("start", "help"):
        await message.reply("Key must be 'start' or 'help'.")
        return
    if message.reply_to_message and message.reply_to_message.text:
        text = message.reply_to_message.text
    elif len(parts) > 1:
        text = parts[1]
    else:
        await message.reply("No message content found.")
        return
    set_setting(f"{key}_text", text)
    await message.reply(f"{key.capitalize()} message updated.")

# /setimage handler
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip().lower()
    if args not in ("start", "help"):
        await message.reply("Usage: reply to a photo with /setimage start|help")
        return
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("Please reply to a photo to set as start/help image.")
        return
    largest = message.reply_to_message.photo[-1]
    file_id = largest.file_id
    set_setting(f"{args}_image", file_id)
    await message.reply(f"{args.capitalize()} image saved.")

# /setchannel and /setforcechannel
@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none")
        return
    if args.lower() == "none":
        set_channels("optional_channels", [])
        await message.reply("Optional channels cleared.")
        return
    try:
        name, link = args.split(None, 1)
    except Exception:
        await message.reply("Usage: /setchannel <name> <channel_link>")
        return
    channels = get_channels("optional_channels")
    if len(channels) >= 4:
        await message.reply("Max 4 optional channels.")
        return
    channels.append({"name": name, "link": link})
    set_channels("optional_channels", channels)
    await message.reply(f"Optional channel '{name}' set.")

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setforcechannel <name> <channel_link> OR /setforcechannel none")
        return
    if args.lower() == "none":
        set_channels("force_channels", [])
        await message.reply("Forced channels cleared.")
        return
    try:
        name, link = args.split(None, 1)
    except Exception:
        await message.reply("Usage: /setforcechannel <name> <channel_link>")
        return
    channels = get_channels("force_channels")
    if len(channels) >= 3:
        await message.reply("Max 3 forced channels.")
        return
    channels.append({"name": name, "link": link})
    set_channels("force_channels", channels)
    await message.reply(f"Forced channel '{name}' set.")

# Admin panel and help
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    text = (
        "Admin Panel:\n"
        "/upload\n"
        "/d (finalize)\n"
        "/e (cancel)\n"
        "/setmessage\n"
        "/setimage\n"
        "/setchannel\n"
        "/setforcechannel\n"
        "/list_sessions\n"
        "/revoke <id>\n"
        "/broadcast (reply)\n"
        "/backup_db\n"
        "/restore_db\n"
        "/stats\n"
    )
    await message.reply(text)

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    help_text = get_setting("help_text", "Default help text. Owner can set via /setmessage help.")
    await message.reply(help_text)

# Session listing and revoke
@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, owner_id, created_at, protect, auto_delete_hours, revoked FROM sessions ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    lines = []
    for r in rows:
        lines.append(f"#{r['id']} owner:{r['owner_id']} created:{r['created_at']} protect:{bool(r['protect'])} auto_del:{r['auto_delete_hours']} revoked:{bool(r['revoked'])}")
    await message.reply("\n".join(lines) if lines else "No sessions.")

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args.isdigit():
        await message.reply("Usage: /revoke <session_id>")
        return
    sid = int(args)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE sessions SET revoked = 1 WHERE id = ?", (sid,))
    conn.commit()
    conn.close()
    await message.reply(f"Session {sid} revoked.")

# Stats
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    conn = get_conn()
    cur = conn.cursor()
    two_days_ago = now_ts() - 2 * 24 * 3600
    cur.execute("SELECT COUNT(*) as c FROM users WHERE last_seen >= ?", (two_days_ago,))
    active = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM users")
    total_users = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM session_files")
    total_files = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM sessions")
    total_sessions = cur.fetchone()["c"]
    conn.close()
    await message.reply(f"Active (2d): {active}\nTotal users: {total_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")

# Broadcast
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message to broadcast.")
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    rows = cur.fetchall()
    conn.close()
    user_ids = [r["user_id"] for r in rows]
    total = len(user_ids)
    await message.reply(f"Broadcasting to {total} users...")
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    results = {"sent": 0, "failed": 0}
    async def worker(uid):
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                results["sent"] += 1
            except BotBlocked:
                results["failed"] += 1
            except ChatNotFound:
                results["failed"] += 1
            except Exception:
                results["failed"] += 1
    tasks = [worker(uid) for uid in user_ids]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast done. Sent: {results['sent']}, Failed: {results['failed']}")

# Backup and restore commands
@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    sent = await upload_and_pin_db()
    if sent:
        await message.reply("DB backup uploaded and pinned.")
    else:
        await message.reply("DB backup failed (see logs).")

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Unauthorized.")
        return
    ok = await restore_db_from_pinned()
    if ok:
        await message.reply("DB restored from pinned backup.")
    else:
        await message.reply("DB restore failed (see logs).")

# Health server
async def health(request):
    return web.Response(text="ok")

def start_health_server():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    async def _run():
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
    asyncio.create_task(_run())

# Error handlers
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Update caused error: %s", exception)
    return True

# Startup and shutdown
async def on_startup(dp):
    logger.info("Bot starting...")
    start_health_server()
    scheduler.start()
    await restore_state_and_jobs()
    logger.info("Startup tasks completed.")

async def on_shutdown(dp):
    logger.info("Shutting down...")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    await bot.close()

# Run polling
if __name__ == "__main__":
    # Ensure scheduler started in event loop
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)