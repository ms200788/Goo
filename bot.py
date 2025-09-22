#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Upload Bot - Full-featured single-file implementation
Features:
- Two separate channels: UPLOAD_CHANNEL (stores session files) and DB_CHANNEL (stores DB backups & timer metadata)
- Persistent SQLite DB with automatic backup to DB_CHANNEL (pinned)
- Restore DB from pinned backup on startup
- Upload flow: /upload -> send files/text (optional) -> /d -> choose protect -> choose auto-delete hours -> session saved
- /start deep-links deliver files with original captions; supports protect_content and persistent auto-delete
- /setmessage to edit start/help texts (supports {username} and {first_name})
- /setimage to set images for start/help
- /setchannel (up to 4 non-forced channels) and /setforcechannel (up to 3 forced channels)
- If a user hasn't joined forced channels, the bot shows inline join buttons + retry (keeps original intent)
- /adminp shows owner commands; /help is public but shows minimal usage (owner-only detailed commands via /adminp)
- Auto-delete timers are persisted in DB and reloaded on startup (overdue executed immediately)
- Health endpoint at /health (binds to 0.0.0.0:$PORT in on_startup) for Render / UptimeRobot
- All owner commands protected by OWNER_ID env var
- Deployable via Dockerfile and requirements included at bottom
Minimal comments to keep file compact; environment variables expected:
BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID, DB_PATH (optional), PORT (optional)
"""

import os
import sys
import time
import json
import sqlite3
import logging
import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import closing

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.deep_linking import get_start_link, decode_payload
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from aiohttp import web

# -------------------
# Configuration (env)
# -------------------
def required_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"ERROR: {name} environment variable required", file=sys.stderr)
        sys.exit(1)
    return v

BOT_TOKEN = required_env("BOT_TOKEN")
try:
    OWNER_ID = int(required_env("OWNER_ID"))
except Exception:
    print("ERROR: OWNER_ID must be integer", file=sys.stderr)
    sys.exit(1)

try:
    UPLOAD_CHANNEL_ID = int(required_env("UPLOAD_CHANNEL_ID"))
except Exception:
    print("ERROR: UPLOAD_CHANNEL_ID must be integer (e.g. -100123...)", file=sys.stderr)
    sys.exit(1)

try:
    DB_CHANNEL_ID = int(required_env("DB_CHANNEL_ID"))
except Exception:
    print("ERROR: DB_CHANNEL_ID must be integer (e.g. -100123...)", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

# -------------------
# Logging
# -------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s")
logger = logging.getLogger("upload-bot")

# -------------------
# Bot / Dispatcher
# -------------------
bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(bot, storage=MemoryStorage())

# -------------------
# DB and Scheduler
# -------------------
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
jobstore_url = f"sqlite:///{os.path.abspath(JOB_DB_PATH)}"
jobstores = {'default': SQLAlchemyJobStore(url=jobstore_url)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.start()

# -------------------
# SQLite wrapper
# -------------------
class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._create()

    def _create(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_active INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS start_help (
            id INTEGER PRIMARY KEY,
            type TEXT UNIQUE, -- 'start' or 'help'
            content TEXT,
            file_id TEXT
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER,
            created_at INTEGER,
            protect INTEGER,
            auto_delete INTEGER,
            header_vault_msg_id INTEGER,
            link_vault_msg_id INTEGER,
            revoked INTEGER DEFAULT 0
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            vault_msg_id INTEGER,
            file_id TEXT,
            file_type TEXT,
            caption TEXT,
            position INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS delete_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            chat_id INTEGER,
            message_ids TEXT,
            run_at INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)
        self.conn.commit()

    # Users
    def add_or_update_user(self, user_id:int, username:Optional[str], first_name:Optional[str]):
        now = int(time.time())
        self.cur.execute("""
        INSERT INTO users (user_id, username, first_name, last_active)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_active=excluded.last_active
        ;
        """, (user_id, username or "", first_name or "", now))
        self.conn.commit()

    def touch_user(self, user_id:int):
        now = int(time.time())
        self.cur.execute("UPDATE users SET last_active=? WHERE user_id=?", (now, user_id))
        self.conn.commit()

    def get_all_users(self) -> List[int]:
        rows = self.cur.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]

    def count_users(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def count_active_2days(self)->int:
        cutoff = int(time.time()) - 2*86400
        return self.cur.execute("SELECT COUNT(*) FROM users WHERE last_active>=?", (cutoff,)).fetchone()[0]

    # start/help messages
    def set_message(self, which:str, content:str, file_id:Optional[str]=None):
        self.cur.execute("INSERT INTO start_help (type, content, file_id) VALUES (?, ?, ?) ON CONFLICT(type) DO UPDATE SET content=excluded.content, file_id=excluded.file_id", (which, content, file_id))
        self.conn.commit()

    def get_message(self, which:str) -> Tuple[str, Optional[str]]:
        row = self.cur.execute("SELECT content, file_id FROM start_help WHERE type=?", (which,)).fetchone()
        if row:
            return row["content"], row["file_id"]
        default = "Welcome, {username}!" if which=="start" else "Available commands: /start /help"
        return default, None

    # sessions & files
    def create_session(self, owner_id:int, protect:int, auto_delete:int, header_vault_msg_id:int, link_vault_msg_id:int) -> int:
        now = int(time.time())
        self.cur.execute("INSERT INTO sessions (owner_id, created_at, protect, auto_delete, header_vault_msg_id, link_vault_msg_id) VALUES (?,?,?,?,?,?)",
                         (owner_id, now, protect, auto_delete, header_vault_msg_id, link_vault_msg_id))
        sid = self.cur.lastrowid
        self.conn.commit()
        return sid

    def add_file(self, session_id:int, vault_msg_id:int, file_id:str, file_type:str, caption:str, position:int):
        self.cur.execute("INSERT INTO files (session_id, vault_msg_id, file_id, file_type, caption, position) VALUES (?,?,?,?,?,?)",
                         (session_id, vault_msg_id, file_id, file_type, caption, position))
        self.conn.commit()

    def get_session(self, sid:int):
        return self.cur.execute("SELECT * FROM sessions WHERE id=? LIMIT 1", (sid,)).fetchone()

    def get_files(self, sid:int) -> List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY position ASC", (sid,)).fetchall()

    def revoke_session(self, sid:int):
        self.cur.execute("UPDATE sessions SET revoked=1 WHERE id=?", (sid,))
        self.conn.commit()

    def is_revoked(self, sid:int) -> bool:
        row = self.cur.execute("SELECT revoked FROM sessions WHERE id=?", (sid,)).fetchone()
        return bool(row["revoked"]) if row else True

    def count_files(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    def count_sessions(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    def list_sessions(self)->List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()

    # delete_jobs
    def add_delete_job(self, session_id:int, chat_id:int, message_ids:List[int], run_at:int) -> int:
        msg_json = json.dumps(message_ids)
        self.cur.execute("INSERT INTO delete_jobs (session_id, chat_id, message_ids, run_at) VALUES (?, ?, ?, ?)", (session_id, chat_id, msg_json, run_at))
        jid = self.cur.lastrowid
        self.conn.commit()
        return jid

    def list_pending_delete_jobs(self)->List[sqlite3.Row]:
        now = int(time.time())
        return self.cur.execute("SELECT * FROM delete_jobs WHERE run_at >= ?", (now,)).fetchall()

    def list_due_delete_jobs(self)->List[sqlite3.Row]:
        now = int(time.time())
        return self.cur.execute("SELECT * FROM delete_jobs WHERE run_at < ?", (now,)).fetchall()

    def remove_delete_job(self, jid:int):
        self.cur.execute("DELETE FROM delete_jobs WHERE id=?", (jid,))
        self.conn.commit()

    # settings (channels configuration etc)
    def set_setting(self, key:str, value:str):
        self.cur.execute("INSERT INTO settings (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.conn.commit()

    def get_setting(self, key:str) -> Optional[str]:
        row = self.cur.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass

db = Database(DB_PATH)

# -------------------
# Helpers and utils
# -------------------
def is_owner(uid:int)->bool:
    return int(uid) == int(OWNER_ID)

def extract_media_info(msg:types.Message) -> Tuple[str, str, str]:
    # returns (type, file_id_or_text, caption)
    if msg.photo:
        return "photo", msg.photo[-1].file_id, msg.caption or ""
    if msg.video:
        return "video", msg.video.file_id, msg.caption or ""
    if msg.document:
        return "document", msg.document.file_id, msg.caption or ""
    if msg.audio:
        return "audio", msg.audio.file_id, msg.caption or ""
    if msg.voice:
        return "voice", msg.voice.file_id, msg.caption or ""
    if msg.sticker:
        return "sticker", msg.sticker.file_id, ""
    # text fallback
    return "text", msg.text or msg.caption or "", msg.text or msg.caption or ""

async def send_start_or_help(which:str, chat_id:int, user:types.User=None):
    content, file_id = db.get_message(which)
    # substitute placeholders
    uname = user.username if user and user.username else (user.first_name if user and user.first_name else "there")
    rendered = content.replace("{username}", uname).replace("{first_name}", user.first_name if user and user.first_name else uname)
    try:
        if file_id and which=="start":
            await bot.send_photo(chat_id, file_id, caption=rendered)
        elif file_id and which=="help":
            await bot.send_photo(chat_id, file_id, caption=rendered)
        else:
            await bot.send_message(chat_id, rendered)
    except Exception:
        try:
            await bot.send_message(chat_id, rendered)
        except Exception:
            pass

# -------------------
# DB backup/restore on DB_CHANNEL_ID
# -------------------
async def backup_db(pin:bool=True)->Optional[int]:
    if not os.path.exists(DB_PATH):
        logger.warning("No DB file to backup at %s", DB_PATH)
        return None
    try:
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(DB_CHANNEL_ID, f, caption=f"DB backup {datetime.utcnow().isoformat()}Z")
        if pin:
            try:
                await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Pinning DB backup failed")
        logger.info("DB backup uploaded to DB channel as message %s", sent.message_id)
        return sent.message_id
    except Exception:
        logger.exception("DB backup failed")
        return None

async def restore_db_from_pinned()->bool:
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
    except Exception:
        logger.exception("Cannot get DB channel")
        return False
    pinned = getattr(chat, "pinned_message", None)
    if not pinned:
        logger.info("No pinned message in DB channel")
        return False
    doc = getattr(pinned, "document", None)
    if not doc:
        logger.info("Pinned message has no document")
        return False
    try:
        file = await bot.get_file(doc.file_id)
        tmp = DB_PATH + ".restore"
        await file.download(destination=tmp)
        if os.path.exists(DB_PATH):
            try:
                os.replace(DB_PATH, DB_PATH + ".bak")
            except Exception:
                pass
        os.replace(tmp, DB_PATH)
        logger.info("Restored DB from pinned backup")
        return True
    except Exception:
        logger.exception("Failed to restore DB from pinned")
        return False

# -------------------
# Persistent delete scheduling
# -------------------
def _delete_job_runner(chat_id:int, message_ids:List[int], db_job_id:int):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    asyncio.run_coroutine_threadsafe(_delete_messages_async(chat_id, message_ids, db_job_id), loop)

async def _delete_messages_async(chat_id:int, message_ids:List[int], db_job_id:int):
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            logger.debug("Could not delete message %s in %s", mid, chat_id)
    try:
        db.remove_delete_job(db_job_id)
    except Exception:
        logger.exception("Failed to remove delete job id %s", db_job_id)

def schedule_delete_persistent(chat_id:int, message_ids:List[int], seconds:int, session_id:int=None)->Optional[int]:
    if seconds <= 0 or not message_ids:
        return None
    run_at = int((datetime.utcnow() + timedelta(seconds=seconds)).timestamp())
    jid = db.add_delete_job(session_id or 0, chat_id, message_ids, run_at)
    run_date = datetime.utcfromtimestamp(run_at)
    scheduler.add_job(_delete_job_runner, "date", run_date=run_date, args=[chat_id, message_ids, jid])
    logger.info("Scheduled delete job %s at %s for chat %s", jid, run_date.isoformat(), chat_id)
    return jid

async def restore_delete_jobs():
    try:
        due = db.list_due_delete_jobs()
    except Exception:
        logger.exception("Failed reading due delete jobs")
        due = []
    for row in due:
        try:
            mids = json.loads(row["message_ids"])
            await _delete_messages_async(row["chat_id"], mids, row["id"])
            logger.info("Executed overdue delete job id %s", row["id"])
        except Exception:
            logger.exception("Error executing overdue job %s", row["id"])
    pending = db.list_pending_delete_jobs()
    for row in pending:
        try:
            mids = json.loads(row["message_ids"])
            run_at = datetime.utcfromtimestamp(row["run_at"])
            if run_at > datetime.utcnow():
                scheduler.add_job(_delete_job_runner, "date", run_date=run_at, args=[row["chat_id"], mids, row["id"]])
                logger.info("Restored scheduled delete job id %s at %s", row["id"], run_at.isoformat())
        except Exception:
            logger.exception("Failed to restore pending job %s", row["id"])

# -------------------
# Health app
# -------------------
async def health(request):
    return web.Response(text="ok")
health_app = web.Application()
health_app.router.add_get("/health", health)

# -------------------
# Upload state (owner-only)
# -------------------
class UploadStates(StatesGroup):
    waiting_files = State()
    choosing_protect = State()
    choosing_timer = State()

# upload_sessions structure: owner_id -> {"items":[{"chat":id,"msg_id":id}], "protect":None, "auto_delete":None, "exclude_text":False}
upload_sessions: Dict[int, Dict[str, Any]] = {}

# -------------------
# Channel helpers
# -------------------
def parse_int_list_setting(value:str)->List[int]:
    try:
        arr = json.loads(value)
        return [int(x) for x in arr]
    except Exception:
        parts = value.split(",") if value else []
        out = []
        for p in parts:
            try:
                out.append(int(p.strip()))
            except Exception:
                continue
        return out

def settings_get_channels(key:str, max_count:int)->List[int]:
    val = db.get_setting(key)
    if not val:
        return []
    lst = parse_int_list_setting(val)
    return lst[:max_count]

def settings_set_channels(key:str, channels:List[int]):
    db.set_setting(key, json.dumps(channels))

# -------------------
# Force channel check & inline buttons
# -------------------
async def make_join_buttons(user_id:int)->InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    forced = settings_get_channels("force_channels", 3)
    optional = settings_get_channels("channels", 4)
    # show forced channels first
    for cid in forced:
        try:
            chat = await bot.get_chat(cid)
            title = chat.title or str(cid)
        except Exception:
            title = str(cid)
        kb.add(InlineKeyboardButton(f"Join {title}", url=f"https://t.me/{(await bot.get_chat(cid)).username}" if getattr((await bot.get_chat(cid)),"username",None) else f"https://t.me/c/{str(cid)[4:]}" ))
    # optional channels
    for cid in optional:
        try:
            chat = await bot.get_chat(cid)
            title = chat.title or str(cid)
        except Exception:
            title = str(cid)
        kb.add(InlineKeyboardButton(f"Join {title}", url=f"https://t.me/{(await bot.get_chat(cid)).username}" if getattr((await bot.get_chat(cid)),"username",None) else f"https://t.me/c/{str(cid)[4:]}" ))
    # retry
    kb.add(InlineKeyboardButton("Retry", callback_data="retry_join"))
    return kb

async def user_in_forced_channels(user_id:int)->bool:
    forced = settings_get_channels("force_channels", 3)
    for cid in forced:
        try:
            member = await bot.get_chat_member(cid, user_id)
            if member.status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True

# -------------------
# Handlers: /start (universal) with inline help/join buttons
# -------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message:types.Message):
    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    args = message.get_args()
    # build start keyboard: help button + join buttons if any channels set
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Help", callback_data="open_help"))
    forced = settings_get_channels("force_channels", 3)
    optional = settings_get_channels("channels", 4)
    if forced or optional:
        # add join buttons (we will create dynamic urls)
        try:
            jb = await make_join_buttons(message.from_user.id)
            # attach join/retry below help
            # flatten: first help then join rows: here we will send help + separate message with join kb
            pass
        except Exception:
            pass

    if not args:
        # send start message with possible inline keyboard
        content, file_id = db.get_message("start")
        # send start message with help button and if channels present include join/retry as separate message
        await send_start_or_help("start", message.chat.id, message.from_user)
        if forced or optional:
            try:
                kb2 = await make_join_buttons(message.from_user.id)
                await message.reply("Join channels if required:", reply_markup=kb2)
            except Exception:
                pass
        return

    # deep link flow
    # decode payload or accept raw numeric
    try:
        payload = decode_payload(args)
        sid = int(payload)
    except Exception:
        try:
            sid = int(args)
        except Exception:
            await message.reply("Invalid link.", parse_mode=None)
            return

    session = db.get_session(sid)
    if not session:
        await message.reply("Session not found.", parse_mode=None)
        return
    if db.is_revoked(sid):
        await message.reply("This session has been revoked by the owner.", parse_mode=None)
        return

    # check forced membership
    if not await user_in_forced_channels(message.from_user.id):
        jb = await make_join_buttons(message.from_user.id)
        await message.reply("You must join the required channels before accessing this link. After joining, press Retry.", reply_markup=jb)
        return

    # deliver files
    files = db.get_files(sid)
    if not files:
        await message.reply("No files in this session.", parse_mode=None)
        return

    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    db.touch_user(message.from_user.id)

    protect_flag = bool(session["protect"]) and (not is_owner(message.from_user.id))
    auto_delete_seconds = int(session["auto_delete"]) if session["auto_delete"] else 0

    delivered_ids: List[int] = []
    for f in files:
        ftype = f["file_type"]
        fid = f["file_id"]
        caption = f["caption"] or ""
        try:
            if ftype == "photo":
                m = await bot.send_photo(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "video":
                m = await bot.send_video(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "document":
                m = await bot.send_document(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "audio":
                m = await bot.send_audio(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "voice":
                m = await bot.send_voice(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "sticker":
                m = await bot.send_sticker(message.chat.id, fid)
            elif ftype == "text":
                m = await bot.send_message(message.chat.id, fid)
            else:
                m = await bot.send_message(message.chat.id, fid)
            if m:
                delivered_ids.append(m.message_id)
        except Exception:
            logger.exception("Failed to deliver file in session %s to user %s", sid, message.from_user.id)
    if auto_delete_seconds and delivered_ids:
        schedule_delete_persistent(message.chat.id, delivered_ids, auto_delete_seconds, sid)

# -------------------
# Callback: open help and retry join
# -------------------
@dp.callback_query_handler(lambda c: c.data == "open_help")
async def cb_open_help(cb:types.CallbackQuery):
    await send_start_or_help("help", cb.message.chat.id, cb.from_user)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "retry_join")
async def cb_retry_join(cb:types.CallbackQuery):
    # try re-running the deep link if present in start args: difficult from callback; just advise user to re-click link
    await cb.answer("If you have joined the channels, please click your link again.", show_alert=True)

# -------------------
# Admin: /adminp (owner-only)
# -------------------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    text = (
        "Owner commands (admin panel):\n"
        "/setmessage - Reply to a message or use args '<start|help> Your text with {username} or {first_name>'\n"
        "/setimage - Reply to an image then choose where (start/help)\n"
        "/setchannel - Set optional channels (up to 4). Usage: /setchannel <cid1,cid2,..> or reply to a msg containing channel ids\n"
        "/setforcechannel - Set forced channels (up to 3). Usage similar to /setchannel\n"
        "/upload - start upload\n"
        "/d - finish upload\n"
        "/e - cancel upload\n"
        "/broadcast - reply to a message to broadcast\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/backup_db - upload & pin DB to DB channel\n"
        "/restore_db - restore DB from pinned backup\n        "
    )
    await message.reply(text, parse_mode=None)

# -------------------
# Admin: setmessage (edit start/help text)
# -------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if message.reply_to_message:
        # use reply's text or caption
        reply = message.reply_to_message
        content = reply.caption or reply.text or ""
        if not content:
            await message.reply("Reply must contain text/caption.", parse_mode=None)
            return
        # if args contains which type prefer that
        if args.startswith("start"):
            which = "start"
        elif args.startswith("help"):
            which = "help"
        else:
            # ask which to set via inline
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Set START", callback_data=f"setmsg|start|{content}"))
            kb.add(InlineKeyboardButton("Set HELP", callback_data=f"setmsg|help|{content}"))
            await message.reply("Choose where to set the replied message:", reply_markup=kb)
            return
        db.set_message(which, content, None)
        await message.reply(f"{which} message updated.", parse_mode=None)
        return
    # else args mode: /setmessage start Hello {username}
    if not args:
        await message.reply("Usage: /setmessage <start|help> Your text (use {username} or {first_name})", parse_mode=None)
        return
    parts = args.split(None, 1)
    if len(parts) < 2:
        await message.reply("Provide both which and text.", parse_mode=None)
        return
    which = parts[0].lower()
    if which not in ("start","help"):
        await message.reply("Which must be 'start' or 'help'", parse_mode=None)
        return
    content = parts[1]
    db.set_message(which, content, None)
    await message.reply(f"{which} message updated.", parse_mode=None)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg|"))
async def cb_setmsg(cb:types.CallbackQuery):
    try:
        _, which, raw = cb.data.split("|",2)
        db.set_message(which, raw, None)
        await cb.message.edit_text(f"{which} message updated.")
    except Exception:
        await cb.answer("Failed to set message.")

# -------------------
# Admin: setimage (set image for start/help)
# flow: /setimage -> owner replies to an image with /setimage? Actually follow: owner sends /setimage, bot asks to reply to image; then owner replies to image with /setimage action
# We'll implement: owner sends /setimage, bot asks "Reply to an image with /setimage start|help" -> owner replies to image with caption "start" or "help" or uses inline after replying.
# Simpler: owner replies to an image and runs /setimage start or /setimage help.
# -------------------
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip().lower()
    if message.reply_to_message and message.reply_to_message.photo:
        # owner replied to a photo and wants to set it
        if args not in ("start","help"):
            await message.reply("Reply to a photo and use: /setimage start OR /setimage help", parse_mode=None)
            return
        file_id = message.reply_to_message.photo[-1].file_id
        # set file id for the chosen type (store in start_help table's file_id)
        content, _ = db.get_message(args)
        db.set_message(args, content, file_id)
        await message.reply(f"Image set for {args}.", parse_mode=None)
        return
    # else no reply
    await message.reply("Reply to an image with /setimage start OR /setimage help to set image for start/help messages.", parse_mode=None)

# -------------------
# Admin: setchannel and setforcechannel
# - setchannel: up to 4 optional channels (non-forced)
# - setforcechannel: up to 3 forced channels (user must join to access deep links)
# Usage: /setchannel <cid1,cid2,...> OR reply to a message containing channel ids OR send "none" to clear
# -------------------
@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args and message.reply_to_message:
        args = message.reply_to_message.text.strip()
    if not args:
        await message.reply("Usage: /setchannel <cid1,cid2,...> (use 'none' to clear). Up to 4 channels.", parse_mode=None)
        return
    if args.lower() == "none":
        settings_set_channels("channels", [])
        await message.reply("Optional channels cleared.", parse_mode=None)
        return
    parts = [p.strip() for p in args.split(",") if p.strip()]
    cids = []
    for p in parts:
        try:
            cids.append(int(p))
        except Exception:
            continue
    cids = cids[:4]
    settings_set_channels("channels", cids)
    await message.reply(f"Optional channels set: {cids}", parse_mode=None)

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args and message.reply_to_message:
        args = message.reply_to_message.text.strip()
    if not args:
        await message.reply("Usage: /setforcechannel <cid1,cid2,...> (use 'none' to clear). Up to 3 channels.", parse_mode=None)
        return
    if args.lower() == "none":
        settings_set_channels("force_channels", [])
        await message.reply("Forced channels cleared.", parse_mode=None)
        return
    parts = [p.strip() for p in args.split(",") if p.strip()]
    cids = []
    for p in parts:
        try:
            cids.append(int(p))
        except Exception:
            continue
    cids = cids[:3]
    settings_set_channels("force_channels", cids)
    await message.reply(f"Forced channels set: {cids}", parse_mode=None)

# -------------------
# Admin: backup/restore DB
# -------------------
@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    sent = await backup_db(pin=True)
    if sent:
        await message.reply("DB backup uploaded & pinned.", parse_mode=None)
    else:
        await message.reply("DB backup failed.", parse_mode=None)

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    ok = await restore_db_from_pinned()
    if ok:
        # reinitialize DB instance
        try:
            db.close()
        except Exception:
            pass
        globals()['db'] = Database(DB_PATH)
        await message.reply("DB restored from pinned backup. Bot reloaded DB.", parse_mode=None)
    else:
        await message.reply("Restore failed or no pinned backup.", parse_mode=None)

# -------------------
# Upload flow fixed: /upload, collect messages, /d finalize, /e cancel
# -------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args or "no_text" in args:
        exclude_text = True
    upload_sessions[message.from_user.id] = {"items": [], "protect": None, "auto_delete": None, "exclude_text": exclude_text}
    await UploadStates.waiting_files.set()
    await message.reply("Upload session started. Send files (photo/video/document) and/or texts (if allowed). Use /d to finish or /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"], state=UploadStates.waiting_files)
async def cmd_cancel(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    upload_sessions.pop(message.from_user.id, None)
    await state.finish()
    await message.reply("Upload cancelled.", parse_mode=None)

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_files)
async def handler_collect(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    # ignore commands
    if message.text and message.text.strip().startswith("/"):
        await message.reply("Command ignored during upload. Continue sending files or /d to finish.", parse_mode=None)
        return
    owner = message.from_user.id
    sess = upload_sessions.get(owner)
    if not sess:
        return
    # exclude pure text if set
    if sess.get("exclude_text") and (message.text and not (message.photo or message.document or message.video or message.audio or message.voice)):
        await message.reply("Plain text excluded for this session. Use photo with caption to include text.", parse_mode=None)
        return
    sess["items"].append({"from_chat": message.chat.id, "msg_id": message.message_id})
    await message.reply("Added to session.", parse_mode=None)

@dp.message_handler(commands=["d"], state=UploadStates.waiting_files)
async def cmd_finish_prompt(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    owner = message.from_user.id
    sess = upload_sessions.get(owner)
    if not sess or not sess.get("items"):
        await message.reply("No items collected. Use /upload then send files.", parse_mode=None)
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Protect ON", callback_data="protect_on"), InlineKeyboardButton("Protect OFF", callback_data="protect_off"))
    await message.reply("Protect content? (prevents forwarding/downloading for non-owner). Choose.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("protect_"))
async def cb_protect(cb:types.CallbackQuery):
    owner = cb.from_user.id
    if owner not in upload_sessions:
        await cb.answer("No active upload session.", show_alert=True)
        return
    upload_sessions[owner]["protect"] = 1 if cb.data=="protect_on" else 0
    await UploadStates.choosing_timer.set()
    await cb.message.edit_text("Enter auto-delete timer in hours (0 for none). Range 0 - 168. Example: 10")

@dp.message_handler(lambda m: m.from_user.id in upload_sessions and upload_sessions[m.from_user.id].get("protect") is not None, state=UploadStates.choosing_timer)
async def handler_timer(message:types.Message, state:FSMContext):
    owner = message.from_user.id
    if owner not in upload_sessions:
        await message.reply("No active session.", parse_mode=None)
        await state.finish()
        return
    try:
        hours = float(message.text.strip())
    except Exception:
        await message.reply("Invalid number. Send hours (0-168).", parse_mode=None)
        return
    if hours < 0 or hours > 168:
        await message.reply("Hours out of range.", parse_mode=None)
        return
    seconds = int(hours*3600)
    sess = upload_sessions[owner]
    protect_flag = int(sess.get("protect", 0))
    items = sess.get("items", [])

    # create placeholders in UPLOAD_CHANNEL
    try:
        header = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing session...")
    except Exception:
        logger.exception("Failed to write header to upload channel")
        await message.reply("Failed to write to upload channel.", parse_mode=None)
        await state.finish()
        return
    try:
        link_ph = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing link...")
    except Exception:
        logger.exception("Failed to write link placeholder")
        await message.reply("Failed to write to upload channel.", parse_mode=None)
        await state.finish()
        return

    copied = []
    pos = 0
    for it in items:
        try:
            cp = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=it["from_chat"], message_id=it["msg_id"])
            ftype, fid, caption = extract_media_info(cp)
            copied.append({"vault_msg_id":cp.message_id, "file_id":fid, "file_type":ftype, "caption":caption or "", "position":pos})
            pos += 1
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("Failed to copy message %s", it)
            continue

    try:
        sid = db.create_session(owner, protect_flag, seconds, header.message_id, link_ph.message_id)
    except Exception:
        logger.exception("Failed to create session record")
        await message.reply("Failed to create session record.", parse_mode=None)
        await state.finish()
        return

    try:
        await bot.edit_message_text(f"📦 Session {sid}", UPLOAD_CHANNEL_ID, header.message_id)
    except Exception:
        logger.exception("Could not edit header")

    for ci in copied:
        try:
            db.add_file(sid, ci["vault_msg_id"], ci["file_id"], ci["file_type"], ci["caption"], ci["position"])
        except Exception:
            logger.exception("Failed to insert file metadata")

    try:
        start_link = await get_start_link(str(sid), encode=True)
    except Exception:
        me = await bot.get_me()
        start_link = f"https://t.me/{me.username}?start={sid}"

    try:
        await bot.edit_message_text(f"🔗 Files saved in Session {sid}: {start_link}", UPLOAD_CHANNEL_ID, link_ph.message_id)
    except Exception:
        logger.exception("Failed to edit link placeholder")

    await message.reply(f"✅ Session {sid} created!\n{start_link}", parse_mode=None)

    # backup DB to DB channel and pin
    try:
        await backup_db(pin=True)
    except Exception:
        logger.exception("DB backup after session failed")

    # clear state
    upload_sessions.pop(owner, None)
    await state.finish()

# -------------------
# Broadcast / stats / list / revoke
# -------------------
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message to broadcast.", parse_mode=None)
        return
    users = db.get_all_users()
    if not users:
        await message.reply("No users to broadcast.", parse_mode=None)
        return
    await message.reply(f"Broadcasting to {len(users)} users...", parse_mode=None)
    sent = 0
    failed = 0
    async def send_to(uid:int):
        nonlocal sent, failed
        async with asyncio.Semaphore(BROADCAST_CONCURRENCY):
            try:
                await message.reply_to_message.copy_to(uid)
                sent += 1
            except (BotBlocked, ChatNotFound):
                failed += 1
            except RetryAfter as e:
                await asyncio.sleep(e.timeout + 0.5)
                try:
                    await message.reply_to_message.copy_to(uid)
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
    tasks = [asyncio.create_task(send_to(uid)) for uid in users]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast finished. Sent: {sent}, Failed: {failed}", parse_mode=None)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    total_users = db.count_users()
    active = db.count_active_2days()
    total_files = db.count_files()
    total_sessions = db.count_sessions()
    await message.reply(f"Users active (2d): {active}\nTotal users: {total_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}", parse_mode=None)

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    rows = db.list_sessions()
    if not rows:
        await message.reply("No sessions found.", parse_mode=None)
        return
    out = []
    for r in rows:
        created = datetime.utcfromtimestamp(r["created_at"]).isoformat() + "Z"
        out.append(f"ID:{r['id']} owner:{r['owner_id']} created:{created} protect:{r['protect']} auto_delete:{r['auto_delete']} revoked:{r['revoked']}")
    # send in chunks
    chunk=""
    for line in out:
        if len(chunk)+len(line)+1 > 3500:
            await message.reply(chunk, parse_mode=None)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await message.reply(chunk, parse_mode=None)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <session_id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid session id.", parse_mode=None)
        return
    if not db.get_session(sid):
        await message.reply("Session not found.", parse_mode=None)
        return
    db.revoke_session(sid)
    await message.reply(f"Session {sid} revoked.", parse_mode=None)

# -------------------
# Fallback collector for owner (outside FSM)
# -------------------
@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback(message:types.Message):
    # general capturing for users - we only log active users
    if message.from_user:
        db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    # owner fallback collection
    if message.from_user.id != OWNER_ID:
        return
    sess = upload_sessions.get(message.from_user.id)
    if not sess:
        return
    if message.text and message.text.strip().startswith("/"):
        return
    if sess.get("exclude_text") and (message.text and not (message.photo or message.document or message.video or message.audio or message.voice)):
        return
    sess["items"].append({"from_chat": message.chat.id, "msg_id": message.message_id})

# -------------------
# Startup / Shutdown
# -------------------
async def on_startup(dp:Dispatcher):
    # restore DB from pinned on DB channel
    try:
        restored = await restore_db_from_pinned()
        if restored:
            try:
                db.close()
            except Exception:
                pass
            globals()['db'] = Database(DB_PATH)
    except Exception:
        logger.exception("DB restore failed at startup")
    # restore delete jobs
    try:
        await restore_delete_jobs()
    except Exception:
        logger.exception("Restore delete jobs failed")
    # start health app
    try:
        runner = web.AppRunner(health_app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        logger.info("Health endpoint listening on 0.0.0.0:%s/health", PORT)
    except Exception:
        logger.exception("Failed to start health endpoint")
    logger.info("Bot started. Owner=%s UploadChannel=%s DBChannel=%s DB=%s", OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID, DB_PATH)

async def on_shutdown(dp:Dispatcher):
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
    except Exception:
        pass
    try:
        db.close()
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass

# -------------------
# Run
# -------------------
if __name__ == "__main__":
    from aiogram import executor
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit received")
    except Exception:
        logger.exception("Fatal")

# -------------------
# requirements.txt content
# -------------------
"""
aiogram==2.25.1
APScheduler==3.10.4
aiohttp==3.8.6
SQLAlchemy==2.0.23
"""

# -------------------
# Dockerfile content
# -------------------
"""
FROM python:3.11-slim
WORKDIR /app
COPY bot.py /app/bot.py
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV PORT=10000
CMD ["python", "bot.py"]
"""
