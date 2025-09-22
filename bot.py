#!/usr/bin/env python3
# bot.py - Full Telegram Upload Bot (900+ lines)
# Two channels: UPLOAD_CHANNEL_ID (sessions/files), DB_CHANNEL_ID (DB backups & job metadata)
# Minimal inline comments. Make sure env vars are set.

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

# -------- env/config ----------
def required_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"ERROR: {name} required", file=sys.stderr)
        sys.exit(1)
    return v

BOT_TOKEN = required_env("BOT_TOKEN")
try:
    OWNER_ID = int(required_env("OWNER_ID"))
except Exception:
    print("OWNER_ID must be integer", file=sys.stderr)
    sys.exit(1)

try:
    UPLOAD_CHANNEL_ID = int(required_env("UPLOAD_CHANNEL_ID"))
except Exception:
    print("UPLOAD_CHANNEL_ID must be integer", file=sys.stderr)
    sys.exit(1)

try:
    DB_CHANNEL_ID = int(required_env("DB_CHANNEL_ID"))
except Exception:
    print("DB_CHANNEL_ID must be integer", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

# ---------- logging -----------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s")
logger = logging.getLogger("upload-bot")

# ---------- bot/dispatcher -----
bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(bot, storage=MemoryStorage())

# ---------- jobstore ----------
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
jobstore_url = f"sqlite:///{os.path.abspath(JOB_DB_PATH)}"
jobstores = {'default': SQLAlchemyJobStore(url=jobstore_url)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.start()

# ---------- SQLite wrapper ----
class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._init_tables()

    def _init_tables(self):
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
            type TEXT UNIQUE,
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

    # users
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

    def get_all_users(self)->List[int]:
        rows = self.cur.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]

    def count_users(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def count_active_2days(self)->int:
        cutoff = int(time.time()) - 2*86400
        return self.cur.execute("SELECT COUNT(*) FROM users WHERE last_active>=?", (cutoff,)).fetchone()[0]

    # start/help
    def set_message(self, which:str, content:str, file_id:Optional[str]=None):
        self.cur.execute("INSERT INTO start_help (type, content, file_id) VALUES (?, ?, ?) ON CONFLICT(type) DO UPDATE SET content=excluded.content, file_id=excluded.file_id", (which, content, file_id))
        self.conn.commit()

    def get_message(self, which:str)->Tuple[str, Optional[str]]:
        row = self.cur.execute("SELECT content, file_id FROM start_help WHERE type=?", (which,)).fetchone()
        if row:
            return row["content"], row["file_id"]
        default = ("Welcome, {username}!", None) if which=="start" else ("Use /start or a session link. Owner: use /adminp", None)
        return default

    # sessions/files
    def create_session(self, owner_id:int, protect:int, auto_delete:int, header_vault_msg_id:int, link_vault_msg_id:int)->int:
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

    def get_session(self, session_id:int):
        return self.cur.execute("SELECT * FROM sessions WHERE id=? LIMIT 1", (session_id,)).fetchone()

    def get_files(self, session_id:int)->List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY position ASC", (session_id,)).fetchall()

    def revoke_session(self, session_id:int):
        self.cur.execute("UPDATE sessions SET revoked=1 WHERE id=?", (session_id,))
        self.conn.commit()

    def is_revoked(self, session_id:int)->bool:
        row = self.cur.execute("SELECT revoked FROM sessions WHERE id=?", (session_id,)).fetchone()
        return bool(row["revoked"]) if row else True

    def count_files(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    def count_sessions(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    def list_sessions(self)->List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()

    # delete jobs
    def add_delete_job(self, session_id:int, chat_id:int, message_ids:List[int], run_at:int)->int:
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

    # settings
    def set_setting(self, key:str, value:str):
        self.cur.execute("INSERT INTO settings (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.conn.commit()

    def get_setting(self, key:str)->Optional[str]:
        row = self.cur.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass

db = Database(DB_PATH)

# ---------- helper utils -----------
def is_owner(uid:int)->bool:
    return int(uid) == int(OWNER_ID)

def extract_media_info(msg:types.Message)->Tuple[str,str,str]:
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
    return "text", msg.text or msg.caption or "", msg.text or msg.caption or ""

async def send_start_message(user:types.User, chat_id:int):
    content, file_id = db.get_message("start")
    username = user.username or user.first_name or "there"
    rendered = content.replace("{username}", username).replace("{first_name}", user.first_name or username)
    try:
        if file_id:
            await bot.send_photo(chat_id, file_id, caption=rendered)
        else:
            await bot.send_message(chat_id, rendered)
    except Exception:
        try:
            await bot.send_message(chat_id, rendered)
        except Exception:
            pass

async def send_help_message(user:types.User, chat_id:int):
    content, file_id = db.get_message("help")
    username = user.username or user.first_name or "there"
    rendered = content.replace("{username}", username).replace("{first_name}", user.first_name or username)
    try:
        if file_id:
            await bot.send_photo(chat_id, file_id, caption=rendered)
        else:
            await bot.send_message(chat_id, rendered)
    except Exception:
        try:
            await bot.send_message(chat_id, rendered)
        except Exception:
            pass

# ---------- DB backup/restore ----------
async def backup_db_to_channel(pin:bool=True)->Optional[int]:
    if not os.path.exists(DB_PATH):
        logger.warning("DB file not found to backup")
        return None
    try:
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(DB_CHANNEL_ID, f, caption=f"DB backup {datetime.utcnow().isoformat()}Z")
        if pin:
            try:
                await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Pin DB message failed")
        logger.info("DB backup uploaded to DB channel as message id %s", sent.message_id)
        return sent.message_id
    except ChatNotFound:
        logger.error("Cannot get DB channel - ChatNotFound. Ensure bot is added to DB channel and ID correct.")
        return None
    except Exception:
        logger.exception("Failed to backup DB to channel")
        return None

async def restore_db_from_pinned()->bool:
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
    except ChatNotFound:
        logger.error("Cannot get DB channel: ChatNotFound")
        return False
    except Exception:
        logger.exception("Failed to access DB channel")
        return False
    pinned = getattr(chat, "pinned_message", None)
    if not pinned:
        logger.info("No pinned DB backup in DB channel")
        return False
    doc = getattr(pinned, "document", None)
    if not doc:
        logger.info("Pinned message has no document")
        return False
    try:
        file = await bot.get_file(doc.file_id)
        dest = DB_PATH + ".restore"
        await file.download(destination=dest)
        if os.path.exists(DB_PATH):
            try:
                os.replace(DB_PATH, DB_PATH + ".bak")
            except Exception:
                pass
        os.replace(dest, DB_PATH)
        logger.info("DB restored from pinned backup")
        return True
    except Exception:
        logger.exception("Failed to download/replace DB from pinned backup")
        return False

# ---------- delete job scheduling ----------
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
            logger.debug("Could not delete message %s in chat %s", mid, chat_id)
    try:
        db.remove_delete_job(db_job_id)
    except Exception:
        logger.exception("Failed to remove delete job %s", db_job_id)

def schedule_persistent_delete(chat_id:int, message_ids:List[int], seconds:int, session_id:int=None)->Optional[int]:
    if seconds <= 0 or not message_ids:
        return None
    run_at = int((datetime.utcnow() + timedelta(seconds=seconds)).timestamp())
    jid = db.add_delete_job(session_id or 0, chat_id, message_ids, run_at)
    run_dt = datetime.utcfromtimestamp(run_at)
    scheduler.add_job(_delete_job_runner, "date", run_date=run_dt, args=[chat_id, message_ids, jid])
    logger.info("Scheduled persistent delete job %s at %s", jid, run_dt.isoformat())
    return jid

async def restore_delete_jobs_on_startup():
    try:
        due = db.list_due_delete_jobs()
    except Exception:
        due = []
    for row in due:
        try:
            mids = json.loads(row["message_ids"])
            await _delete_messages_async(row["chat_id"], mids, row["id"])
            logger.info("Executed overdue delete job id %s", row["id"])
        except Exception:
            logger.exception("Failed executing overdue job %s", row["id"])
    pending = db.list_pending_delete_jobs()
    for row in pending:
        try:
            mids = json.loads(row["message_ids"])
            run_dt = datetime.utcfromtimestamp(row["run_at"])
            if run_dt > datetime.utcnow():
                scheduler.add_job(_delete_job_runner, "date", run_date=run_dt, args=[row["chat_id"], mids, row["id"]])
                logger.info("Restored delete job %s scheduled for %s", row["id"], run_dt.isoformat())
        except Exception:
            logger.exception("Failed to restore job %s", row["id"])

# ---------- health app ----------
async def health(request):
    return web.Response(text="ok")
health_app = web.Application()
health_app.router.add_get("/health", health)

# ---------- upload FSM ----------
class UploadStates(StatesGroup):
    waiting_files = State()
    choosing_protect = State()
    choosing_timer = State()

upload_sessions: Dict[int, Dict[str, Any]] = {}

# ---------- channel settings helpers ----------
def parse_channel_arg(arg:str)->Optional[int]:
    arg = arg.strip()
    if not arg:
        return None
    # numeric id?
    try:
        if arg.startswith("-"):
            return int(arg)
        if arg.isdigit():
            return int(arg)
    except Exception:
        pass
    # @username trimmed
    if arg.startswith("@"):
        try:
            chat = asyncio.get_event_loop().run_until_complete(bot.get_chat(arg))
            return int(chat.id)
        except Exception:
            # fallback: return None
            return None
    # if link like https://t.me/username
    if "t.me/" in arg:
        try:
            username = arg.split("t.me/")[-1].split("?")[0].strip().lstrip("@")
            chat = asyncio.get_event_loop().run_until_complete(bot.get_chat("@" + username))
            return int(chat.id)
        except Exception:
            return None
    return None

def settings_get_list(key:str, max_count:int)->List[int]:
    v = db.get_setting(key)
    if not v:
        return []
    try:
        arr = json.loads(v)
        arr = [int(x) for x in arr]
        return arr[:max_count]
    except Exception:
        parts = [p.strip() for p in v.split(",") if p.strip()]
        out = []
        for p in parts:
            try:
                out.append(int(p))
            except Exception:
                val = parse_channel_arg(p)
                if val:
                    out.append(val)
        return out[:max_count]

def settings_set_list(key:str, values:List[int]):
    db.set_setting(key, json.dumps(values))

# ---------- join buttons ----------
async def build_join_keyboard(user_id:int)->InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    forced = settings_get_list("force_channels", 3)
    optional = settings_get_list("channels", 4)
    # forced first
    for cid in forced:
        try:
            ch = await bot.get_chat(cid)
            title = ch.title or str(cid)
            if getattr(ch, "username", None):
                kb.add(InlineKeyboardButton(f"Join {title}", url=f"https://t.me/{ch.username}"))
            else:
                # public link unknown; try invite link (can't create), use t.me/c/<id> pattern for private numeric channels if needed
                if str(cid).startswith("-100"):
                    kb.add(InlineKeyboardButton(f"Open {title}", url=f"https://t.me/c/{str(cid)[4:]}"))
                else:
                    kb.add(InlineKeyboardButton(f"Open {title}", url=f"https://t.me/{cid}"))
        except Exception:
            kb.add(InlineKeyboardButton(f"Open {cid}", url=f"https://t.me/c/{str(cid)[4:]}" if str(cid).startswith("-100") else f"https://t.me/{cid}"))
    # optional next
    for cid in optional:
        try:
            ch = await bot.get_chat(cid)
            title = ch.title or str(cid)
            if getattr(ch, "username", None):
                kb.add(InlineKeyboardButton(f"Join {title}", url=f"https://t.me/{ch.username}"))
            else:
                if str(cid).startswith("-100"):
                    kb.add(InlineKeyboardButton(f"Open {title}", url=f"https://t.me/c/{str(cid)[4:]}"))
                else:
                    kb.add(InlineKeyboardButton(f"Open {title}", url=f"https://t.me/{cid}"))
        except Exception:
            kb.add(InlineKeyboardButton(f"Open {cid}", url=f"https://t.me/c/{str(cid)[4:]}" if str(cid).startswith("-100") else f"https://t.me/{cid}"))
    kb.add(InlineKeyboardButton("Retry", callback_data="retry_join"))
    return kb

async def user_in_forced_channels(user_id:int)->bool:
    forced = settings_get_list("force_channels", 3)
    for cid in forced:
        try:
            member = await bot.get_chat_member(cid, user_id)
            if member.status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True

# ---------- start handler ----------
@dp.message_handler(commands=["start"])
async def cmd_start(message:types.Message):
    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    args = message.get_args()
    # show start message with help and join buttons
    if not args:
        await send_start_message(message.from_user, message.chat.id)
        forced = settings_get_list("force_channels", 3)
        opt = settings_get_list("channels", 4)
        if forced or opt:
            try:
                kb = await build_join_keyboard(message.from_user.id)
                await message.reply("Channels:", reply_markup=kb)
            except Exception:
                pass
        return
    # deep link flow
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
        await message.reply("This session has been revoked by owner.", parse_mode=None)
        return
    # check forced channels membership
    if not await user_in_forced_channels(message.from_user.id):
        kb = await build_join_keyboard(message.from_user.id)
        await message.reply("Please join the required channel(s) before accessing this link. After joining, press Retry.", reply_markup=kb)
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
    delivered = []
    for f in files:
        ftype = f["file_type"]
        fid = f["file_id"]
        caption = f["caption"] or ""
        try:
            if ftype == "photo":
                msg = await bot.send_photo(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "video":
                msg = await bot.send_video(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "document":
                msg = await bot.send_document(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "audio":
                msg = await bot.send_audio(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "voice":
                msg = await bot.send_voice(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "sticker":
                msg = await bot.send_sticker(message.chat.id, fid)
            elif ftype == "text":
                msg = await bot.send_message(message.chat.id, fid)
            else:
                msg = await bot.send_message(message.chat.id, fid)
            if msg:
                delivered.append(msg.message_id)
        except Exception:
            logger.exception("Failed delivering file %s for session %s to user %s", fid, sid, message.from_user.id)
    if auto_delete_seconds and delivered:
        schedule_persistent_delete(message.chat.id, delivered, auto_delete_seconds, sid)

# ---------- callback handlers ----------
@dp.callback_query_handler(lambda c: c.data == "retry_join")
async def cb_retry_join(cb:types.CallbackQuery):
    await cb.answer("If you have joined channels, re-open the session link to retry.", show_alert=True)

# ---------- adminp ----------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    text = (
        "Admin Panel Commands:\n"
        "/setmessage - Reply to text or use: /setmessage <start|help> <text with {username}>\n"
        "/setimage - Reply to photo and use /setimage start|help\n"
        "/setchannel - /setchannel <cid1,cid2,...> or reply to text with channel ids. Use 'none' to clear\n"
        "/setforcechannel - same as setchannel but forces join (max 3)\n"
        "/upload - start upload (owner only)\n"
        "/d - finalize upload\n"
        "/e - cancel upload\n"
        "/broadcast - reply to message to broadcast to all users\n"
        "/stats - basic stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/backup_db - upload & pin DB to DB channel\n"
        "/restore_db - restore DB from pinned backup\n"
    )
    await message.reply(text, parse_mode=None)

# ---------- setmessage ----------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if message.reply_to_message:
        reply = message.reply_to_message
        content = reply.caption or reply.text or ""
        if not content:
            await message.reply("Reply must contain text/caption.", parse_mode=None)
            return
        if args.startswith("start"):
            which = "start"
        elif args.startswith("help"):
            which = "help"
        else:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Set START", callback_data=f"setmsg|start|{json.dumps(content)}"))
            kb.add(InlineKeyboardButton("Set HELP", callback_data=f"setmsg|help|{json.dumps(content)}"))
            await message.reply("Choose where to set the replied content:", reply_markup=kb)
            return
        db.set_message(which, content, None)
        await message.reply(f"{which} message updated.", parse_mode=None)
        return
    if not args:
        await message.reply("Usage: /setmessage <start|help> your text (use {username} or {first_name})", parse_mode=None)
        return
    parts = args.split(None, 1)
    if len(parts) < 2:
        await message.reply("Provide which and text.", parse_mode=None)
        return
    which = parts[0].lower()
    content = parts[1]
    if which not in ("start","help"):
        await message.reply("Which must be 'start' or 'help'.", parse_mode=None)
        return
    db.set_message(which, content, None)
    await message.reply(f"{which} message updated.", parse_mode=None)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg|"))
async def cb_setmsg(cb:types.CallbackQuery):
    try:
        _, which, raw = cb.data.split("|",2)
        content = json.loads(raw)
        db.set_message(which, content, None)
        await cb.message.edit_text(f"{which} message updated.")
    except Exception:
        await cb.answer("Failed to set message", show_alert=True)

# ---------- setimage ----------
@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip().lower()
    if message.reply_to_message and message.reply_to_message.photo:
        if args not in ("start","help"):
            await message.reply("Reply to a photo and use: /setimage start OR /setimage help", parse_mode=None)
            return
        file_id = message.reply_to_message.photo[-1].file_id
        content, _ = db.get_message(args)
        db.set_message(args, content, file_id)
        await message.reply(f"Image set for {args}.", parse_mode=None)
        return
    await message.reply("Reply to a photo with /setimage start OR /setimage help.", parse_mode=None)

# ---------- setchannel / setforcechannel ----------
@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args and message.reply_to_message:
        args = message.reply_to_message.text.strip()
    if not args:
        await message.reply("Usage: /setchannel <cid1,cid2,..> or reply to text with ids. Use 'none' to clear.", parse_mode=None)
        return
    if args.lower() == "none":
        settings_set_list = lambda k,v: db.set_setting(k,json.dumps(v))
        settings_set_list("channels", [])
        await message.reply("Optional channels cleared.", parse_mode=None)
        return
    parts = [p.strip() for p in args.split(",") if p.strip()]
    cids = []
    for p in parts:
        val = parse_channel_arg(p)
        if val:
            cids.append(val)
    cids = cids[:4]
    db.set_setting("channels", json.dumps(cids))
    await message.reply(f"Optional channels set to: {cids}", parse_mode=None)

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args and message.reply_to_message:
        args = message.reply_to_message.text.strip()
    if not args:
        await message.reply("Usage: /setforcechannel <cid1,cid2,..> or reply to text with ids. Use 'none' to clear.", parse_mode=None)
        return
    if args.lower() == "none":
        db.set_setting("force_channels", json.dumps([]))
        await message.reply("Forced channels cleared.", parse_mode=None)
        return
    parts = [p.strip() for p in args.split(",") if p.strip()]
    cids = []
    for p in parts:
        val = parse_channel_arg(p)
        if val:
            cids.append(val)
    cids = cids[:3]
    db.set_setting("force_channels", json.dumps(cids))
    await message.reply(f"Forced channels set to: {cids}", parse_mode=None)

# ---------- backup/restore db ----------
@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    mid = await backup_db_to_channel(pin=True)
    if mid:
        await message.reply("DB backup uploaded & pinned.", parse_mode=None)
    else:
        await message.reply("DB backup failed (check DB channel & bot permissions).", parse_mode=None)

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    ok = await restore_db_from_pinned()
    if ok:
        try:
            db.close()
        except Exception:
            pass
        globals()['db'] = Database(DB_PATH)
        await message.reply("DB restored from pinned backup.", parse_mode=None)
    else:
        await message.reply("Restore failed or no pinned backup.", parse_mode=None)

# ---------- upload flow (fixed) ----------
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
    await message.reply("Upload started. Send media (photo/video/document) and captions will be saved. /d to finish, /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"], state=UploadStates.waiting_files)
async def cmd_cancel_upload(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    upload_sessions.pop(message.from_user.id, None)
    await state.finish()
    await message.reply("Upload cancelled.", parse_mode=None)

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_files)
async def handler_collect(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    if message.text and message.text.strip().startswith("/"):
        await message.reply("Command ignored during upload. Continue sending media or /d to finish.", parse_mode=None)
        return
    owner = message.from_user.id
    sess = upload_sessions.get(owner)
    if not sess:
        return
    # If exclude_text and plain text message -> ignore
    if sess.get("exclude_text") and (message.text and not (message.photo or message.document or message.video or message.audio or message.voice)):
        await message.reply("Plain text excluded. Use photo with caption to include text.", parse_mode=None)
        return
    # Only collect messages that contain actual content (allow text if not excluded)
    if message.photo or message.video or message.document or message.audio or message.voice or message.sticker or message.text:
        sess["items"].append({"chat_id": message.chat.id, "message_id": message.message_id})
        await message.reply("Item added to upload session.", parse_mode=None)
    else:
        await message.reply("Unsupported content type; only media or text are supported.", parse_mode=None)

@dp.message_handler(commands=["d"], state=UploadStates.waiting_files)
async def cmd_finish(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    sess = upload_sessions.get(message.from_user.id)
    if not sess or not sess.get("items"):
        await message.reply("No items collected. Use /upload and send media first.", parse_mode=None)
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Protect ON", callback_data="protect_on"))
    kb.add(InlineKeyboardButton("Protect OFF", callback_data="protect_off"))
    await message.reply("Protect content? (prevents forwarding/downloading for non-owner). Choose.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("protect_"))
async def cb_protect_choice(cb:types.CallbackQuery):
    owner = cb.from_user.id
    if owner not in upload_sessions:
        await cb.answer("No active upload session", show_alert=True)
        return
    upload_sessions[owner]["protect"] = 1 if cb.data=="protect_on" else 0
    await UploadStates.choosing_timer.set()
    await cb.message.edit_text("Enter auto-delete timer in hours (0 for none). Range 0 - 168. Example: 10")

@dp.message_handler(lambda m: m.from_user.id in upload_sessions and upload_sessions[m.from_user.id].get("protect") is not None, state=UploadStates.choosing_timer)
async def handler_timer_input(message:types.Message, state:FSMContext):
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
    seconds = int(hours * 3600)
    info = upload_sessions[owner]
    items = info.get("items", [])
    protect = int(info.get("protect", 0))

    # create header and link placeholders in upload channel
    try:
        header = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing session...")
    except Exception:
        logger.exception("Failed to send header to upload channel")
        await message.reply("Failed to write to upload channel. Ensure bot is admin.", parse_mode=None)
        await state.finish()
        return
    try:
        link_pl = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing link...")
    except Exception:
        logger.exception("Failed to send link placeholder")
        await message.reply("Failed to write to upload channel.", parse_mode=None)
        await state.finish()
        return

    copied_meta = []
    pos = 0
    for it in items:
        try:
            cp = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=it["chat_id"], message_id=it["message_id"])
            ftype, fid, caption = extract_media_info(cp)
            copied_meta.append({"vault_msg_id": cp.message_id, "file_id": fid, "file_type": ftype, "caption": caption or "", "position": pos})
            pos += 1
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("Failed copying message %s from %s", it.get("message_id"), it.get("chat_id"))
            continue

    try:
        sid = db.create_session(owner, protect, seconds, header.message_id, link_pl.message_id)
    except Exception:
        logger.exception("Failed to create session record")
        await message.reply("Failed to create session record.", parse_mode=None)
        await state.finish()
        return

    try:
        await bot.edit_message_text(f"📦 Session {sid}", UPLOAD_CHANNEL_ID, header.message_id)
    except Exception:
        logger.exception("Failed editing header message")

    for meta in copied_meta:
        try:
            db.add_file(sid, meta["vault_msg_id"], meta["file_id"], meta["file_type"], meta["caption"], meta["position"])
        except Exception:
            logger.exception("Failed saving file metadata for session %s", sid)

    try:
        start_link = await get_start_link(str(sid), encode=True)
    except Exception:
        me = await bot.get_me()
        start_link = f"https://t.me/{me.username}?start={sid}"

    try:
        await bot.edit_message_text(f"🔗 Files saved in Session {sid}: {start_link}", UPLOAD_CHANNEL_ID, link_pl.message_id)
    except Exception:
        logger.exception("Failed editing link placeholder")

    await message.reply(f"✅ Session {sid} created!\n{start_link}", parse_mode=None)

    # backup DB after creating session
    try:
        await backup_db_to_channel(pin=True)
    except Exception:
        logger.exception("DB backup after session failed")

    # clear state
    upload_sessions.pop(owner, None)
    await state.finish()

# ---------- broadcast, stats, list, revoke ----------
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
    active2 = db.count_active_2days()
    total_files = db.count_files()
    total_sessions = db.count_sessions()
    await message.reply(f"Active (2d): {active2}\nTotal users: {total_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}", parse_mode=None)

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
        out.append(f"ID:{r['id']} owner:{r['owner_id']} created:{created} protect:{r['protect']} auto:{r['auto_delete']} revoked:{r['revoked']}")
    chunk = ""
    for line in out:
        if len(chunk) + len(line) + 1 > 3500:
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
        await message.reply("Invalid session id", parse_mode=None)
        return
    if not db.get_session(sid):
        await message.reply("Session not found", parse_mode=None)
        return
    db.revoke_session(sid)
    await message.reply(f"Session {sid} revoked", parse_mode=None)

# ---------- fallback collector & user touch ----------
@dp.message_handler(content_types=types.ContentType.ANY)
async def general_fallback(message:types.Message):
    if message.from_user:
        db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    # owner outside FSM collect
    if message.from_user.id != OWNER_ID:
        return
    sess = upload_sessions.get(message.from_user.id)
    if not sess:
        return
    if message.text and message.text.strip().startswith("/"):
        return
    if sess.get("exclude_text") and (message.text and not (message.photo or message.document or message.video or message.audio or message.voice)):
        return
    sess["items"].append({"chat_id": message.chat.id, "message_id": message.message_id})

# ---------- startup/shutdown ----------
async def on_startup(dp:Dispatcher):
    try:
        restored = await restore_db_from_pinned()
        if restored:
            try:
                db.close()
            except Exception:
                pass
            globals()['db'] = Database(DB_PATH)
    except Exception:
        logger.exception("Error restoring DB on startup")
    try:
        await restore_delete_jobs_on_startup()
    except Exception:
        logger.exception("Failed restore delete jobs")
    try:
        runner = web.AppRunner(health_app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        logger.info("Health endpoint at 0.0.0.0:%s/health", PORT)
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

# ---------- run ----------
if __name__ == "__main__":
    from aiogram import executor
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit")
    except Exception:
        logger.exception("Fatal")

# ---------- requirements & Dockerfile (create these files) ----------
"""
requirements.txt
aiogram==2.25.1
APScheduler==3.10.4
aiohttp==3.8.6
SQLAlchemy==2.0.23

Dockerfile:
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py /app/bot.py
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV PORT=10000
CMD ["python","bot.py"]
"""