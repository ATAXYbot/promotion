"""
Public Multi-User Telegram Link Management System
-------------------------------------------------
A fully automated Telegram link manager bot that allows ANY user to message it,
login to their personal Telegram account via OTP, and run their own isolated 
invite link joining background tasks.
"""

import asyncio
import logging
import os
import random
import time
import re
import json
import platform
import psutil
import socket
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from telethon.sessions import StringSession

from telethon import TelegramClient, events, Button, types
from telethon.errors import (
    FloodWaitError, UserAlreadyParticipantError, SessionPasswordNeededError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, PhoneCodeExpiredError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, SendReactionRequest, SendMessageRequest
from telethon.tl.types import BotCommand, BotCommandScopeDefault, ChatInvite, ChatInviteAlready, ReactionEmoji
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.functions.account import GetBotBusinessConnectionRequest
from telethon.tl.functions import InvokeWithBusinessConnectionRequest
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("TG_API_ID", "20543583"))  # <--- SET YOUR API_ID HERE
API_HASH = os.environ.get("TG_API_HASH", "505e57baf9b48347e18446d352cacce3")  # <--- SET YOUR API_HASH HERE
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "8925952271:AAGScKJAK1sIA7c8JXCUqmbjQm05B3WOMNo")

# Create and set a global event loop before Telethon initializes
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Ensure sessions directory exists
if not os.path.exists("sessions"):
    os.makedirs("sessions")

# MongoDB Setup
MONGO_URI = os.environ.get("MONGO_URI", "")
db_client = None
db_collection = None
spam_collection = None
GLOBAL_SPAMMERS = set()
PHYSICAL_BOOT_TIME = time.time()

if MONGO_URI:
    db_client = AsyncIOMotorClient(MONGO_URI)
    db_collection = db_client.get_database("telegram_bot").get_collection("users")
    spam_collection = db_client.get_database("telegram_bot").get_collection("spam_database")
    logger.info("MongoDB initialized for persistent storage.")
else:
    logger.warning("MONGO_URI not found. State will not survive Render deployments.")

bot_client = TelegramClient('sessions/control_bot', API_ID, API_HASH)

# ==========================================
# MULTI-USER STATE MANAGEMENT
# ==========================================
user_data = {}
STATE_FILE = "sessions/state.json"

async def load_state():
    global user_data
    loaded_from_db = False
    
    if db_collection is not None:
        try:
            cursor = db_collection.find({})
            async for doc in cursor:
                str_user_id = doc.get("user_id")
                if not str_user_id: continue
                user_id = int(str_user_id)
                user_data[user_id] = {
                    "client": None,
                    "task": None,
                    "queue": doc.get("queue", []),
                    "current_index": doc.get("current_index", 0),
                    "loop_active": doc.get("loop_active", False),
                    "daily_joins": doc.get("daily_joins", []),
                    "login_state": None,
                    "phone": doc.get("phone", None),
                    "phone_code_hash": None,
                    "next_join_time": doc.get("next_join_time", 0),
                    "first_join_done": doc.get("first_join_done", False),
                    "link_stats": doc.get("link_stats", {}),
                    "high_traffic_links": doc.get("high_traffic_links", {}),
                    "link_schedule": doc.get("link_schedule", {}),
                    "link_last_action": doc.get("link_last_action", {}),
                    "link_seen_users": doc.get("link_seen_users", {}),
                    "active_links_count": doc.get("active_links_count", 0),
                    "passive_links_count": doc.get("passive_links_count", 0),
                    "session_string": doc.get("session_string", ""),
                    "paused_links": doc.get("paused_links", []),
                    "stopped_links": doc.get("stopped_links", []),
                    "queue_page": doc.get("queue_page", 0),
                    "editing_link": doc.get("editing_link", None),
                    "link_performance": doc.get("link_performance", {}),
                    "link_active_hours": doc.get("link_active_hours", {}),
                    "link_titles": doc.get("link_titles", {}),
                    "notification_mode": doc.get("notification_mode", "ALL"),
                    "global_seen_users": doc.get("global_seen_users", {}),
                    "global_blacklist": doc.get("global_blacklist", []),
                    "flood_history": doc.get("flood_history", []),
                    "panic_mode_until": doc.get("panic_mode_until", 0),
                    "hour_activity_log": doc.get("hour_activity_log", {}),
                    "engine_uptime_start": doc.get("engine_uptime_start", time.time()),
                    "user_proxies": doc.get("user_proxies", []),
                    "hibernating_links": doc.get("hibernating_links", []),
                    "first_login_time": doc.get("first_login_time", 0),
                    "business_auto_reply": doc.get("business_auto_reply", None),
                    "business_replied_users": doc.get("business_replied_users", {}),
                    "business_keyword_replies": doc.get("business_keyword_replies", {})
                }
            loaded_from_db = True
            logger.info("State successfully loaded from MongoDB.")
            
            # Load Global Spammer Hivemind
            if spam_collection is not None:
                spam_doc = await spam_collection.find_one({"_id": "global_blacklist"})
                if spam_doc and "user_ids" in spam_doc:
                    GLOBAL_SPAMMERS.update(spam_doc["user_ids"])
                logger.info(f"Loaded {len(GLOBAL_SPAMMERS)} permanent spammers from Hivemind.")
                
        except Exception as e:
            logger.error(f"Error loading state from MongoDB: {e}")
            
    if not loaded_from_db and os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                for str_user_id, state in data.items():
                    user_id = int(str_user_id)
                    user_data[user_id] = {
                        "client": None,
                        "task": None,
                        "queue": state.get("queue", []),
                        "current_index": state.get("current_index", 0),
                        "loop_active": state.get("loop_active", False),
                        "daily_joins": state.get("daily_joins", []),
                        "login_state": state.get("login_state", None),
                        "phone": state.get("phone", None),
                        "phone_code_hash": state.get("phone_code_hash", None),
                        "next_join_time": state.get("next_join_time", 0),
                        "first_join_done": state.get("first_join_done", False),
                        "link_stats": state.get("link_stats", {}),
                        "high_traffic_links": state.get("high_traffic_links", {}),
                        "link_schedule": state.get("link_schedule", {}),
                        "link_last_action": state.get("link_last_action", {}),
                        "link_seen_users": state.get("link_seen_users", {}),
                        "active_links_count": state.get("active_links_count", 0),
                        "passive_links_count": state.get("passive_links_count", 0),
                        "session_string": state.get("session_string", ""),
                        "paused_links": state.get("paused_links", []),
                        "stopped_links": state.get("stopped_links", []),
                        "queue_page": state.get("queue_page", 0),
                        "editing_link": state.get("editing_link", None),
                        "link_performance": state.get("link_performance", {}),
                        "link_active_hours": state.get("link_active_hours", {}),
                        "link_titles": state.get("link_titles", {}),
                        "notification_mode": state.get("notification_mode", "ALL"),
                        "global_seen_users": state.get("global_seen_users", {}),
                        "global_blacklist": state.get("global_blacklist", []),
                        "flood_history": state.get("flood_history", []),
                        "panic_mode_until": state.get("panic_mode_until", 0),
                        "hour_activity_log": state.get("hour_activity_log", {}),
                        "engine_uptime_start": state.get("engine_uptime_start", time.time()),
                        "user_proxies": state.get("user_proxies", []),
                        "hibernating_links": state.get("hibernating_links", []),
                        "first_login_time": state.get("first_login_time", 0),
                        "business_auto_reply": state.get("business_auto_reply", None),
                        "business_replied_users": state.get("business_replied_users", {}),
                        "business_keyword_replies": state.get("business_keyword_replies", {})
                    }
        except Exception as e:
            logger.error(f"Error loading state from local file: {e}")

STATE_DIRTY = False

def save_state():
    global STATE_DIRTY
    STATE_DIRTY = True

def instant_save_state():
    # Bypass debouncer for critical events (like login)
    asyncio.create_task(_save_state_async())

async def _db_saver_loop():
    global STATE_DIRTY
    while True:
        await asyncio.sleep(300) # Save every 5 minutes to guarantee free bandwidth
        if STATE_DIRTY:
            await _save_state_async()
            STATE_DIRTY = False

async def _save_state_async():
    state_to_save = {}
    for user_id, state in user_data.items():
        doc = {
            "user_id": str(user_id),
            "queue": state["queue"],
            "current_index": state["current_index"],
            "loop_active": state["loop_active"],
            "daily_joins": state["daily_joins"],
            "phone": state.get("phone", None),
            "next_join_time": state.get("next_join_time", 0),
            "first_join_done": state.get("first_join_done", False),
            "link_stats": state.get("link_stats", {}),
            "high_traffic_links": state.get("high_traffic_links", {}),
            "link_schedule": state.get("link_schedule", {}),
            "link_last_action": state.get("link_last_action", {}),
            "active_links_count": state.get("active_links_count", 0),
            "passive_links_count": state.get("passive_links_count", 0),
            "session_string": state.get("session_string", ""),
            "paused_links": state.get("paused_links", []),
            "stopped_links": state.get("stopped_links", []),
            "queue_page": state.get("queue_page", 0),
            "editing_link": state.get("editing_link", None),
            "link_performance": state.get("link_performance", {}),
            "link_active_hours": state.get("link_active_hours", {}),
            "link_titles": state.get("link_titles", {}),
            "notification_mode": state.get("notification_mode", "ALL"),
            "flood_history": state.get("flood_history", []),
            "panic_mode_until": state.get("panic_mode_until", 0),
            "hour_activity_log": state.get("hour_activity_log", {}),
            "engine_uptime_start": state.get("engine_uptime_start", time.time()),
            "user_proxies": state.get("user_proxies", []),
            "hibernating_links": state.get("hibernating_links", []),
            "first_login_time": state.get("first_login_time", 0),
            "business_auto_reply": state.get("business_auto_reply", None),
            "business_replied_users": state.get("business_replied_users", {}),
            "business_keyword_replies": state.get("business_keyword_replies", {})
        }
        state_to_save[str(user_id)] = doc
        
        if db_collection is not None:
            try:
                await db_collection.update_one(
                    {"user_id": str(user_id)}, 
                    {
                        "$set": doc,
                        "$unset": {
                            "global_seen_users": "",
                            "link_seen_users": "",
                            "global_blacklist": ""
                        }
                    }, 
                    upsert=True
                )
            except Exception as e:
                logger.error(f"🚨 CRITICAL MONGODB SAVE ERROR: {e}")
                
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state_to_save, f)
    except Exception as e:
        logger.error(f"Error saving local state: {e}")

def get_user_data(user_id):
    if user_id not in user_data:
        user_data[user_id] = {
            "client": None,
            "queue": [],
            "current_index": 0,
            "loop_active": False,
            "daily_joins": [],
            "login_state": None,
            "phone": None,
            "phone_code_hash": None,
            "task": None,
            "next_join_time": 0,
            "first_join_done": False,
            "link_stats": {},
            "high_traffic_links": {},
            "link_schedule": {},
            "link_last_action": {},
            "link_seen_users": {},
            "active_links_count": 0,
            "passive_links_count": 0,
            "paused_links": [],
            "stopped_links": [],
            "queue_page": 0,
            "editing_link": None,
            "link_performance": {},
            "link_active_hours": {},
            "link_titles": {},
            "notification_mode": "SILENT",
            "global_seen_users": {},
            "global_blacklist": [],
            "flood_history": [],
            "panic_mode_until": 0,
            "hour_activity_log": {},
            "engine_uptime_start": time.time(),
            "user_proxies": [],
            "hibernating_links": [],
            "first_login_time": 0,
            "spoofed_device": None,
            "business_auto_reply": None,
            "business_replied_users": {},
            "business_keyword_replies": {}
        }
    return user_data[user_id]

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def extract_hash(link: str) -> str:
    link = link.strip()
    if '/+' in link: return link.split('/+')[-1]
    if '/joinchat/' in link: return link.split('/joinchat/')[-1]
    if 't.me/' in link: return link.split('t.me/')[-1]
    return link

def get_link_grade(checks: float, joins: float) -> str:
    if checks < 3: return "🆕 (Init)"
    if joins < 0.1 and checks >= 15: return "💀 F (Dead)"
    
    ratio = joins / checks if checks > 0 else 0
    if ratio >= 0.50: return "🔥 A+ (Viral)"
    if ratio >= 0.20: return "⭐ A (Excellent)"
    if ratio >= 0.10: return "📈 B (Good)"
    if ratio >= 0.05: return "📊 C (Average)"
    if ratio >= 0.01: return "📉 D (Slow)"
    return "🥱 E (Very Slow)"

def add_live_log(user_id: int, msg: str):
    data = get_user_data(user_id)
    logs = data.setdefault("live_log", [])
    
    # Create timestamp [hh:mm AM/PM]
    timestamp = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("[%I:%M %p]")
    
    # Clean up multiline messages for the log
    clean_msg = msg.replace('\n', ' ')
    if len(clean_msg) > 80:
        clean_msg = clean_msg[:77] + "..."
        
    logs.append(f"`{timestamp}` {clean_msg}")
    if len(logs) > 10:
        data["live_log"] = logs[-10:]
    save_state()

async def send_alert(user_id: int, chat_id: int, msg: str, priority="NORMAL"):
    add_live_log(user_id, msg)
    
    # FORCE SILENT MODE to protect bandwidth. Only send CRITICAL priority.
    if priority != "CRITICAL":
        return
        
    data = get_user_data(user_id)
    mode = data.get("notification_mode", "SILENT")
    
    if mode == "SILENT":
        return
        
    await bot_client.send_message(chat_id, msg)

async def interruptible_sleep(seconds: int, user_id: int) -> bool:
    data = get_user_data(user_id)
    if not data["loop_active"]: return False
    new_time = time.time() + seconds
    if new_time > data.get("next_join_time", 0):
        data["next_join_time"] = new_time
        save_state()
        
    while time.time() < data.get("next_join_time", 0):
        if not data["loop_active"]:
            return False
        await asyncio.sleep(1)
        
    if not data["loop_active"]: return False
    return True

async def show_menu(chat_id: int, user_id: int, event=None):
    data = get_user_data(user_id)
    
    # Lazy load existing session from disk/DB if bot was restarted
    if data["client"] is None:
        has_string = bool(data.get("session_string"))
        
        if has_string:
            kwargs = get_client_kwargs(data)
            save_state()
            client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, **kwargs)
            await client.connect()
            try: await client.get_me() # Sync AuthKey globally on Render IP change
            except: pass
            
            if await client.is_user_authorized():
                data["client"] = client
            else:
                await client.disconnect()
                
    authorized = False
    if data["client"] is not None and await data["client"].is_user_authorized():
        authorized = True
        
    if authorized:
        keyboard = [
            [Button.inline("▶️ START ENGINE", b"start_loop"), Button.inline("⏸️ STOP ENGINE", b"stop_loop")],
            [Button.inline("➕ Add New Link", b"add_link"), Button.inline("📊 Live Queue", b"show_queue")],
            [Button.inline("📝 Live Logs", b"show_live_log"), Button.inline("⚙️ Settings & Proxy", b"settings_menu")],
            [Button.inline("🤖 Chat Automation", b"business_menu"), Button.inline("🩺 Live Diagnostics", b"show_diagnostics")],
            [Button.inline("🚪 Secure Logout", b"logout")]
        ]
        status = "🟢 ACTIVE (Running)" if data["loop_active"] else "🔴 PAUSED (Stopped)"
        
        now = time.time()
        data["daily_joins"] = [ts for ts in data["daily_joins"] if now - ts < 86400]
        
        # Calculate Session Security / Warmup Status
        warmup_status = "🟢 Secured (Full Speed)"
        if "first_login_time" in data and data["first_login_time"] > 0:
            time_elapsed = now - data["first_login_time"]
            if time_elapsed < (72 * 3600):
                progress = int((time_elapsed / (72 * 3600)) * 100)
                warmup_status = f"🟡 Warmup Mode ({progress}%)"
                
        uptime_str = "0h 0m"
        if data["loop_active"]:
            up_sec = int(now - data.get("engine_uptime_start", now))
            uptime_str = f"{up_sec // 3600}h {(up_sec % 3600) // 60}m"
            
        text = (
            f"🖥️ **MAIN CONTROL PANEL**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**Engine State:** {status}\n"
            f"**Uptime:** {uptime_str}\n"
            f"**Session Trust:** {warmup_status}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 **PERFORMANCE (24H)**\n"
            f"**Links in Queue:** `{len(data['queue'])}`\n"
            f"**Successful Joins:** `{len(data['daily_joins'])}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
    else:
        keyboard = [
            [Button.inline("🔑 Login to Link Manager", b"login_prompt")],
            [Button.inline("🤖 Chat Automation", b"business_menu")]
        ]
        text = (
            "👋 **Welcome to the Automated Telegram Link Manager & Auto-Responder!**\n\n"
            "Choose a feature below to get started.\n\n"
            "*(Note: The Link Manager requires you to login with your personal account, while Chat Automation works instantly without logging in.)*"
        )

    if event and hasattr(event, 'edit'):
        await event.edit(text, buttons=keyboard)
    else:
        await bot_client.send_message(chat_id, text, buttons=keyboard)

# ==========================================
# BOT INTERFACE & LOGIN FLOW
# ==========================================

@bot_client.on(events.NewMessage(pattern='(?i)^/start'))
async def start_handler(event):
    await show_menu(event.chat_id, event.sender_id)

@bot_client.on(events.NewMessage(pattern='(?i)^/help'))
async def help_handler(event):
    help_text = (
        "🤖 **Bot Instructions & Help**\n\n"
        "**Commands:**\n"
        "• `/start` - Open your dashboard or see the welcome message.\n"
        "• `/login` - Securely connect your Telegram account.\n"
        "• `/cancel` - Abort any current action (like logging in).\n"
        "• `/help` - Show this message.\n\n"
        "**How to use the Dashboard:**\n"
        "Once logged in, use the inline buttons to add links to your queue. Click **Start Loop** to begin processing them. "
        "The bot will intentionally wait several minutes between actions to keep your account safe from spam filters."
    )
    await event.respond(help_text)

def get_client_kwargs(data):
    proxies = data.get("user_proxies", [])
    
    kwargs = {
        "flood_sleep_threshold": 0, 
        "connection_retries": 3,
        "device_model": "Desktop",
        "system_version": "Windows 11",
        "app_version": "4.16.8 x64",
        "lang_code": "en",
        "system_lang_code": "en-US"
    }
    
    if proxies:
        kwargs["proxy"] = random.choice(proxies)
        
    return kwargs

@bot_client.on(events.NewMessage(pattern='(?i)^/login'))
async def login_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    # Check existing session
    if data["client"] is None and data.get("session_string"):
        kwargs = get_client_kwargs(data)
        save_state()
        client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, **kwargs)
        await client.connect()
        if await client.is_user_authorized():
            data["client"] = client
        else:
            await client.disconnect()
            data["session_string"] = ""
            save_state()
            
    if data["client"] is not None and await data["client"].is_user_authorized():
        await event.respond("✅ You are already logged in! Send /start to open the panel.")
        return
        
    data["login_state"] = "WAITING_PHONE"
    await event.respond(
        "📱 **Login Process Started**\n\n"
        "Please reply with your Telegram Phone Number in international format (e.g., `+1234567890`).\n"
        "Send /cancel to abort."
    )

@bot_client.on(events.NewMessage(pattern='(?i)^/cancel'))
async def cancel_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    data["login_state"] = None
    save_state()
    await event.respond("❌ Action cancelled.")

@bot_client.on(events.NewMessage())
async def message_handler(event):
    text = getattr(event, 'text', '') or ''
    if text.startswith('/'):
        return
        
    user_id = event.sender_id
    data = get_user_data(user_id)
    state = data.get("login_state")
    
    if state == "WAITING_PHONE":
        phone = event.text.strip().replace(' ', '')
        if not phone.startswith('+'):
            await event.respond("❌ Invalid format. Please include the country code (e.g., +1234567890).")
            return
            
        data["phone"] = phone
        kwargs = get_client_kwargs(data)
        save_state()
        client = TelegramClient(StringSession(""), API_ID, API_HASH, **kwargs)
        
        try:
            await client.connect()
            data["client"] = client
            res = await client.send_code_request(phone)
            data["phone_code_hash"] = res.phone_code_hash
            data["login_state"] = "WAITING_CODE"
            await event.respond(
                "💬 **OTP Code Sent!**\n\n"
                "Please check your Telegram app for the login code.\n\n"
                "**⚠️ IMPORTANT:** To prevent Telegram from recognizing and revoking the code, "
                "please send it with spaces or dashes! (e.g., if your code is `12345`, send `1 2 3 4 5` or `1-2-3-4-5`)."
            )
        except PhoneNumberInvalidError:
            await event.respond("❌ Invalid phone number. Please try /login again.")
            data["login_state"] = None
        except Exception as e:
            await event.respond(f"❌ Error sending code: {e}")
            data["login_state"] = None
            save_state()
            
    elif state == "WAITING_CODE":
        # Strip everything except numbers to safely parse formatted codes (like '1 2 3 4 5')
        code = re.sub(r'\D', '', event.text.strip())
        if not code:
            await event.respond("❌ Please provide the numeric code (e.g. `1 2 3 4 5`).")
            return
            
        client = data["client"]
        try:
            await client.sign_in(data["phone"], code, phone_code_hash=data["phone_code_hash"])
            # Human App Simulation: Fetch recent dialogs immediately to prove we are a real app UI loading
            try: await client.get_dialogs(limit=5) 
            except: pass
            
            # Save the session string AFTER fetching dialogs in case the Data Center IP was migrated
            data["session_string"] = client.session.save()
            data["login_state"] = None
            data["first_login_time"] = time.time()
            data["engine_uptime_start"] = time.time()
            instant_save_state()
            await event.respond("✅ **Login Successful!** Session Warmup Protocol engaged for 72 hours. Send /start to open your control panel.")
        except SessionPasswordNeededError:
            data["login_state"] = "WAITING_PASSWORD"
            data["session_string"] = client.session.save()
            instant_save_state()
            await event.respond("🔒 **Two-Step Verification Enabled.**\n\nPlease enter your 2FA password:")
        except (PhoneCodeInvalidError, PhoneCodeExpiredError):
            await event.respond("❌ Invalid or expired code. Please try /login again.")
            data["login_state"] = None
            save_state()
        except Exception as e:
            await event.respond(f"❌ Login error: {e}")
            data["login_state"] = None
            instant_save_state()
            
    elif state == "WAITING_PASSWORD":
        password = event.text.strip()
        client = data["client"]
        try:
            await client.sign_in(password=password)
            # Human App Simulation
            try: await client.get_dialogs(limit=5) 
            except: pass
            
            # Save session AFTER dialogs
            data["session_string"] = client.session.save()
            data["login_state"] = None
            data["first_login_time"] = time.time()
            data["engine_uptime_start"] = time.time()
            instant_save_state()
            await event.respond("✅ **Login Successful!** Session Warmup Protocol engaged for 72 hours. Send /start to open your control panel.")
            try:
                await event.delete() # Delete password from chat history
            except:
                pass
        except Exception as e:
            await event.respond(f"❌ Password error: {e}. Please try again or /cancel.")
            
    elif state == "WAITING_ADD_LINK":
        link = event.text.strip()
        if 't.me' in link:
            if link in data["queue"]:
                await event.respond("⚠️ Link is already in the queue!")
            else:
                data["queue"].append(link)
                await event.respond(f"✅ Added link to queue. Total links: {len(data['queue'])}")
        else:
            await event.respond("❌ Invalid link format. Must contain 't.me'.")
        data["login_state"] = None
        save_state()
        await show_menu(event.chat_id, user_id)
        
    elif state == "WAITING_REMOVE_LINK":
        try:
            idx = int(event.text.strip())
            if 0 <= idx < len(data["queue"]):
                removed = data["queue"].pop(idx)
                if data["current_index"] >= len(data["queue"]) and len(data["queue"]) > 0:
                    data["current_index"] = 0
                await event.respond(f"✅ Removed link at index {idx}: {removed}")
            else:
                await event.respond("❌ Invalid index. Please provide a valid number from the list.")
        except ValueError:
            await event.respond("❌ Please send a valid numeric index.")
        data["login_state"] = None
        save_state()
        await show_menu(event.chat_id, user_id)
        
    elif state == "WAITING_BUSINESS_REPLY":
        raw_ents = event.message.entities if event.message.entities else []
        new_text, new_ents = process_manual_emojis(event.message.message, raw_ents)
        
        ents_dicts = [ent.to_dict() if hasattr(ent, 'to_dict') else ent for ent in new_ents]
        data["business_auto_reply"] = {
            "text": new_text,
            "entities": ents_dicts
        }
        data["login_state"] = None
        instant_save_state()
        await event.respond(f"✅ **Auto-Reply text saved and enabled for THIS account ({user_id})!**\nMake sure you are connecting the bot in the Telegram Settings of THIS exact account, otherwise it won't work.")
        await show_menu(event.chat_id, user_id)
        
    elif state == "WAITING_BUSINESS_KEYWORD":
        raw_text = event.message.message.strip()
        
        # Check if it's bulk format with "="
        if "=" in raw_text and "\n" in raw_text:
            lines = raw_text.split('\n')
            added = 0
            for line in lines:
                if "=" in line:
                    kw_part, reply_part = line.split("=", 1)
                    kw = kw_part.strip().lower()
                    reply_text = reply_part.strip()
                    if kw and reply_text:
                        # Process manual bracket emojis
                        new_text, new_ents = process_manual_emojis(reply_text, [])
                        ents_dicts = [ent.to_dict() if hasattr(ent, 'to_dict') else ent for ent in new_ents]
                        
                        data.setdefault("business_keyword_replies", {})[kw] = {
                            "text": new_text,
                            "entities": ents_dicts
                        }
                        added += 1
            
            data["login_state"] = None
            instant_save_state()
            await event.respond(f"✅ Successfully added {added} bulk keyword replies!")
            await show_menu(event.chat_id, user_id)
            return

        # Otherwise it's single or comma-separated list
        kws = [k.strip().lower() for k in raw_text.split(',') if k.strip()]
        
        if not kws:
            await event.respond("❌ Invalid keyword.")
        else:
            data["temp_keyword"] = kws # Save as a list!
            data["login_state"] = "WAITING_BUSINESS_KEYWORD_REPLY"
            save_state()
            if len(kws) > 1:
                await event.respond(f"✅ {len(kws)} Keywords received: `{', '.join(kws)}`.\nNow, send me the exact reply you want the bot to send for ALL of these keywords:")
            else:
                await event.respond(f"✅ Keyword `{kws[0]}` received.\nNow, send me the exact reply you want the bot to send when someone says this keyword:")
            return
            
    elif state == "WAITING_BUSINESS_KEYWORD_REPLY":
        kws = data.get("temp_keyword")
        if kws:
            if isinstance(kws, str): kws = [kws] # backwards compatibility
            
            raw_ents = event.message.entities if event.message.entities else []
            new_text, new_ents = process_manual_emojis(event.message.message, raw_ents)
            
            ents_dicts = [ent.to_dict() if hasattr(ent, 'to_dict') else ent for ent in new_ents]
            
            for kw in kws:
                data.setdefault("business_keyword_replies", {})[kw] = {
                    "text": new_text,
                    "entities": ents_dicts
                }
                
            data.pop("temp_keyword", None)
            data["login_state"] = None
            instant_save_state()
            
            if len(kws) > 1:
                await event.respond(f"✅ **Bulk Keyword Replies Saved!**\nWhenever someone says any of those {len(kws)} keywords, the bot will instantly send this reply.")
            else:
                await event.respond(f"✅ **Keyword Reply Saved!**\nWhenever someone says `{kws[0]}`, the bot will instantly send this reply.")
        await show_menu(event.chat_id, user_id)
        
    elif state == "WAITING_REMOVE_BUSINESS_KEYWORD":
        kw = event.text.strip().lower()
        kws = data.get("business_keyword_replies", {})
        if kw in kws:
            del kws[kw]
            instant_save_state()
            await event.respond(f"✅ Removed keyword `{kw}`.")
        else:
            await event.respond("❌ Keyword not found.")
        data["login_state"] = None
        save_state()
        await show_menu(event.chat_id, user_id)
        
    elif state == "WAITING_EDIT_LINK":
        link = event.text.strip()
        idx = data.get("editing_link")
        
        if idx is not None and 0 <= idx < len(data["queue"]):
            if 't.me' in link:
                old_link = data["queue"][idx]
                old_hash = extract_hash(old_link)
                new_hash = extract_hash(link)
                
                # Replace link
                data["queue"][idx] = link
                
                # Clean up old hash states
                if old_hash in data.setdefault("paused_links", []): data["paused_links"].remove(old_hash)
                if old_hash in data.setdefault("stopped_links", []): data["stopped_links"].remove(old_hash)
                if old_hash in data.setdefault("link_schedule", {}): del data["link_schedule"][old_hash]
                
                await event.respond(f"✅ Link updated successfully!")
            else:
                await event.respond("❌ Invalid link format. Must contain 't.me'.")
        else:
            await event.respond("❌ Error updating link.")
            
        data["login_state"] = None
        data["editing_link"] = None
        save_state()
        # Go back to queue instead of menu
        class DummyEvent:
            data = b"show_queue_refresh"
            sender_id = user_id
            chat_id = event.chat_id
            async def edit(self, *args, **kwargs):
                pass
            async def answer(self, *args, **kwargs):
                pass
        await callback_handler(DummyEvent())
        
    elif state == "WAITING_SCHEDULE_START":
        try:
            hr = int(event.text.strip())
            if 0 <= hr <= 23:
                idx = data.get("editing_link")
                if idx is not None and 0 <= idx < len(data["queue"]):
                    hash_str = extract_hash(data["queue"][idx])
                    data.setdefault("link_active_hours", {})[hash_str] = {"start": hr, "end": 0}
                    data["login_state"] = "WAITING_SCHEDULE_END"
                    save_state()
                    await event.respond(f"✅ Start Hour set to **{hr:02d}:00**.\n\nNow send me the **End Hour** (0 to 23):\n*(Example: 17 for 5 PM)*")
                else:
                    await event.respond("❌ Link not found.")
                    data["login_state"] = None
                    save_state()
            else:
                await event.respond("❌ Invalid hour. Must be between 0 and 23.")
        except ValueError:
            await event.respond("❌ Please send a valid number between 0 and 23.")
            
    elif state == "WAITING_SCHEDULE_END":
        try:
            hr = int(event.text.strip())
            if 0 <= hr <= 23:
                idx = data.get("editing_link")
                if idx is not None and 0 <= idx < len(data["queue"]):
                    hash_str = extract_hash(data["queue"][idx])
                    data["link_active_hours"][hash_str]["end"] = hr
                    data["login_state"] = None
                    save_state()
                    await event.respond(f"✅ Schedule Saved! Link will only run between {data['link_active_hours'][hash_str]['start']:02d}:00 and {hr:02d}:00.")
                    
                    # Rerender manage link
                    class DummyEvent:
                        data = f"manage_link_{idx}".encode('utf-8')
                        sender_id = user_id
                        chat_id = event.chat_id
                        async def edit(self, *args, **kwargs):
                            pass
                        async def answer(self, *args, **kwargs):
                            pass
                    await callback_handler(DummyEvent())
                else:
                    await event.respond("❌ Link not found.")
                    data["login_state"] = None
                    save_state()
            else:
                await event.respond("❌ Invalid hour. Must be between 0 and 23.")
        except ValueError:
            await event.respond("❌ Please send a valid number between 0 and 23.")
            
    elif state == "WAITING_ADD_PROXY":
        proxy_str = event.text.strip()
        try:
            # Basic parser
            if "://" not in proxy_str:
                raise Exception("Missing protocol (e.g. socks5://)")
            parts = proxy_str.split("://")
            proto = parts[0].lower()
            if proto not in ["socks5", "socks4", "http"]:
                raise Exception("Unsupported protocol. Use socks5, socks4, or http.")
            
            auth_parts = parts[1].split(":")
            if len(auth_parts) == 2: # IP:Port
                ip, port = auth_parts
                user, pw = None, None
            elif len(auth_parts) == 4: # IP:Port:User:Pass
                ip, port, user, pw = auth_parts
            else:
                raise Exception("Invalid format.")
                
            proxy_dict = {
                "proxy_type": proto,
                "addr": ip,
                "port": int(port),
                "rdns": True,
                "username": user,
                "password": pw
            }
            data.setdefault("user_proxies", []).append(proxy_dict)
            await event.respond(f"✅ Added {proto.upper()} proxy: {ip}:{port}")
        except Exception as e:
            await event.respond(f"❌ Proxy error: {e}\nFormat: socks5://ip:port:user:pass")
            
        data["login_state"] = None
        save_state()
        
        # Go back to proxy menu
        class DummyEvent:
            data = b"proxies_menu"
            sender_id = user_id
            chat_id = event.chat_id
            async def edit(self, *args, **kwargs): pass
            async def answer(self, *args, **kwargs): pass
        await callback_handler(DummyEvent())
        
    else:
        # Smart Bulk Link Extractor fallback
        text = event.text.strip()
        if 't.me' in text:
            # Find all links that look like Telegram links
            links = re.findall(r'(?:https?://)?t\.me/[^\s]+', text)
            if links:
                added = 0
                for l in links:
                    # Clean up trailing punctuation if any
                    l = l.rstrip("),.\"\'")
                    # Ensure prefix
                    if not l.startswith("http"):
                        l = "https://" + l
                        
                    if l not in data["queue"]:
                        data["queue"].append(l)
                        added += 1
                
                if added > 0:
                    save_state()
                    await event.respond(f"🧠 **Smart Extractor**\n✅ Found and added **{added}** new links to your queue!\n*(Total Links: {len(data['queue'])})*")
                else:
                    await event.respond("⚠️ All found links are already in your queue.")
                    
                # Clean up the massive forwarded message
                try:
                    await event.delete()
                except:
                    pass
                await show_menu(event.chat_id, user_id)

@bot_client.on(events.CallbackQuery())
async def callback_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    # Lazy load existing session from disk/DB if bot was restarted
    if data["client"] is None:
        has_string = bool(data.get("session_string"))
        
        if has_string:
            kwargs = get_client_kwargs(data)
            client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, **kwargs)
            await client.connect()
            try: await client.get_me() # Forces Telegram to globally sync the AuthKey on a new Render IP
            except: pass
            if await client.is_user_authorized():
                data["client"] = client
            else:
                await client.disconnect() # PREVENT TCP LEAK
                save_state()
                
    cb_data = event.data.decode('utf-8')
    unauthenticated_callbacks = ["business_menu", "set_business_reply", "turn_off_business", "back_to_menu", "login_prompt", "add_business_keyword", "remove_business_keyword"]
    
    if data["client"] is None and cb_data not in unauthenticated_callbacks:
        await event.answer("You are not logged in! Send /login", alert=True)
        return
        
    if cb_data == "login_prompt":
        await event.answer("To use the Link Manager, please send the command /login in this chat.", alert=True)
        return
    
    if cb_data == "add_link":
        data["login_state"] = "WAITING_ADD_LINK"
        save_state()
        await event.respond("Send me the invite link to add (e.g., https://t.me/+...):")
        
    elif cb_data == "remove_link":
        if not data["queue"]:
            await event.respond("Queue is currently empty.")
            return
        msg = "Send the index number of the link to remove:\n\n"
        for i, l in enumerate(data["queue"]):
            msg += f"`{i}`: {l}\n"
        data["login_state"] = "WAITING_REMOVE_LINK"
        save_state()
        await event.respond(msg)
        
    elif cb_data == "business_menu":
        reply_txt = data.get("business_auto_reply")
        reply_disp = reply_txt.get("text") if isinstance(reply_txt, dict) else reply_txt
        keyword_replies = data.get("business_keyword_replies", {})
        
        status = "🟢 ON" if (reply_txt or keyword_replies) else "🔴 OFF"
        msg = f"🤖 **CHAT AUTOMATION (Business)**\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"**Auto-Responder:** `{status}`\n\n"
        
        if reply_disp:
            msg += f"**Default 24H Reply:**\n`{reply_disp}`\n\n"
            
        if keyword_replies:
            msg += f"**Keyword Replies ({len(keyword_replies)}):**\n"
            for kw in keyword_replies:
                msg += f"• `{kw}`\n"
            msg += "\n"
                
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"*Connect this bot to your Personal Account via Telegram Settings -> Telegram Business -> Chat Automation to auto-reply to DMs!*\n"
        
        kb = [[Button.inline("📝 Set Default 24H Reply", b"set_business_reply")]]
        kb.append([Button.inline("➕ Add Keyword Reply", b"add_business_keyword")])
        
        if keyword_replies:
            kb.append([Button.inline("➖ Remove Keyword", b"remove_business_keyword")])
            
        if reply_txt or keyword_replies:
            kb.append([Button.inline("❌ Turn OFF All", b"turn_off_business")])
            
        kb.append([Button.inline("🔙 Back to Dashboard", b"back_to_menu")])
        await event.edit(msg, buttons=kb)
        
    elif cb_data == "set_business_reply":
        data["login_state"] = "WAITING_BUSINESS_REPLY"
        save_state()
        await event.respond("Send me the exact text you want the bot to auto-reply to users with as a default 24-hour welcome message:")
        
    elif cb_data == "add_business_keyword":
        data["login_state"] = "WAITING_BUSINESS_KEYWORD"
        save_state()
        await event.respond(
            "Send me the **KEYWORD** you want the bot to detect.\n\n"
            "**Bulk Adding Options:**\n"
            "• To map multiple keywords to ONE reply, send them separated by commas: `price, cost, fee`\n"
            "• To map multiple keywords to DIFFERENT replies, use the format `keyword = reply` on new lines:\n"
            "`price = The price is $50`\n"
            "`hello = Hi there!`"
        )
        
    elif cb_data == "remove_business_keyword":
        keyword_replies = data.get("business_keyword_replies", {})
        if not keyword_replies:
            await event.answer("No keywords to remove.", alert=True)
            return
            
        msg = "Send me the exact KEYWORD you want to remove:\n\n"
        for kw in keyword_replies:
            msg += f"• `{kw}`\n"
            
        data["login_state"] = "WAITING_REMOVE_BUSINESS_KEYWORD"
        save_state()
        await event.respond(msg)
        
    elif cb_data == "turn_off_business":
        data["business_auto_reply"] = None
        data["business_keyword_replies"] = {}
        save_state()
        await event.answer("Auto-Responder Disabled", alert=True)
        # Re-render business menu
        class DummyEventBusiness:
            data = b"business_menu"
            sender_id = user_id
            chat_id = event.chat_id
            async def edit(self, *args, **kwargs): pass
            async def answer(self, *args, **kwargs): pass
        await callback_handler(DummyEventBusiness())
        
    elif cb_data == "show_diagnostics":
        process_uptime = int(time.time() - PHYSICAL_BOOT_TIME)
        p_up_str = f"{process_uptime // 60}m {process_uptime % 60}s"
        
        has_client = data["client"] is not None
        sess_len = len(data.get("session_string", ""))
        
        diag_msg = "🩺 **SYSTEM DIAGNOSTICS**\n"
        diag_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        diag_msg += f"**Physical Server Uptime:** `{p_up_str}`\n"
        diag_msg += f"**Active Client Object:** `{'YES' if has_client else 'NO (Wiped)'}`\n"
        diag_msg += f"**Saved Session Length:** `{sess_len} chars`\n"
        diag_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        diag_msg += "*If Physical Uptime is low but Engine Uptime is high, Render forcefully restarted your server in the background!*\n"
        
        # Try to fetch current IP
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get('https://api.ipify.org?format=json', timeout=3) as r:
                    res = await r.json()
                    diag_msg += f"\n**Current Server IP:** `{res['ip']}`"
        except:
            pass
            
        await event.answer("Diagnostics Loaded", alert=False)
        await event.respond(diag_msg)
        
    elif cb_data == "start_loop":
        if not data["queue"]:
            await event.answer("Cannot start. Queue is empty!", alert=True)
            return
        if data["loop_active"]:
            await event.answer("Engine is already running!", alert=True)
            return
            
        data["loop_active"] = True
        save_state()
        
        # Spawn the background task for this specific user
        if data["task"] is None or data["task"].done():
            data["task"] = asyncio.create_task(runner_engine(user_id, event.chat_id))
            
        await event.answer("Engine Started! Running silently in background.", alert=True)
        await show_menu(event.chat_id, user_id, event=event)
        
    elif cb_data == "stop_loop":
        if not data["loop_active"]:
            await event.answer("Engine is already stopped!", alert=True)
            return
        data["loop_active"] = False
        data["first_join_done"] = False
        data["next_join_time"] = 0
        save_state()
        await event.answer("Engine Paused!", alert=True)
        await show_menu(event.chat_id, user_id, event=event)
        
    elif cb_data == "settings_menu":
        mode = data.get("notification_mode", "ALL")
        msg = "⚙️ **SYSTEM CONFIGURATION**\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "**Notification Level:**\n"
        msg += "Choose how aggressively the bot alerts you about queue events.\n"
        
        btn_all = "✅ Everything" if mode == "ALL" else "Everything"
        btn_viral = "✅ Viral Only" if mode == "VIRAL_ONLY" else "Viral Only"
        btn_silent = "✅ Silent Mode" if mode == "SILENT" else "Silent Mode"
        
        keyboard = [
            [Button.inline(btn_all, b"set_notif_ALL")],
            [Button.inline(btn_viral, b"set_notif_VIRAL_ONLY")],
            [Button.inline(btn_silent, b"set_notif_SILENT")],
            [Button.inline("🌐 Proxy Manager", b"proxies_menu")],
            [Button.inline("🔙 Back to Dashboard", b"back_to_menu")]
        ]
        await event.edit(msg, buttons=keyboard)
        
    elif cb_data.startswith("set_notif_"):
        new_mode = cb_data.split("set_notif_")[1]
        data["notification_mode"] = new_mode
        save_state()
        await event.answer("Settings Saved!", alert=True)
        event.data = b"settings_menu"
        await callback_handler(event)
        
    elif cb_data == "back_to_menu":
        await show_menu(event.chat_id, user_id, event=event)
        
    elif cb_data.startswith("show_live_log"):
        logs = data.get("live_log", [])
        msg = "📝 **LIVE ENGINE LOG**\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        if not logs:
            msg += "*(No recent actions recorded.)*\n"
        else:
            for log_entry in reversed(logs):
                msg += f"{log_entry}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "*Click Refresh to fetch latest events.*\n"
        
        keyboard = [
            [Button.inline("🔄 Refresh Log", b"show_live_log_refresh")],
            [Button.inline("🔙 Back to Dashboard", b"back_to_menu")]
        ]
        await event.edit(msg, buttons=keyboard)
        await event.answer()
        
    elif cb_data == "show_live_log_refresh":
        event.data = b"show_live_log"
        await callback_handler(event)
        
    elif cb_data == "proxies_menu":
        proxies = data.get("user_proxies", [])
        msg = f"🌐 **PROXY MANAGER (IP ROTATION)**\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"**Active Proxies:** `{len(proxies)}`\n\n"
        if proxies:
            for i, p in enumerate(proxies):
                msg += f"🟢 `[{i + 1}]` **{p['proxy_type'].upper()}** ➔ `{p['addr']}:{p['port']}`\n"
        else:
            msg += "⚠️ *No proxies added. Using Default Server IP.*\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        keyboard = [
            [Button.inline("➕ Add Proxy", b"add_proxy"), Button.inline("❌ Clear All", b"clear_proxies")],
            [Button.inline("🔙 Back to Settings", b"settings_menu")]
        ]
        await event.edit(msg, buttons=keyboard)
        
    elif cb_data == "add_proxy":
        data["login_state"] = "WAITING_ADD_PROXY"
        save_state()
        await event.respond("**🌐 Add Proxy**\n\nSend me your SOCKS5 or HTTP proxy in this format:\n`socks5://ip:port:username:password`\n\nOr without auth:\n`http://ip:port`")
        
    elif cb_data == "clear_proxies":
        data["user_proxies"] = []
        save_state()
        await event.answer("All proxies cleared!", alert=True)
        event.data = b"proxies_menu"
        await callback_handler(event)
        
    elif cb_data.startswith("show_queue"):
        if not data["queue"]:
            await event.answer("Queue is empty.", alert=True)
            return
            
        page = data.get("queue_page", 0)
        
        if cb_data == "show_queue_next":
            page += 1
        elif cb_data == "show_queue_prev":
            page -= 1
        elif cb_data == "show_queue":
            page = 0
            
        max_page = max(0, (len(data["queue"]) - 1) // 5)
        page = max(0, min(page, max_page))
        data["queue_page"] = page
        save_state()
        
        start_idx = page * 5
        end_idx = start_idx + 5
        page_queue = data["queue"][start_idx:end_idx]
        
        msg = f"📊 **LIVE QUEUE MONITOR**\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"**Page:** {page+1} of {max_page+1} | **Total Links:** {len(data['queue'])}\n\n"
        now = time.time()
        
        row_buttons = []
        for i, l in enumerate(page_queue):
            actual_idx = start_idx + i
            hash_str = extract_hash(l)
            
            perf = data.get("link_performance", {}).get(hash_str, {"checks": 0, "joins": 0})
            grade = get_link_grade(perf["checks"], perf["joins"]).split(' ')[0] # just get the emoji/letter
            
            # Check Schedule
            sched_str = ""
            active_hours = data.get("link_active_hours", {}).get(hash_str)
            if active_hours:
                start_hr = active_hours["start"]
                end_hr = active_hours["end"]
                sched_str = f" [🕒 {start_hr:02d}:00-{end_hr:02d}:00]"
                
            if hash_str in data.get("stopped_links", []):
                status = "🔴 STOPPED"
            elif hash_str in data.get("paused_links", []):
                status = "🟡 PAUSED"
            elif hash_str in data.get("hibernating_links", []):
                status = "💤 SLEEPING (Dead Group)"
            else:
                check_time = data.get("link_schedule", {}).get(hash_str, 0)
                if check_time == 0:
                    status = "⏳ WAITING FOR SCAN"
                elif check_time <= now:
                    status = "🔥 ACTIVE NOW"
                else:
                    wait_sec = int(check_time - now)
                    status = f"🕒 IN {wait_sec // 60:02d}:{wait_sec % 60:02d}"
                    
            title = data.get("link_titles", {}).get(hash_str, "⏳ Fetching Group Info...")
            
            msg += f"**[{actual_idx + 1}] {title}**\n"
            msg += f"└ 🔗 {l}\n"
            msg += f"└ {grade} | {status}{sched_str}\n\n"
            
            row_buttons.append(Button.inline(f"[{actual_idx + 1}]", f"manage_link_{actual_idx}".encode('utf-8')))
            
        keyboard = []
        if row_buttons:
            # Chunk row_buttons into rows of 5
            for j in range(0, len(row_buttons), 5):
                keyboard.append(row_buttons[j:j+5])
                
        nav_buttons = []
        if page > 0:
            nav_buttons.append(Button.inline(f"⬅️ Page {page}", b"show_queue_prev"))
        if page < max_page:
            nav_buttons.append(Button.inline(f"Page {page+2} ➡️", b"show_queue_next"))
        if nav_buttons:
            keyboard.append(nav_buttons)
            
        keyboard.append([Button.inline("🔄 Refresh Live Queue", b"show_queue_refresh")])
        keyboard.append([Button.inline("🧹 Prune Dead Links", b"prune_dead_links")])
        keyboard.append([Button.inline("🔙 Back to Dashboard", b"back_to_menu")])
        await event.edit(msg, buttons=keyboard, link_preview=False)
        await event.answer()
        
    elif cb_data.startswith("manage_link_"):
        idx = int(cb_data.split("_")[2])
        if idx >= len(data["queue"]):
            await event.answer("Link not found.", alert=True)
            return
            
        l = data["queue"][idx]
        hash_str = extract_hash(l)
        
        is_paused = hash_str in data.get("paused_links", [])
        is_stopped = hash_str in data.get("stopped_links", [])
        
        is_hibernating = hash_str in data.get("hibernating_links", [])
        
        perf = data.get("link_performance", {}).get(hash_str, {"checks": 0, "joins": 0})
        grade = get_link_grade(perf["checks"], perf["joins"])
        
        active_hours = data.get("link_active_hours", {}).get(hash_str)
        title = data.get("link_titles", {}).get(hash_str, "⏳ Fetching...")
        
        msg = f"🎛️ **GROUP PROFILE `[{idx + 1}]`**\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"**Name:** {title}\n"
        msg += f"**Link:** {l}\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📊 **ANALYTICS**\n"
        msg += f"**Health Grade:** {grade}\n"
        msg += f"**Total Lifetime Checks:** `{perf.get('total_checks', int(perf['checks']))}` | **Total Joins:** `{perf.get('total_joins', int(perf['joins']))}`\n"
        msg += f"*(AI Rolling Window: {int(perf['checks'])} checks, {int(perf['joins'])} joins)*\n"
        
        if active_hours:
            msg += f"**Schedule (IST):** 🕒 {active_hours['start']:02d}:00 - {active_hours['end']:02d}:00\n"
            
        if is_stopped:
            msg += "**Status:** 🔴 STOPPED (Ignored by engine)\n"
        elif is_paused:
            msg += "**Status:** 🟡 PAUSED (Skipping queue)\n"
        elif is_hibernating:
            msg += "**Status:** 💤 SLEEPING (Dead group, waiting for sonar)\n"
        else:
            now = time.time()
            check_time = data.get("link_schedule", {}).get(hash_str, 0)
            if check_time == 0:
                msg += "**Status:** ⏳ WAITING for initial scan\n"
            elif check_time <= now:
                msg += "**Status:** 🔥 ACTIVE NOW\n"
            else:
                wait_sec = int(check_time - now)
                msg += f"**Status:** 🕒 IN {wait_sec // 60}M {wait_sec % 60}S\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
                
        pause_btn = Button.inline("🟡 PAUSED [RESUME]", f"resume_link_{idx}".encode('utf-8')) if is_paused else Button.inline("🟢 ACTIVE [PAUSE]", f"pause_link_{idx}".encode('utf-8'))
        stop_btn = Button.inline("🔴 STOPPED [START]", f"start_link_{idx}".encode('utf-8')) if is_stopped else Button.inline("🛑 STOP LINK", f"stop_link_{idx}".encode('utf-8'))
        
        sched_row = [Button.inline("🕒 Edit Schedule", f"set_sched_{idx}".encode('utf-8')), Button.inline("❌ Clear Schedule", f"clr_sched_{idx}".encode('utf-8'))] if active_hours else [Button.inline("🕒 Set Schedule", f"set_sched_{idx}".encode('utf-8'))]
        
        keyboard = [
            [pause_btn, stop_btn],
            sched_row,
            [Button.inline("✏️ Edit URL", f"edit_link_{idx}".encode('utf-8')), Button.inline("🗑️ Delete Link", f"del_link_{idx}".encode('utf-8'))],
            [Button.inline("🔙 Back to Queue", b"show_queue_refresh")]
        ]
        await event.edit(msg, buttons=keyboard, link_preview=False)
        await event.answer()
        
    elif cb_data == "show_queue_refresh":
        event.data = b"show_queue_refresh"
        await callback_handler(event) # This will hit the startswith("show_queue") block and reload the current page

    elif cb_data.startswith("pause_link_"):
        idx = int(cb_data.split("_")[2])
        hash_str = extract_hash(data["queue"][idx])
        if hash_str not in data.setdefault("paused_links", []):
            data["paused_links"].append(hash_str)
        if hash_str in data.setdefault("stopped_links", []):
            data["stopped_links"].remove(hash_str)
        save_state()
        event.data = f"manage_link_{idx}".encode('utf-8')
        await callback_handler(event)

    elif cb_data.startswith("resume_link_"):
        idx = int(cb_data.split("_")[2])
        hash_str = extract_hash(data["queue"][idx])
        if hash_str in data.setdefault("paused_links", []):
            data["paused_links"].remove(hash_str)
        save_state()
        event.data = f"manage_link_{idx}".encode('utf-8')
        await callback_handler(event)

    elif cb_data.startswith("stop_link_"):
        idx = int(cb_data.split("_")[2])
        hash_str = extract_hash(data["queue"][idx])
        if hash_str not in data.setdefault("stopped_links", []):
            data["stopped_links"].append(hash_str)
        if hash_str in data.setdefault("paused_links", []):
            data["paused_links"].remove(hash_str)
        # Clear its schedule
        if hash_str in data.setdefault("link_schedule", {}):
            del data["link_schedule"][hash_str]
        save_state()
        event.data = f"manage_link_{idx}".encode('utf-8')
        await callback_handler(event)

    elif cb_data.startswith("start_link_"):
        idx = int(cb_data.split("_")[2])
        hash_str = extract_hash(data["queue"][idx])
        if hash_str in data.setdefault("stopped_links", []):
            data["stopped_links"].remove(hash_str)
        # Reset schedule to check immediately
        data.setdefault("link_schedule", {})[hash_str] = 0
        save_state()
        event.data = f"manage_link_{idx}".encode('utf-8')
        await callback_handler(event)
        
    elif cb_data.startswith("del_link_"):
        idx = int(cb_data.split("_")[2])
        if idx < len(data["queue"]):
            hash_str = extract_hash(data["queue"][idx])
            data["queue"].pop(idx)
            if hash_str in data.setdefault("stopped_links", []): data["stopped_links"].remove(hash_str)
            if hash_str in data.setdefault("paused_links", []): data["paused_links"].remove(hash_str)
            if hash_str in data.setdefault("link_schedule", {}): del data["link_schedule"][hash_str]
            save_state()
            await event.answer("Link deleted!", alert=True)
        event.data = b"show_queue_refresh"
        await callback_handler(event)

    elif cb_data.startswith("edit_link_"):
        idx = int(cb_data.split("_")[2])
        data["login_state"] = "WAITING_EDIT_LINK"
        data["editing_link"] = idx
        save_state()
        await event.respond(f"Send me the new invite link to replace Link `[{idx + 1}]`:")
        
    elif cb_data == "prune_dead_links":
        queue_copy = list(data["queue"])
        pruned_count = 0
        for l in queue_copy:
            hash_str = extract_hash(l)
            perf = data.get("link_performance", {}).get(hash_str, {"checks": 0, "joins": 0})
            if perf["joins"] == 0 and perf["checks"] >= 50:
                data["queue"].remove(l)
                if hash_str in data.setdefault("stopped_links", []): data["stopped_links"].remove(hash_str)
                if hash_str in data.setdefault("paused_links", []): data["paused_links"].remove(hash_str)
                if hash_str in data.setdefault("link_schedule", {}): del data["link_schedule"][hash_str]
                if hash_str in data.setdefault("link_active_hours", {}): del data["link_active_hours"][hash_str]
                pruned_count += 1
        save_state()
        await event.answer(f"Pruned {pruned_count} dead links!", alert=True)
        event.data = b"show_queue_refresh"
        await callback_handler(event)

    elif cb_data.startswith("set_sched_"):
        idx = int(cb_data.split("_")[2])
        data["login_state"] = "WAITING_SCHEDULE_START"
        data["editing_link"] = idx
        save_state()
        await event.respond(f"**🕒 Set Schedule for Link `[{idx + 1}]`**\n\nSend me the **Start Hour** (0 to 23 in IST time):\n*(Example: 9 for 9 AM)*")
        
    elif cb_data.startswith("clr_sched_"):
        idx = int(cb_data.split("_")[2])
        hash_str = extract_hash(data["queue"][idx])
        if hash_str in data.setdefault("link_active_hours", {}):
            del data["link_active_hours"][hash_str]
        save_state()
        event.data = f"manage_link_{idx}".encode('utf-8')
        await callback_handler(event)
        
    elif cb_data == "logout":
        data["loop_active"] = False
        if data["client"]:
            await data["client"].log_out()
            data["client"] = None
        data["queue"] = []
        data["daily_joins"] = []
        data["next_join_time"] = 0
        instant_save_state()
        # Delete session file if it exists
        session_file = f'sessions/user_{user_id}.session'
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except:
                pass
        await event.respond("🚪 **Logged out successfully.** Your session and queue have been cleared.")

# ==========================================
# ISOLATED RUNNER ENGINE
# ==========================================

async def runner_engine(user_id: int, chat_id: int):
    data = get_user_data(user_id)
    
    while True:
        if not data["loop_active"] or not data["queue"]:
            if not data["loop_active"]:
                break
            await asyncio.sleep(5)
            continue
            
        user_client = data.get("client")
        if user_client is None:
            has_string = bool(data.get("session_string"))
            
            if has_string:
                kwargs = get_client_kwargs(data)
                
                if "proxy" in kwargs:
                    # To display proxy info safely, we check original list since kwargs["proxy"] is a dict
                    p = kwargs["proxy"]
                    await send_alert(user_id, chat_id, f"🌐 **Proxy Connected:** Engine started on {p['proxy_type'].upper()} proxy ({p['addr']})")
                    
                user_client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, **kwargs)
                    
                await user_client.connect()
                try: await user_client.get_me() # Sync AuthKey
                except: pass
                
                data["client"] = user_client
                
                # Boot Delay (Protect new sessions from instant API requests upon server restarts)
                uptime = time.time() - data.get("engine_uptime_start", time.time())
                if uptime < 300:
                    is_warmup = (time.time() - data.get("first_login_time", 0)) < (3 * 86400)
                    if is_warmup:
                        delay_left = 300 - int(uptime)
                        await send_alert(user_id, chat_id, f"🛡️ **Warmup Protection:** Delaying engine start for {delay_left}s to prevent Telegram anti-spam from flagging your new session.", priority="CRITICAL")
                        await interruptible_sleep(delay_left, user_id)
                # Save the session string immediately in case connecting updated the AuthKey or Datacenter
                data["session_string"] = user_client.session.save()
                save_state()
            else:
                await send_alert(user_id, chat_id, "⚠️ **Session missing.** Please /login again.")
                data["loop_active"] = False
                save_state()
                break

        now = time.time()
        
        # PANIC MODE CHECK
        panic_until = data.get("panic_mode_until", 0)
        if now < panic_until:
            wait_sec = int(panic_until - now)
            # Sleep in chunks to allow interruption
            chunk = min(wait_sec, 60)
            if not await interruptible_sleep(chunk, user_id):
                break
            continue
            
        # SMART MICRO-NAPS (Rush-Hour Aware Stutter Stepping)
        uptime = now - data.setdefault("engine_uptime_start", now)
        # Random trigger between 45 to 90 mins (2700 to 5400 seconds)
        if uptime > random.randint(2700, 5400):
            # Check Rush Hour override
            ist = timezone(timedelta(hours=5, minutes=30))
            now_ist = datetime.now(ist)
            current_hour_str = str(now_ist.hour)
            
            is_rush_hour = False
            for hash_str, logs in data.get("hour_activity_log", {}).items():
                total = sum(logs.values())
                if total >= 5:
                    hr_joins = logs.get(current_hour_str, 0)
                    if hr_joins / total >= 0.2:
                        is_rush_hour = True
                        break
                        
            if is_rush_hour:
                await send_alert(user_id, chat_id, "🔥 **AI Overdrive:** Canceled scheduled Micro-Nap because it's Rush Hour. Running at 100% capacity!")
                data["engine_uptime_start"] = now # Reset uptime to check again later
                save_state()
            else:
                nap_sec = random.randint(120, 480) # 2 to 8 mins
                await send_alert(user_id, chat_id, f"☕ **Micro-Nap Triggered:** Taking a deeply randomized break for {nap_sec // 60}m {nap_sec % 60}s to break API heartbeat...")
                if not await interruptible_sleep(nap_sec, user_id):
                    break
                data["engine_uptime_start"] = time.time()
                save_state()
        
        # Prevent ghost 24-hour sleeps from old limits locking up the loop
        if data.get("next_join_time", 0) > now + 2:
            sleep_left = int(data["next_join_time"] - now)
            if sleep_left > 7200: # Over 2 hours (likely the old daily limit bug)
                data["next_join_time"] = 0
                save_state()
                await send_alert(user_id, chat_id, "🧹 **Cleared ghost sleep.** Resuming fast loop...")
            else:
                if sleep_left > 10:
                    await send_alert(user_id, chat_id, f"💤 **Resuming Wait:** Sleeping for {sleep_left // 60}m {sleep_left % 60}s before continuing.")
                if not await interruptible_sleep(0, user_id):
                    break
        
        # Priority Scheduling Logic
        earliest_link = None
        earliest_time = float('inf')
        
        # Current IST Time
        ist = timezone(timedelta(hours=5, minutes=30))
        now_ist = datetime.now(ist)
        current_hour = now_ist.hour
        
        for link in data["queue"]:
            hash_str = extract_hash(link)
            if hash_str in data.get("paused_links", []) or hash_str in data.get("stopped_links", []):
                continue
                
            # Check Custom IST Schedule
            active_hours = data.get("link_active_hours", {}).get(hash_str)
            if active_hours:
                start_hr = active_hours["start"]
                end_hr = active_hours["end"]
                # Handle overnight ranges like 22 to 6
                if start_hr < end_hr:
                    is_active_now = start_hr <= current_hour < end_hr
                else:
                    is_active_now = current_hour >= start_hr or current_hour < end_hr
                if not is_active_now:
                    continue
                    
            # Default to 0 so new links get checked immediately
            check_time = data.get("link_schedule", {}).get(hash_str, 0)
            if check_time < earliest_time:
                earliest_time = check_time
                earliest_link = link
                
        if earliest_link is None:
            # All links are paused or stopped
            if not await interruptible_sleep(5, user_id):
                break
            continue
            
        # If the earliest link is still in the future, we sleep until it's ready
        if earliest_time > time.time():
            sleep_needed = int(earliest_time - time.time())
            # We enforce a max chunk sleep of 30s so the loop can quickly react to Stop commands
            chunk = min(sleep_needed, 30)
            if sleep_needed > 30 and chunk == 30:
                # Only spam the log if it's a long sleep
                await send_alert(user_id, chat_id, f"💤 **Queue Sleeping:** No links ready. Sleeping for {sleep_needed // 60}m {sleep_needed % 60}s...")
            if not await interruptible_sleep(chunk, user_id):
                break
            continue # Restart the loop to re-evaluate schedules
                
        link = earliest_link
        hash_str = extract_hash(link)
        
        # -----------------------------
        # DYNAMIC TRAFFIC CHECK
        # -----------------------------
        is_active_mode = True
        participants_count = None
        diff = 0
        is_high_traffic = False
        last_count = data.get("link_stats", {}).get(hash_str, 0)
        
        try:
            invite_info = await user_client(CheckChatInviteRequest(hash_str))
            
            # Record analytics: Intelligent Decay (Rolling Window)
            # Keeps the grade dynamically shifting based on RECENT traffic
            perf = data.setdefault("link_performance", {}).setdefault(hash_str, {"checks": 0, "joins": 0})
            
            # Absolute non-decaying tracking for user display
            perf["total_checks"] = perf.get("total_checks", int(perf.get("checks", 0))) + 1
            
            current_checks = float(perf.get("checks", 0))
            current_joins = float(perf.get("joins", 0))
            
            if current_checks >= 50:
                current_checks *= 0.8
                current_joins *= 0.8
                
            perf["checks"] = round(current_checks + 1, 2)
            perf["joins"] = round(current_joins, 2)
            
            # Extract participants count
            if hasattr(invite_info, 'participants_count'):
                participants_count = invite_info.participants_count
            elif hasattr(invite_info, 'chat') and hasattr(invite_info.chat, 'participants_count'):
                participants_count = invite_info.chat.participants_count
                
            # Cache Title
            if hasattr(invite_info, 'title'):
                data.setdefault("link_titles", {})[hash_str] = invite_info.title
            elif hasattr(invite_info, 'chat') and hasattr(invite_info.chat, 'title'):
                data.setdefault("link_titles", {})[hash_str] = invite_info.chat.title
            elif hash_str not in data.get("link_titles", {}):
                data.setdefault("link_titles", {})[hash_str] = "Unknown Group"
            
            recent_ids = []
            new_unique_users = 0
            provided_participants = False
            if hasattr(invite_info, 'participants') and invite_info.participants:
                provided_participants = True
                seen_users = data.get("link_seen_users", {}).get(hash_str, [])
                global_blacklist = data.setdefault("global_blacklist", [])
                global_seen = data.setdefault("global_seen_users", {})
                
                for p in invite_info.participants:
                    # Hive Mind Spam Check
                    if p.id in global_blacklist or p.id in GLOBAL_SPAMMERS:
                        continue # Completely ignore blacklisted user
                        
                    # Track globally
                    user_groups = global_seen.setdefault(str(p.id), [])
                    if hash_str not in user_groups:
                        user_groups.append(hash_str)
                        if len(user_groups) >= 3:
                            global_blacklist.append(p.id)
                            if p.id not in GLOBAL_SPAMMERS:
                                GLOBAL_SPAMMERS.add(p.id)
                                if spam_collection is not None:
                                    asyncio.create_task(spam_collection.update_one(
                                        {"_id": "global_blacklist"},
                                        {"$addToSet": {"user_ids": p.id}},
                                        upsert=True
                                    ))
                            continue # Ignore this user, they are a spammer
                            
                    recent_ids.append(p.id)
                    if p.id not in seen_users:
                        new_unique_users += 1
            
            if participants_count is not None:
                # High Traffic Logic
                is_high_traffic = data.get("high_traffic_links", {}).get(hash_str, 0) > time.time() - 300 # Valid for 5 mins
                
                if last_count > 0:
                    time_since_last_action = time.time() - data.get("link_last_action", {}).get(hash_str, 0)
                    diff = participants_count - last_count
                    
                    if diff >= 10:
                        if provided_participants and new_unique_users == 0:
                            is_active_mode = False
                            await send_alert(user_id, chat_id, f"📉 **Passive Mode (Spam Filter):** Ignored {diff} joins in `{link}` because they were all repeat spammers.", priority="LOW")
                        else:
                            is_active_mode = True
                            data.setdefault("high_traffic_links", {})[hash_str] = time.time()
                            await send_alert(user_id, chat_id, f"🔥 **Active Mode (High Traffic):** {diff} new users joined `{link}`. Engaging!", priority="HIGH")
                    elif new_unique_users > 0 or (diff >= 1 and not provided_participants):
                        if is_high_traffic:
                            is_active_mode = False
                            await send_alert(user_id, chat_id, f"⏳ **Passive Mode (Throttling):** Genuine new users detected in `{link}`, waiting for 10 users because group is High Traffic.")
                        else:
                            is_active_mode = True
                            await send_alert(user_id, chat_id, f"🔥 **Active Mode:** Genuine new users detected in `{link}`. Engaging!")
                    else:
                        is_active_mode = False
                        if diff > 0:
                            await send_alert(user_id, chat_id, f"📉 **Passive Mode (Spam Filter):** Ignored {diff} joins in `{link}` because they were all repeat spammers.", priority="LOW")
                        else:
                            await send_alert(user_id, chat_id, f"📉 **Passive Mode:** No new users detected in `{link}`. Skipping join.", priority="LOW")
                else:
                    # First time checking
                    is_active_mode = True
                    await send_alert(user_id, chat_id, f"🔥 **Active Mode:** First time checking `{link}` ({participants_count} members). Engaging!", priority="NORMAL")
                
                # Update stats ONLY when we actually take action
                if is_active_mode:
                    data["link_stats"][hash_str] = participants_count
                    if len(recent_ids) > 0:
                        seen = set(data.get("link_seen_users", {}).get(hash_str, []))
                        seen.update(recent_ids)
                        data.setdefault("link_seen_users", {})[hash_str] = list(seen)[-200:]
                    data.setdefault("link_last_action", {})[hash_str] = time.time()
                    save_state()
        except Exception as e:
            # If we can't check it, default to active and let the join try block handle errors
            pass

        if is_active_mode:
            data["active_links_count"] += 1
            # Step A: Pre-Action Delay (Prevent Telegram Anti-Spam)
            if not data.get("first_join_done"):
                delay = random.randint(2, 5)
                data["first_join_done"] = True
                save_state()
            else:
                delay = random.randint(5, 15)
                
            if not await interruptible_sleep(delay, user_id):
                break
                
            try:
                updates = await user_client(ImportChatInviteRequest(hash_str))
                
                if updates.chats:
                    joined_chat_id = updates.chats[0].id
                else:
                    raise Exception("Could not resolve Chat ID from the join request updates.")
                
                data["daily_joins"].append(time.time())
                
                # Record analytics: 1 Join
                perf = data.setdefault("link_performance", {}).setdefault(hash_str, {"checks": 0, "joins": 0})
                
                # Absolute non-decaying tracking for user display
                perf["total_joins"] = perf.get("total_joins", int(perf.get("joins", 0))) + 1
                
                perf["joins"] = round(float(perf.get("joins", 0)) + 1, 2)
                
                # RESURRECTION FROM HIBERNATION
                if hash_str in data.get("hibernating_links", []):
                    data["hibernating_links"].remove(hash_str)
                    perf["checks"] = 0 # Reset checks so grade is back to 🆕
                    perf["joins"] = 0
                    await send_alert(user_id, chat_id, f"🎉 **RESURRECTED:** `{link}` was dead but just got traffic! Removing from Hibernation and pushing to Active Queue!", priority="HIGH")
                    
                # Peak Hour AI Recording & Intelligent Decay
                current_hour = str(datetime.now(timezone(timedelta(hours=5, minutes=30))).hour)
                hour_log = data.setdefault("hour_activity_log", {}).setdefault(hash_str, {})
                
                # If total joins recorded exceed 50, decay all hours by 10% to let new patterns take over
                if sum(hour_log.values()) > 50:
                    for h in hour_log:
                        hour_log[h] = round(float(hour_log[h]) * 0.9, 2)
                        
                hour_log[current_hour] = round(float(hour_log.get(current_hour, 0)) + 1, 2)
                
                save_state()
                
                # Step C: The Stay Simulation (Deep Human Emulation)
                if (participants_count and participants_count > 10000) or diff > 5:
                    stay_delay = random.randint(120, 300) # 2-5 mins
                elif (participants_count and participants_count > 1000) or diff > 0:
                    stay_delay = random.randint(30, 60) # 30-60s
                else:
                    stay_delay = random.randint(5, 15) # Dead group, leave fast
                
                # Sleep half the stay duration
                half_delay = stay_delay // 2
                if not await interruptible_sleep(half_delay, user_id):
                    break
                    
                # BANDWIDTH OPTIMIZED: GHOST TYPING EMULATION ONLY
                try:
                    # 50% chance to simulate typing (uses almost zero data)
                    if random.random() > 0.5:
                        async with user_client.action(joined_chat_id, 'typing'):
                            await interruptible_sleep(random.randint(2, 4), user_id)
                except Exception:
                    pass # Ignore read/typing errors, we are just pretending
                    
                # Sleep the remaining duration
                rem_delay = stay_delay - half_delay
                if not await interruptible_sleep(rem_delay, user_id):
                    break
                    
                await user_client.delete_dialog(joined_chat_id)
                
            except UserAlreadyParticipantError:
                await send_alert(user_id, chat_id, f"🧹 Already in `{link}` (likely due to a previous crash). Cleaning up and keeping in queue.", priority="LOW")
                try:
                    # Resolve the chat entity and leave to fix the zombie state
                    invite_info = await user_client(CheckChatInviteRequest(hash_str))
                    if hasattr(invite_info, 'chat'):
                        await user_client.delete_dialog(invite_info.chat.id)
                except Exception:
                    pass
                continue
                
            except FloodWaitError as e:
                # Track flood history for Panic Mode
                now = time.time()
                history = data.setdefault("flood_history", [])
                history.append(now)
                # Prune > 15 mins
                history = [t for t in history if now - t < 900]
                data["flood_history"] = history
                save_state()
                
                if len(history) >= 3:
                    # Trigger PANIC MODE
                    data["panic_mode_until"] = now + 7200 # 2 Hours
                    data["flood_history"] = []
                    save_state()
                    await send_alert(user_id, chat_id, f"🚨 **PANIC MODE ACTIVATED!** Caught 3 API limits in 15 mins. Entire engine is going into Deep Sleep for 2 HOURS to cool down account safety flags.", priority="CRITICAL")
                    if not await interruptible_sleep(10, user_id): break
                    continue
                    
                sleep_time = e.seconds + 30
                await send_alert(user_id, chat_id, f"🚨 **FloodWaitError Caught!** Telegram asked to wait {e.seconds}s. Sleeping for {sleep_time} seconds before resuming...")
                if not await interruptible_sleep(sleep_time, user_id):
                    break
                
            except Exception as e:
                await send_alert(user_id, chat_id, f"❌ **Error during join sequence for `{link}`:** {e}")
                # Don't break, just continue to next link for a long time to prevent tight loop errors
                data.setdefault("link_schedule", {})[hash_str] = time.time() + 3600 # 1 hour
                save_state()
                await interruptible_sleep(10, user_id)
                continue
        else:
            data.setdefault("passive_links_count", 0)
            data["passive_links_count"] += 1
        # Check for IST Night Time (1 AM to 5 AM)
        ist = timezone(timedelta(hours=5, minutes=30))
        now_ist = datetime.now(ist)
        is_night_mode = 1 <= now_ist.hour < 5

        # Determine reschedule delay based on diff and active mode
        
        # Get link grade performance for intelligent scaling
        perf = data.get("link_performance", {}).get(hash_str, {"checks": 0, "joins": 0})
        grade = get_link_grade(perf["checks"], perf["joins"]).split(' ')[0] # 🔥, ⭐, 📈, 📊, 📉, 💀, 🆕
        
        # Smart Grade Multiplier (Better grade = faster checks when idle/night)
        grade_multiplier = 1.0
        if "🔥" in grade: grade_multiplier = 0.6
        elif "⭐" in grade: grade_multiplier = 0.8
        elif "📈" in grade: grade_multiplier = 1.0
        elif "📊" in grade: grade_multiplier = 1.2
        elif "📉" in grade or "💀" in grade: grade_multiplier = 1.5
        
        # AI Peak Hour Multiplier
        current_hour_str = str(now_ist.hour)
        activity_log = data.get("hour_activity_log", {}).get(hash_str, {})
        total_joins_for_link = sum(activity_log.values())
        is_peak_hour = False
        if total_joins_for_link >= 5: # Need enough data to make AI predictions
            hour_joins = activity_log.get(current_hour_str, 0)
            ratio = hour_joins / total_joins_for_link
            if ratio >= 0.2: # Peak hour (>20% of traffic)
                is_peak_hour = True
                grade_multiplier *= 0.4 # Speed up massively
            elif ratio == 0: # Dead hour
                grade_multiplier *= 1.3 # Slow down
                
        # Session Warmup Protocol (3-Day Training Wheels)
        first_login = data.get("first_login_time", 0)
        if first_login > 0 and time.time() - first_login < (3 * 86400):
            grade_multiplier *= 3.0 # Force massive delays during 72-hour warmup

        if participants_count is None:
            next_delay = 3600 # 1 hour for errors
            traffic_str = "❌ Error/Invalid"
        elif "💀" in grade:
            # HIBERNATION PROTOCOL
            next_delay = 86400 # 24 hours
            traffic_str = "💤 Hibernating (Sonar Ping pending)"
            if hash_str not in data.setdefault("hibernating_links", []):
                data["hibernating_links"].append(hash_str)
                await send_alert(user_id, chat_id, f"🥶 **HIBERNATING `{link}`:** Group is dead (Grade F). Auto-Pausing to save engine power. Will send a Sonar Ping tomorrow.")
        elif is_night_mode:
            # Smart Night Mode: Deep sleep, scaled by grade
            base_night = random.randint(3600, 7200) # 1 to 2 hours
            next_delay = int(base_night * grade_multiplier)
            traffic_str = f"🌙 Night Mode ({grade} Smart Delay)"
        else:
            # NEXT-LEVEL AI THROTTLING (Grade-Based Focus)
            if "🔥" in grade: # A+ Viral
                next_delay = random.randint(120, 300) # 2-5 mins
                traffic_str = f"🔥 Viral Focus"
            elif "⭐" in grade: # A Excellent
                next_delay = random.randint(300, 600) # 5-10 mins
                traffic_str = f"⭐ Prime Focus"
            elif "📈" in grade: # B Active
                next_delay = random.randint(600, 900) # 10-15 mins
                traffic_str = f"📈 Active Focus"
            elif "📊" in grade: # C Slow
                next_delay = random.randint(1800, 2700) # 30-45 mins
                traffic_str = f"📊 Slow (Saving API Limits)"
            elif "📉" in grade or "🥱" in grade: # D or E Dead/Spam
                next_delay = random.randint(3600, 10800) # 1-3 hours! Anti-Ban Protection
                traffic_str = f"{grade} Dead Group (Anti-Ban Throttling)"
            else: # 🆕 Init
                next_delay = random.randint(300, 600) # 5-10 mins (Learn quickly)
                traffic_str = f"🆕 Scanning Mode"
                
            # APPLY AI MULTIPLIERS
            next_delay = int(next_delay * grade_multiplier)
            
            if is_peak_hour:
                # Never sleep more than 15 mins during a historical rush hour!
                next_delay = min(next_delay, 900)
                traffic_str += " ⚡ (Rush Hour AI Override)"
                
            # Override for absolute Viral spikes (diff >= 10)
            if is_active_mode and (is_high_traffic or diff >= 10):
                next_delay = random.randint(60, 180) # 1-3 mins MAX Speed
                traffic_str = "🚀 VIRAL SPIKE DETECTED (Max Speed)"

                
        # Ensure session string is always synced with any internal Telethon updates
        user_client = data.get("client")
        if user_client:
            data["session_string"] = user_client.session.save()
            
        data.setdefault("link_schedule", {})[hash_str] = time.time() + next_delay
        save_state()
        
        if participants_count is not None:
            await send_alert(user_id, chat_id, f"📅 **Rescheduled `{link}`:** ({traffic_str}) Next check in {next_delay // 60}m {next_delay % 60}s.", priority="LOW")

        # Minimum global delay to prevent API Anti-Flood Warning from Peeking
        queue_size = len(data["queue"])
        if queue_size < 10:
            anti_flood_delay = random.randint(45, 60) # Force slow loop for tiny queues
        elif queue_size < 50:
            anti_flood_delay = random.randint(30, 45)
        else:
            anti_flood_delay = random.randint(25, 35) # Fast loop for huge queues
            
        if not await interruptible_sleep(anti_flood_delay, user_id):
            break

# ==========================================
# MAIN EXECUTION & DUMMY SERVER
# ==========================================

async def handle_ping(request):
    return web.Response(text="Bot is running!")

# ==========================================
# TELEGRAM BUSINESS AUTO-RESPONDER
# ==========================================
BUSINESS_CONN_CACHE = {}

import re
def get_utf16_length(s):
    return len(s.encode('utf-16-le')) // 2

def process_manual_emojis(text, entities):
    pattern = re.compile(r'\[emoji:(\d+):([^\]]+)\]')
    matches = list(pattern.finditer(text))
    if not matches:
        return text, entities
        
    new_text = ""
    new_entities = []
    
    last_idx = 0
    utf16_offset_old = 0
    utf16_offset_new = 0
    
    shifts = []
    
    for match in matches:
        doc_id = int(match.group(1))
        base_char = match.group(2)
        
        prefix = text[last_idx:match.start()]
        new_text += prefix
        
        utf16_offset_old += get_utf16_length(prefix)
        utf16_offset_new += get_utf16_length(prefix)
        
        tag_len = get_utf16_length(match.group(0))
        base_len = get_utf16_length(base_char)
        
        new_entities.append(types.MessageEntityCustomEmoji(
            offset=utf16_offset_new,
            length=base_len,
            document_id=doc_id
        ))
        
        new_text += base_char
        utf16_offset_new += base_len
        utf16_offset_old += tag_len
        
        shrink = tag_len - base_len
        shifts.append((utf16_offset_old, shrink))
        
        last_idx = match.end()
        
    new_text += text[last_idx:]
    
    if entities:
        for ent in entities:
            # Shift existing entities
            d = ent.to_dict() if hasattr(ent, 'to_dict') else ent
            old_off = d['offset']
            total_shrink = sum(s[1] for s in shifts if s[0] <= old_off)
            d['offset'] -= total_shrink
            
            cls_name = d.get('_')
            if cls_name:
                cls = getattr(types, cls_name)
                d_copy = {k:v for k,v in d.items() if k != '_'}
                new_entities.append(cls(**d_copy))
            else:
                new_entities.append(ent)
                
    return new_text, new_entities

def rebuild_entities(reply_obj):
    if not isinstance(reply_obj, dict):
        return reply_obj, []
        
    text = reply_obj.get("text", "")
    ents = reply_obj.get("entities", [])
    
    rebuilt = []
    for ent in ents:
        try:
            cls_name = ent.get("_")
            if not cls_name: continue
            cls = getattr(types, cls_name)
            d_copy = {k:v for k,v in ent.items() if k != "_"}
            rebuilt.append(cls(**d_copy))
        except Exception as e:
            logger.error(f"Failed to rebuild entity: {e}")
            
    return text, rebuilt

@bot_client.on(events.Raw(types.UpdateBotNewBusinessMessage))
async def business_message_handler(event):
    conn_id = event.connection_id
    msg = getattr(event, 'message', None)
    
    logger.info(f"Received Business Msg! conn_id: {conn_id}, msg: {msg is not None}")
    
    if not msg:
        return
        
    peer = getattr(msg, 'peer_id', None)
    from_id = getattr(msg, 'from_id', None)
    logger.info(f"Business Msg Details - peer: {peer}, from_id: {from_id}, out: {getattr(msg, 'out', False)}")
        
    # We only auto-reply to private DMs, not groups
    if not isinstance(peer, types.PeerUser):
        logger.info("Not a PeerUser, ignoring.")
        return
        
    # Find which user owns this connection
    user_id = BUSINESS_CONN_CACHE.get(conn_id)
    if not user_id:
        try:
            logger.info("Fetching business connection info from Telegram...")
            conn_info = await bot_client(GetBotBusinessConnectionRequest(connection_id=conn_id))
            
            # conn_info is an Updates object containing the connection update
            for u in getattr(conn_info, 'updates', []):
                if hasattr(u, 'connection') and hasattr(u.connection, 'user_id'):
                    user_id = u.connection.user_id
                    break
                    
            if not user_id:
                logger.error(f"Could not find user_id in business connection updates for {conn_id}")
                return
                
            BUSINESS_CONN_CACHE[conn_id] = user_id
        except Exception as e:
            logger.error(f"Failed to fetch business connection: {e}")
            return
            
    data = get_user_data(user_id)
    
    # In business messages, the sender is usually msg.from_id.user_id if present, else msg.peer_id.user_id
    sender_id = None
    if isinstance(from_id, types.PeerUser):
        sender_id = from_id.user_id
    elif isinstance(peer, types.PeerUser):
        sender_id = peer.user_id
        
    logger.info(f"Determined Sender ID: {sender_id}")
    if not sender_id:
        return
        
    # Don't reply to yourself!
    if getattr(msg, 'out', False) or sender_id == user_id:
        logger.info("Ignoring outgoing message or message sent to self.")
        return
        
    now = time.time()
    
    # -----------------------------
    # 1. KEYWORD REPLY CHECK
    # -----------------------------
    keyword_replies = data.get("business_keyword_replies", {})
    matched_reply = None
    msg_text = getattr(msg, 'message', '').lower()
    
    for kw, kw_reply in keyword_replies.items():
        # Check if keyword exists as a whole word, or just substring. Substring is simpler for now.
        if kw.lower() in msg_text:
            matched_reply = kw_reply
            break
            
    if matched_reply:
        # Keyword replies bypass the 24-hour limit!
        # But we enforce a tiny 5-second anti-loop cooldown
        kw_cache = data.setdefault("business_kw_throttle", {})
        if now - kw_cache.get(str(sender_id), 0) < 5:
            return # Too fast!
            
        try:
            input_peer = await bot_client.get_input_entity(sender_id)
            kw_text, kw_ents = rebuild_entities(matched_reply)
            await bot_client(InvokeWithBusinessConnectionRequest(
                connection_id=conn_id,
                query=SendMessageRequest(
                    peer=input_peer,
                    message=kw_text,
                    entities=kw_ents,
                    random_id=random.randint(-2**63, 2**63 - 1)
                )
            ))
            kw_cache[str(sender_id)] = now
            logger.info(f"Sent Keyword Auto-Reply to sender {sender_id}")
        except Exception as e:
            logger.error(f"Failed to send keyword auto-reply: {e}")
        return

    # -----------------------------
    # 2. DEFAULT 24-HOUR REPLY CHECK
    # -----------------------------
    reply_text = data.get("business_auto_reply")
    logger.info(f"Business Owner ID: {user_id}, Reply Text Set: {reply_text is not None}")
    if not reply_text:
        return
        
    # Clean up old anti-spam entries (older than 24h)
    replied_cache = data.setdefault("business_replied_users", {})
    keys_to_delete = [k for k, v in replied_cache.items() if now - v > 86400]
    for k in keys_to_delete:
        del replied_cache[k]
        
    if str(sender_id) in replied_cache:
        logger.info(f"Already replied to {sender_id} within 24h. Ignoring.")
        return # Already replied to this person today
        
    try:
        logger.info(f"Sending business reply to sender {sender_id} for connection {conn_id}...")
        
        try:
            input_peer = await bot_client.get_input_entity(sender_id)
        except ValueError:
            logger.error(f"Could not resolve InputPeer for sender {sender_id}")
            return
            
        def_text, def_ents = rebuild_entities(reply_text)
            
        # Send the auto reply natively through MTProto
        await bot_client(InvokeWithBusinessConnectionRequest(
            connection_id=conn_id,
            query=SendMessageRequest(
                peer=input_peer,
                message=def_text,
                entities=def_ents,
                random_id=random.randint(-2**63, 2**63 - 1)
            )
        ))
        # Mark as replied
        replied_cache[str(sender_id)] = now
        instant_save_state() # Save cache
        
        logger.info(f"Sent Business Auto-Reply for user {user_id} to sender {sender_id}")
    except Exception as e:
        logger.error(f"Failed to send business auto-reply: {e}")

async def start_web_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle_ping)
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get("PORT", 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"Web server started on port {port} for Render/UptimeRobot.")
    except Exception as e:
        logger.error(f"Web server failed to start: {e}")

async def main():
    if API_ID == 0:
        logger.error("API_ID must be set before running the script!")
        print("ERROR: Please edit the script or set the environment variables for API_ID.")
        return

    logger.info("Starting Bot Client...")
    await start_web_server()
    await bot_client.start(bot_token=BOT_TOKEN)
    
    # Start the periodic DB Saver loop to conserve bandwidth
    asyncio.create_task(_db_saver_loop())
    
    await load_state()
    logger.info(f"Loaded state for {len(user_data)} users.")
    for uid, data in user_data.items():
        if data.get("loop_active"):
            if data["client"] is None and data.get("session_string"):
                try:
                    kwargs = get_client_kwargs(data)
                    client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, **kwargs)
                    await client.connect()
                    try: await client.get_me() # Sync AuthKey
                    except: pass
                    
                    if await client.is_user_authorized():
                        data["client"] = client
                    else:
                        await client.disconnect()
                        save_state()
                except Exception as e:
                    logger.error(f"Failed to resume session for {uid}: {e}")
                    
            logger.info(f"Resuming background loop for user {uid}")
            data["task"] = asyncio.create_task(runner_engine(uid, uid))
    
    # Set the Telegram Bot Menu Commands
    await bot_client(SetBotCommandsRequest(
        scope=BotCommandScopeDefault(),
        lang_code='',
        commands=[
            BotCommand(command="start", description="Open Dashboard"),
            BotCommand(command="login", description="Login to your personal account"),
            BotCommand(command="help", description="Show the help menu"),
            BotCommand(command="cancel", description="Cancel current action")
        ]
    ))
    
    logger.info("Public Multi-User Bot Online! Send /start to the bot.")
    
    await bot_client.run_until_disconnected()

if __name__ == '__main__':
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("System stopped by user.")
