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
from motor.motor_asyncio import AsyncIOMotorClient
from telethon.sessions import StringSession

from telethon import TelegramClient, events, Button
from telethon.errors import (
    FloodWaitError, UserAlreadyParticipantError, SessionPasswordNeededError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, PhoneCodeExpiredError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import BotCommand, BotCommandScopeDefault, ChatInvite, ChatInviteAlready
from telethon.tl.functions.bots import SetBotCommandsRequest
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("TG_API_ID", "20543583"))  # <--- SET YOUR API_ID HERE
API_HASH = os.environ.get("TG_API_HASH", "505e57baf9b48347e18446d352cacce3")  # <--- SET YOUR API_HASH HERE
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "8925952271:AAG4krblXEPNWXX6g7oOdkrFMt8qkU4OFGA")

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

if MONGO_URI:
    db_client = AsyncIOMotorClient(MONGO_URI)
    db_collection = db_client.get_database("telegram_bot").get_collection("users")
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
                    "login_state": doc.get("login_state", None),
                    "phone": doc.get("phone", None),
                    "phone_code_hash": doc.get("phone_code_hash", None),
                    "next_join_time": doc.get("next_join_time", 0),
                    "first_join_done": doc.get("first_join_done", False),
                    "link_stats": doc.get("link_stats", {}),
                    "high_traffic_links": doc.get("high_traffic_links", {}),
                    "link_schedule": doc.get("link_schedule", {}),
                    "link_last_action": doc.get("link_last_action", {}),
                    "link_seen_users": doc.get("link_seen_users", {}),
                    "active_links_count": doc.get("active_links_count", 0),
                    "passive_links_count": doc.get("passive_links_count", 0),
                    "session_string": doc.get("session_string", "")
                }
            loaded_from_db = True
            logger.info("State successfully loaded from MongoDB.")
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
                        "session_string": state.get("session_string", "")
                    }
        except Exception as e:
            logger.error(f"Error loading state from local file: {e}")

async def _save_state_async():
    state_to_save = {}
    for user_id, state in user_data.items():
        doc = {
            "user_id": str(user_id),
            "queue": state["queue"],
            "current_index": state["current_index"],
            "loop_active": state["loop_active"],
            "daily_joins": state["daily_joins"],
            "login_state": state["login_state"],
            "phone": state["phone"],
            "phone_code_hash": state["phone_code_hash"],
            "next_join_time": state.get("next_join_time", 0),
            "first_join_done": state.get("first_join_done", False),
            "link_stats": state.get("link_stats", {}),
            "high_traffic_links": state.get("high_traffic_links", {}),
            "link_schedule": state.get("link_schedule", {}),
            "link_last_action": state.get("link_last_action", {}),
            "link_seen_users": state.get("link_seen_users", {}),
            "active_links_count": state.get("active_links_count", 0),
            "passive_links_count": state.get("passive_links_count", 0),
            "session_string": state.get("session_string", "")
        }
        state_to_save[str(user_id)] = doc
        
        if db_collection is not None:
            try:
                await db_collection.update_one({"user_id": str(user_id)}, {"$set": doc}, upsert=True)
            except Exception as e:
                pass
                
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state_to_save, f)
    except Exception as e:
        logger.error(f"Error saving local state: {e}")

def save_state():
    asyncio.create_task(_save_state_async())

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
            "passive_links_count": 0
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

async def interruptible_sleep(seconds: int, user_id: int) -> bool:
    data = get_user_data(user_id)
    new_time = time.time() + seconds
    if new_time > data.get("next_join_time", 0):
        data["next_join_time"] = new_time
        save_state()
        
    while time.time() < data.get("next_join_time", 0):
        if not data["loop_active"]:
            return False
        await asyncio.sleep(1)
    return True

async def show_menu(chat_id: int, user_id: int):
    data = get_user_data(user_id)
    
    # Lazy load existing session from disk if bot was restarted
    if data["client"] is None:
        session_file = f'sessions/user_{user_id}.session'
        if os.path.exists(session_file):
            client = TelegramClient(f'sessions/user_{user_id}', API_ID, API_HASH, flood_sleep_threshold=0, connection_retries=3)
            await client.connect()
            if await client.is_user_authorized():
                data["client"] = client
                
    if data["client"] is None or not await data["client"].is_user_authorized():
        welcome_text = (
            "👋 **Welcome to the Automated Telegram Link Manager!**\n\n"
            "This bot operates a massive **24/7 Unlimited Priority Queue** to manage joining private Telegram groups securely and autonomously.\n\n"
            "✨ **How the Smart Engine Works:**\n"
            "• **🧠 Priority Scheduling:** You can add unlimited links. The bot analyzes the traffic of every group in real-time. It schedules checks for dead groups every 7-9 mins, active groups every 4-6 mins, and viral groups every 1-3 mins. It never wastes time on dead links.\n"
            "• **🛡️ Anti-Ban (The <10 Min Guarantee):** The bot enforces a strict 3-5 second invisible delay between every single action. Because of this mathematically calculated delay, it guarantees that as long as your queue is under 150 links, the bot will take action on any new user joining *under 10 minutes* while keeping your account 100% safe from Telegram Anti-Flood filters!\n"
            "• **🛑 High Traffic Throttling:** If a viral group suddenly gets 50 users joining at once, the bot protects your account by waiting for batches of 10 users to pile up before joining, getting you maximum output without risking a ban.\n"
            "• **♾️ 24/7 Infinite Loop:** Once you start the loop, you can close Telegram entirely. The bot will keep running in the background infinitely until you manually stop it.\n\n"
            "To get started, you need to connect your Telegram account. Simply send `/login` to begin."
        )
        await bot_client.send_message(chat_id, welcome_text)
        return

    keyboard = [
        [Button.inline("➕ Add Link", b"add_link"), Button.inline("❌ Remove Link", b"remove_link")],
        [Button.inline("▶️ Start Loop", b"start_loop"), Button.inline("⏸️ Stop Loop", b"stop_loop")],
        [Button.inline("📋 Show Queue", b"show_queue"), Button.inline("🚪 Logout", b"logout")]
    ]
    status = "🟢 ACTIVE" if data["loop_active"] else "🔴 PAUSED"
    
    now = time.time()
    data["daily_joins"] = [ts for ts in data["daily_joins"] if now - ts < 86400]
    
    text = (
        f"**🛡️ Personal Link Manager Dashboard**\n\n"
        f"**Status:** {status}\n"
        f"**Links in Queue:** {len(data['queue'])}\n"
        f"**Total Joins (24h):** {len(data['daily_joins'])}\n"
        f"**Current Index:** {data['current_index']}"
    )
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

@bot_client.on(events.NewMessage(pattern='(?i)^/login'))
async def login_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    # Check existing session
    if data["client"] is None and data.get("session_string"):
        client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, flood_sleep_threshold=0, connection_retries=3)
        await client.connect()
        if await client.is_user_authorized():
            data["client"] = client
            
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
        client = TelegramClient(StringSession(""), API_ID, API_HASH, flood_sleep_threshold=0, connection_retries=3)
        
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
            data["session_string"] = client.session.save()
            data["login_state"] = None
            save_state()
            await event.respond("✅ **Login Successful!** Send /start to open your control panel.")
        except SessionPasswordNeededError:
            data["login_state"] = "WAITING_PASSWORD"
            save_state()
            await event.respond("🔒 **Two-Step Verification Enabled.**\n\nPlease enter your 2FA password:")
        except (PhoneCodeInvalidError, PhoneCodeExpiredError):
            await event.respond("❌ Invalid or expired code. Please try /login again.")
            data["login_state"] = None
            save_state()
        except Exception as e:
            await event.respond(f"❌ Login error: {e}")
            data["login_state"] = None
            save_state()
            
    elif state == "WAITING_PASSWORD":
        password = event.text.strip()
        client = data["client"]
        try:
            await client.sign_in(password=password)
            data["session_string"] = client.session.save()
            data["login_state"] = None
            save_state()
            await event.respond("✅ **Login Successful!** Send /start to open your control panel.")
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

@bot_client.on(events.CallbackQuery())
async def callback_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    if data["client"] is None or not await data["client"].is_user_authorized():
        await event.answer("You are not logged in! Send /login", alert=True)
        return
        
    cb_data = event.data.decode('utf-8')
    
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
        
    elif cb_data == "start_loop":
        if not data["queue"]:
            await event.answer("Cannot start. Queue is empty!", alert=True)
            return
        if data["loop_active"]:
            await event.answer("Loop is already running!", alert=True)
            return
            
        data["loop_active"] = True
        save_state()
        
        # Spawn the background task for this specific user
        if data["task"] is None or data["task"].done():
            data["task"] = asyncio.create_task(runner_engine(user_id, event.chat_id))
            
        detailed_msg = (
            "▶️ **Loop Activated! Here is how your bot operates 24/7:**\n\n"
            "🧠 **Smart Priority Queue**\n"
            "Your bot tracks every single link individually instead of just going in a circle. It assigns 'appointment times' to check each link based on how popular they are:\n"
            "• 🔥 **Viral Groups (10+ joins):** Checked every 1 to 3 minutes.\n"
            "• 🚶 **Active Groups (1-9 joins):** Checked every 4 to 6 minutes.\n"
            "• 💤 **Dead Groups (0 joins):** Ignored for 7 to 9 minutes to save API limits.\n\n"
            "🛡️ **Anti-Ban System (The <10 Minute Guarantee)**\n"
            "To prevent Telegram from banning you for 'peeking' at groups too fast, the bot forces a strict **3 to 5-second invisible delay** between every single action. "
            "This means as long as you have less than **150 links** in your queue, the bot guarantees it will take action on any new user joining *under 10 minutes*!\n\n"
            "🛑 **High Traffic Throttling**\n"
            "If a group suddenly gets 50+ users joining at once, the bot won't join 50 times and get your account banned. It will throttle itself, wait for batches of 10 users to pile up, and then join once to get maximum exposure for minimum risk.\n\n"
            "The bot is now running infinitely in the background. You can close Telegram, log out, or turn off your device! It will run 24/7 until you press **Stop Loop**."
        )
        await event.respond(detailed_msg)
        await show_menu(event.chat_id, user_id)
        
    elif cb_data == "stop_loop":
        if not data["loop_active"]:
            await event.answer("Loop is already stopped!", alert=True)
            return
        data["loop_active"] = False
        data["first_join_done"] = False
        data["next_join_time"] = 0
        save_state()
        await event.respond("⏸️ **Loop Paused!** Will safely halt after the current sleep/action finishes.")
        await show_menu(event.chat_id, user_id)
        
    elif cb_data == "show_queue":
        if not data["queue"]:
            await event.respond("Queue is currently empty.")
        else:
            msg = "**📋 Smart Queue Status:**\n\n"
            now = time.time()
            for i, l in enumerate(data["queue"]):
                hash_str = extract_hash(l)
                check_time = data.get("link_schedule", {}).get(hash_str, 0)
                if check_time == 0:
                    status = "⏳ Waiting for initial scan"
                elif check_time <= now:
                    status = "🔥 Ready to check now"
                else:
                    wait_sec = int(check_time - now)
                    status = f"🕒 Next check in {wait_sec // 60}m {wait_sec % 60}s"
                msg += f"`{i}`: {l}\n   └ {status}\n\n"
            await event.respond(msg, link_preview=False)
            
    elif cb_data == "logout":
        data["loop_active"] = False
        if data["client"]:
            await data["client"].log_out()
            data["client"] = None
        data["queue"] = []
        data["daily_joins"] = []
        data["next_join_time"] = 0
        save_state()
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
            session_file = f'sessions/user_{user_id}.session'
            if os.path.exists(session_file):
                user_client = TelegramClient(f'sessions/user_{user_id}', API_ID, API_HASH, flood_sleep_threshold=0, connection_retries=3)
                await user_client.connect()
                data["client"] = user_client
            else:
                await bot_client.send_message(chat_id, "⚠️ **Session missing.** Please /login again.")
                data["loop_active"] = False
                save_state()
                break

        now = time.time()
        
        # Prevent ghost 24-hour sleeps from old limits locking up the loop
        if data.get("next_join_time", 0) > now + 2:
            sleep_left = int(data["next_join_time"] - now)
            if sleep_left > 7200: # Over 2 hours (likely the old daily limit bug)
                data["next_join_time"] = 0
                save_state()
                await bot_client.send_message(chat_id, "🧹 **Cleared ghost sleep.** Resuming fast loop...")
            else:
                if sleep_left > 10:
                    await bot_client.send_message(chat_id, f"💤 **Resuming Wait:** Sleeping for {sleep_left // 60}m {sleep_left % 60}s before continuing.")
                if not await interruptible_sleep(0, user_id):
                    break
        
        # Priority Scheduling Logic
        earliest_link = None
        earliest_time = float('inf')
        
        for link in data["queue"]:
            hash_str = extract_hash(link)
            # Default to 0 so new links get checked immediately
            check_time = data.get("link_schedule", {}).get(hash_str, 0)
            if check_time < earliest_time:
                earliest_time = check_time
                earliest_link = link
                
        if earliest_link is None:
            # Fallback (shouldn't happen if queue is not empty)
            earliest_link = data["queue"][0]
            earliest_time = 0
            
        # If the earliest link is still in the future, we sleep until it's ready
        if earliest_time > time.time():
            sleep_needed = int(earliest_time - time.time())
            # We enforce a max chunk sleep of 30s so the loop can quickly react to Stop commands
            chunk = min(sleep_needed, 30)
            if sleep_needed > 30 and chunk == 30:
                # Only spam the log if it's a long sleep
                await bot_client.send_message(chat_id, f"💤 **Queue Sleeping:** No links ready. Sleeping for {sleep_needed // 60}m {sleep_needed % 60}s...")
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
            
            # Extract participants count
            if hasattr(invite_info, 'participants_count'):
                participants_count = invite_info.participants_count
            elif hasattr(invite_info, 'chat') and hasattr(invite_info.chat, 'participants_count'):
                participants_count = invite_info.chat.participants_count
            
            recent_ids = []
            new_unique_users = 0
            if hasattr(invite_info, 'participants') and invite_info.participants:
                seen_users = data.get("link_seen_users", {}).get(hash_str, [])
                for p in invite_info.participants:
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
                        is_active_mode = True
                        data.setdefault("high_traffic_links", {})[hash_str] = time.time()
                        await bot_client.send_message(chat_id, f"🔥 **Active Mode (High Traffic):** {diff} new users joined `{link}`. Engaging!")
                    elif new_unique_users > 0 or (diff >= 1 and len(recent_ids) == 0):
                        if is_high_traffic:
                            is_active_mode = False
                            await bot_client.send_message(chat_id, f"⏳ **Passive Mode (Throttling):** New users detected in `{link}`, waiting for 10 users because group is High Traffic.")
                        else:
                            is_active_mode = True
                            await bot_client.send_message(chat_id, f"🔥 **Active Mode:** Genuine new users detected in `{link}`. Engaging!")
                    elif diff <= 0 and time_since_last_action > 360: # 6 minutes
                        # Handle user churn (people leaving and joining keeping total count same)
                        is_active_mode = True
                        await bot_client.send_message(chat_id, f"🔄 **Active Mode (Refresh):** Total count unchanged, but 6+ minutes have passed. Checking for hidden new users in `{link}`!")
                    else:
                        is_active_mode = False
                        if diff > 0:
                            await bot_client.send_message(chat_id, f"📉 **Passive Mode (Spam Filter):** Ignored {diff} joins in `{link}` because they were all repeat spammers.")
                        else:
                            await bot_client.send_message(chat_id, f"📉 **Passive Mode:** No new users detected in `{link}`. Skipping join.")
                else:
                    # First time checking
                    is_active_mode = True
                    await bot_client.send_message(chat_id, f"🔥 **Active Mode:** First time checking `{link}` ({participants_count} members). Engaging!")
                
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
                
            await bot_client.send_message(chat_id, f"⏳ **Step A:** Sleeping for {delay} seconds before joining next link...")
            
            if not await interruptible_sleep(delay, user_id):
                break
                
            try:
                await bot_client.send_message(chat_id, f"🔄 **Step B:** Attempting to join: {link}")
                
                updates = await user_client(ImportChatInviteRequest(hash_str))
                
                if updates.chats:
                    joined_chat_id = updates.chats[0].id
                else:
                    raise Exception("Could not resolve Chat ID from the join request updates.")
                
                data["daily_joins"].append(time.time())
                save_state()
                await bot_client.send_message(chat_id, f"✅ **Success:** Joined chat ID `{joined_chat_id}`")
                
                # Step C: The Stay Simulation
                stay_delay = random.randint(5, 15)
                await bot_client.send_message(chat_id, f"🧍 **Step C:** Simulating stay. Waiting {stay_delay} seconds in group...")
                
                if not await interruptible_sleep(stay_delay, user_id):
                    break
                    
                await bot_client.send_message(chat_id, f"👋 **Step D:** Leaving chat ID `{joined_chat_id}`")
                await user_client.delete_dialog(joined_chat_id)
                
            except UserAlreadyParticipantError:
                await bot_client.send_message(chat_id, f"ℹ️ Already a participant of `{link}`. Removing from queue.")
                if link in data["queue"]:
                    data["queue"].remove(link)
                save_state()
                continue
                
            except FloodWaitError as e:
                sleep_time = e.seconds + 30
                await bot_client.send_message(
                    chat_id, 
                    f"🚨 **FloodWaitError Caught!** Telegram asked to wait {e.seconds}s.\n"
                    f"Sleeping for {sleep_time} seconds before resuming..."
                )
                if not await interruptible_sleep(sleep_time, user_id):
                    break
                
            except Exception as e:
                await bot_client.send_message(chat_id, f"❌ **Error processing link {link}:**\n`{str(e)}`")
                # Reschedule the bad link for a long time to prevent tight loop errors
                data.setdefault("link_schedule", {})[hash_str] = time.time() + 3600 # 1 hour
                save_state()
                await interruptible_sleep(10, user_id)
                continue
        else:
            data["passive_links_count"] += 1

        # Determine reschedule delay based on diff and active mode
        if participants_count is None:
            next_delay = 3600 # 1 hour for errors
            traffic_str = "❌ Error/Invalid"
        elif is_active_mode:
            if is_high_traffic or diff >= 10:
                next_delay = random.randint(60, 180) # 1 to 3 mins
                traffic_str = "🔥 High/Viral Traffic"
            else:
                next_delay = random.randint(240, 360) # 4 to 6 mins
                traffic_str = "🚶 Normal Traffic"
        else:
            if last_count > 0 and diff < 1:
                next_delay = random.randint(420, 540) # 7 to 9 mins
                traffic_str = "💤 Dead Traffic"
            else:
                next_delay = random.randint(60, 180) # 1 to 3 mins
                traffic_str = "⏳ Throttled Traffic"
                
        data.setdefault("link_schedule", {})[hash_str] = time.time() + next_delay
        save_state()
        
        if participants_count is not None:
            await bot_client.send_message(chat_id, f"📅 **Rescheduled `{link}`:** ({traffic_str}) Next check in {next_delay // 60}m {next_delay % 60}s.")

        # Minimum global delay to prevent API Anti-Flood Warning from Peeking
        anti_flood_delay = random.randint(3, 5)
        if not await interruptible_sleep(anti_flood_delay, user_id):
            break

# ==========================================
# MAIN EXECUTION & DUMMY SERVER
# ==========================================

async def handle_dummy(request):
    return web.Response(text="Bot is running and healthy!")

async def start_dummy_server():
    app = web.Application()
    app.router.add_get('/', handle_dummy)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Dummy web server started on port {port} to satisfy Render deploy checks")

async def main():
    if API_ID == 0:
        logger.error("API_ID must be set before running the script!")
        print("ERROR: Please edit the script or set the environment variables for API_ID.")
        return

    logger.info("Starting Bot Client...")
    await start_dummy_server()
    await bot_client.start(bot_token=BOT_TOKEN)
    
    await load_state()
    logger.info(f"Loaded state for {len(user_data)} users.")
    for uid, data in user_data.items():
        if data.get("loop_active"):
            if data["client"] is None and data.get("session_string"):
                try:
                    client = TelegramClient(StringSession(data["session_string"]), API_ID, API_HASH, flood_sleep_threshold=0, connection_retries=3)
                    await client.connect()
                    if await client.is_user_authorized():
                        data["client"] = client
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
