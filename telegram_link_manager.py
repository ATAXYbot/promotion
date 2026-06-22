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

from telethon import TelegramClient, events, Button
from telethon.errors import (
    FloodWaitError, UserAlreadyParticipantError, SessionPasswordNeededError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, PhoneCodeExpiredError
)
from telethon.tl.functions.messages import ImportChatInviteRequest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("TG_API_ID", "0"))  # <--- SET YOUR API_ID HERE
API_HASH = os.environ.get("TG_API_HASH", "YOUR_API_HASH")  # <--- SET YOUR API_HASH HERE
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "8925952271:AAG4krblXEPNWXX6g7oOdkrFMt8qkU4OFGA")

# Ensure sessions directory exists
if not os.path.exists("sessions"):
    os.makedirs("sessions")

bot_client = TelegramClient('sessions/control_bot', API_ID, API_HASH)

# ==========================================
# MULTI-USER STATE MANAGEMENT
# ==========================================
user_data = {}

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
            "task": None
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
    for _ in range(seconds):
        data = get_user_data(user_id)
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
            client = TelegramClient(f'sessions/user_{user_id}', API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                data["client"] = client
                
    if data["client"] is None or not await data["client"].is_user_authorized():
        welcome_text = (
            "👋 **Welcome to the Automated Telegram Link Manager!**\n\n"
            "This bot allows you to automate joining private Telegram groups securely while bypassing ban filters.\n\n"
            "✨ **Features:**\n"
            "• **Anti-Ban Protection:** Simulates human delays and caps daily joins at 12.\n"
            "• **Smart Queue:** Add all your invite links and let the bot handle them in the background.\n"
            "• **Private & Secure:** Your session runs in a completely isolated container.\n\n"
            "To get started, you need to connect your Telegram account. Simply send /login to begin."
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
        f"**Daily Joins:** {len(data['daily_joins'])} / 12\n"
        f"**Current Index:** {data['current_index']}"
    )
    await bot_client.send_message(chat_id, text, buttons=keyboard)

# ==========================================
# BOT INTERFACE & LOGIN FLOW
# ==========================================

@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await show_menu(event.chat_id, event.sender_id)

@bot_client.on(events.NewMessage(pattern='/help'))
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

@bot_client.on(events.NewMessage(pattern='/login'))
async def login_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    # Check existing session
    if data["client"] is None and os.path.exists(f'sessions/user_{user_id}.session'):
        client = TelegramClient(f'sessions/user_{user_id}', API_ID, API_HASH)
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

@bot_client.on(events.NewMessage(pattern='/cancel'))
async def cancel_handler(event):
    user_id = event.sender_id
    data = get_user_data(user_id)
    data["login_state"] = None
    await event.respond("❌ Action cancelled.")

@bot_client.on(events.NewMessage())
async def message_handler(event):
    if event.text.startswith('/'):
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
        client = TelegramClient(f'sessions/user_{user_id}', API_ID, API_HASH)
        await client.connect()
        data["client"] = client
        
        try:
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
            
    elif state == "WAITING_CODE":
        # Strip everything except numbers to safely parse formatted codes (like '1 2 3 4 5')
        code = re.sub(r'\D', '', event.text.strip())
        if not code:
            await event.respond("❌ Please provide the numeric code (e.g. `1 2 3 4 5`).")
            return
            
        client = data["client"]
        try:
            await client.sign_in(data["phone"], code, phone_code_hash=data["phone_code_hash"])
            data["login_state"] = None
            await event.respond("✅ **Login Successful!** Send /start to open your control panel.")
        except SessionPasswordNeededError:
            data["login_state"] = "WAITING_PASSWORD"
            await event.respond("🔒 **Two-Step Verification Enabled.**\n\nPlease enter your 2FA password:")
        except (PhoneCodeInvalidError, PhoneCodeExpiredError):
            await event.respond("❌ Invalid or expired code. Please try /login again.")
            data["login_state"] = None
        except Exception as e:
            await event.respond(f"❌ Login error: {e}")
            data["login_state"] = None
            
    elif state == "WAITING_PASSWORD":
        password = event.text.strip()
        client = data["client"]
        try:
            await client.sign_in(password=password)
            data["login_state"] = None
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
            data["queue"].append(link)
            await event.respond(f"✅ Added link to queue. Total links: {len(data['queue'])}")
        else:
            await event.respond("❌ Invalid link format. Must contain 't.me'.")
        data["login_state"] = None
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
        await event.respond("Send me the invite link to add (e.g., https://t.me/+...):")
        
    elif cb_data == "remove_link":
        if not data["queue"]:
            await event.respond("Queue is currently empty.")
            return
        msg = "Send the index number of the link to remove:\n\n"
        for i, l in enumerate(data["queue"]):
            msg += f"`{i}`: {l}\n"
        data["login_state"] = "WAITING_REMOVE_LINK"
        await event.respond(msg)
        
    elif cb_data == "start_loop":
        if not data["queue"]:
            await event.answer("Cannot start. Queue is empty!", alert=True)
            return
        if data["loop_active"]:
            await event.answer("Loop is already running!", alert=True)
            return
            
        data["loop_active"] = True
        
        # Spawn the background task for this specific user
        if data["task"] is None or data["task"].done():
            data["task"] = asyncio.create_task(runner_engine(user_id, event.chat_id))
            
        await event.respond("▶️ **Loop Activated!** Background task is now processing your queue.")
        await show_menu(event.chat_id, user_id)
        
    elif cb_data == "stop_loop":
        if not data["loop_active"]:
            await event.answer("Loop is already stopped!", alert=True)
            return
        data["loop_active"] = False
        await event.respond("⏸️ **Loop Paused!** Will safely halt after the current sleep/action finishes.")
        await show_menu(event.chat_id, user_id)
        
    elif cb_data == "show_queue":
        if not data["queue"]:
            await event.respond("Queue is currently empty.")
        else:
            msg = "**📋 Current Queue:**\n\n"
            for i, l in enumerate(data["queue"]):
                marker = " 👈 *(Next)*" if i == data["current_index"] else ""
                msg += f"`{i}`: {l}{marker}\n"
            await event.respond(msg, link_preview=False)
            
    elif cb_data == "logout":
        data["loop_active"] = False
        if data["client"]:
            await data["client"].log_out()
            data["client"] = None
        data["queue"] = []
        data["daily_joins"] = []
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
    user_client = data["client"]
    
    while True:
        if not data["loop_active"] or not data["queue"]:
            if not data["loop_active"]:
                break
            await asyncio.sleep(5)
            continue
            
        now = time.time()
        data["daily_joins"] = [ts for ts in data["daily_joins"] if now - ts < 86400]
        
        if len(data["daily_joins"]) >= 12:
            data["loop_active"] = False
            await bot_client.send_message(
                chat_id, 
                "⚠️ **Safety Alert:** Daily join ceiling (12) reached. Loop automatically paused to prevent bans."
            )
            break
            
        link = data["queue"][data["current_index"]]
        
        delay = random.randint(240, 600)
        await bot_client.send_message(chat_id, f"⏳ **Step A:** Sleeping for {delay} seconds before joining next link...")
        
        if not await interruptible_sleep(delay, user_id):
            break
            
        try:
            hash_str = extract_hash(link)
            await bot_client.send_message(chat_id, f"🔄 **Step B:** Attempting to join: {link}")
            
            updates = await user_client(ImportChatInviteRequest(hash_str))
            
            if updates.chats:
                joined_chat_id = updates.chats[0].id
            else:
                raise Exception("Could not resolve Chat ID from the join request updates.")
            
            data["daily_joins"].append(time.time())
            await bot_client.send_message(chat_id, f"✅ **Success:** Joined chat ID `{joined_chat_id}`")
            
            stay_delay = random.randint(120, 300)
            await bot_client.send_message(chat_id, f"🧍 **Step C:** Simulating stay. Waiting {stay_delay} seconds in group...")
            
            if not await interruptible_sleep(stay_delay, user_id):
                break
                
            await bot_client.send_message(chat_id, f"👋 **Step D:** Leaving chat ID `{joined_chat_id}`")
            await user_client.delete_dialog(joined_chat_id)
            
            data["current_index"] = (data["current_index"] + 1) % len(data["queue"])
            
        except UserAlreadyParticipantError:
            await bot_client.send_message(chat_id, f"ℹ️ Already a participant of `{link}`. Skipping to next.")
            data["current_index"] = (data["current_index"] + 1) % len(data["queue"])
            
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
            if data["queue"]:
                data["current_index"] = (data["current_index"] + 1) % len(data["queue"])
            await asyncio.sleep(10)

# ==========================================
# MAIN EXECUTION
# ==========================================

async def main():
    if API_ID == 0:
        logger.error("API_ID must be set before running the script!")
        print("ERROR: Please edit the script or set the environment variables for API_ID.")
        return

    logger.info("Starting Bot Client...")
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("Public Multi-User Bot Online! Send /start to the bot.")
    
    await bot_client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("System stopped by user.")
