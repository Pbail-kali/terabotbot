import os
import time
import requests
import mimetypes
import subprocess
import asyncio
import motor.motor_asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.types import InputMediaDocument, InputMediaPhoto
import re
import logging
import magic
import datetime
import aiofiles
import aiohttp
from aiohttp import ClientTimeout
import threading
from collections import deque
import resource
from web import keep_alive

# Set up loggings
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

# Environment variables
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
CHANNEL_USER = os.getenv("CHANNEL_USER", "MODSMAVI")
MIRROR_CHANNEL_ID = int(os.getenv("MIRROR_CHANNEL_ID")) if os.getenv("MIRROR_CHANNEL_ID") else None
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID")) if os.getenv("LOG_CHANNEL_ID") else None
LINK_CHANNEL_ID = int(os.getenv("LINK_CHANNEL_ID")) if os.getenv("LINK_CHANNEL_ID") else None
OWNER_ID = int(os.getenv("OWNER_ID"))
START_IMAGE = os.getenv("START_IMAGE", "https://telegra.ph/file/504babe67ae701cb458f8.jpg")
RAPIDAPI_HOST = "terabox-downloader-direct-download-link-generator2.p.rapidapi.com"
API_KEYS = os.getenv("API_KEYS").split(",") if os.getenv("API_KEYS") else []
MONGO_URI = os.getenv("MONGO_URI")
UPLOAD_TIMEOUT = int(os.getenv("UPLOAD_TIMEOUT", "1200"))
MAX_FOLDER_FILES = int(os.getenv("MAX_FOLDER_FILES", "30"))  # Max files per folder
MAX_CONCURRENT_DOWNLOADS = 3  # Max concurrent downloads

# MongoDB setup
mongo_client = None
db = None
users_collection = None
stats_collection = None
blocked_users_collection = None

TERABOX_LINK_REGEX = None

active_downloads = {}
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

def progress_bar(percent):
        filled = int(percent // 5)
        empty = 20 - filled
        return "▓" * filled + "░" * empty

def human_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def get_video_dimensions(file_path):
    try:
        cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", 
               "-show_entries", "stream=width,height", 
               "-of", "default=noprint_wrappers=1:nokey=1", file_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        width, height = map(int, result.stdout.strip().split("\n"))
        return width, height
    except Exception as e:
        logger.error(f"Error getting video dimensions: {e}")
        return None, None

def generate_thumbnail(file_path, thumb_path):
    try:
        subprocess.run(
            ["ffmpeg", "-i", file_path, "-ss", "00:00:01.000", "-vframes", "1", thumb_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10  # Add timeout to prevent hanging
        )
        return os.path.exists(thumb_path)
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")
        return False

def detect_file_type(file_path):
    """Detect file type using both file extension and magic numbers"""
    try:
        # First try with file extension
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type:
            return mime_type
        
        # Fallback to magic number detection
        mime = magic.Magic(mime=True)
        return mime.from_file(file_path)
    except Exception as e:
        logger.error(f"Error detecting file type: {e}")
        return "application/octet-stream"

async def init_database():
    """Initialize database connection and collections"""
    global mongo_client, db, users_collection, stats_collection, blocked_users_collection, TERABOX_LINK_REGEX
    
    # Initialize MongoDB connection
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = mongo_client["terabot"]
    users_collection = db["users"]
    stats_collection = db["stats"]
    blocked_users_collection = db["blocked_users"]
    
    # Initialize regex pattern
    TERABOX_LINK_REGEX = re.compile(
        r"https?://(?:\w+.)?(terabox|1024terabox|freeterabox|teraboxapp|tera|teraboxlink|mirrorbox|nephobox|1024tera|momerybox|tibibox|terasharelink|teraboxshare|terafileshare).\w+/(s|folder)/[A-Za-z0-9_-]+"
    )
    
    # Initialize stats if not exists
    if await stats_collection.count_documents({}) == 0:
        await stats_collection.insert_one({
            "total_users": 0,
            "total_downloads": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "last_updated": datetime.datetime.now()
        })

async def delete_message_after_delay(client, chat_id, message_id, delay=10):
    """Delete a message after a specified delay using Telethon"""
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id, message_id)
        logger.info(f"Deleted message {message_id} in chat {chat_id} after {delay} seconds")
    except Exception as e:
        logger.warning(f"Failed to delete message {message_id} in chat {chat_id}: {e}")

async def check_membership(event):
    try:
        await event.client.get_permissions(CHANNEL_ID, event.sender_id)
        return True
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        return False

def get_caption(section: str, user) -> str:
    if section == "about_bot":
        return (
            "🧊 <b>ᴀʙᴏᴜᴛ ᴛᴇʀᴀʙᴏᴛ</b>\n\n"
            "<blockquote>"
            "• ꜰᴀꜱᴛᴇꜱᴛ ᴛᴇʀᴀʙᴏx ᴅɪʀᴇᴄᴛ ᴅᴏᴡɴʟᴏᴀᴅ ʙᴏᴛ\n"
            "• ʙᴜɪʟᴛ ᴡɪᴛʜ 🎯 ʙʏ <a href='https://t.me/Mr_Pbail'>ᴘʙᴀɪʟ</a>\n"
            "• ᴏᴘᴇɴ ꜱᴏᴜʀᴄᴇ, ꜱᴀꜟᴇ, ᴀɴᴅ ᴘʀɪᴠᴀᴄʏ-ꜰʀɪᴇɴᴅʟʏ!\n"
            "• ᴇᴅɪᴛ ʙʏ <a href='https://t.me/INDIAN_HACKER_BOTS'>𝐈𝐍𝐃 𝐁𝐎𝐓𝐒</a>\n"
            "</blockquote>\n"
            "👨‍💻 <b>ʙᴏᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ:</b> <a href='https://t.me/Mr_Pbail'>ᴘʙᴀɪʟ</a>"
        )
    elif section == "help_again":
        return (
            "🦮 <b>ʜᴏᴡ ᴛᴏ ᴜꜱᴇ ᴛᴇʀᴀʙᴏᴛ</b>\n\n"
            "🔗 <b>ꜱᴛᴇᴘ 1:</b> ꜱᴇɴᴅ ᴀɴʏ ᴠᴀʟɪᴅ <b>ᴛᴇʀᴀʙᴏx ʟɪɴᴋ</b> ɪɴ ᴛʜɪꜱ ᴄʜᴀᴛ.\n"
            "⏳ <b>ꜱᴛᴇᴘ 2:</b> ᴡᴀɪᴛ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ/ᴠɪᴅᴇᴏ ᴛᴏ ʙᴇ ᴘʀᴏᴄᴇꜱꜱᴇᴅ ᴀɴᴅ ᴜᴘʟᴏᴀᴅᴇᴅ.\n"
            "⬇️ <b>ꜱᴛᴇᴘ 3:</b> <b>ᴅᴏᴡɴʟᴏᴀᴅ</b> ᴏʀ <b>ꜰᴏʀᴡᴀʀᴅ</b>  ʏᴏᴜʀ ꜰɪʟᴇ/ᴠɪᴅᴇᴏ. "
            "ɪᴛ ᴡɪʟʟ ʙᴇ <b>ᴅᴇʟᴇᴛᴇᴅ ᴀꜰᴛᴇʀ 30 ᴍɪɴᴜᴛᴇꜱ</b> ꜰᴏʀ ᴄᴏᴘʏʀɪɢʜᴛ ꜱᴀꜟᴇᴛʏ.\n\n"
            "♻️ <i>ɪꜰ ʏᴏᴜ ɴᴇᴇᴅ ᴛʜᴇ ꜰɪʟᴇ/ᴠɪᴅᴇᴏ ᴀɢᴀɪɴ, ᴊᴜꜱᴛ ꜱᴇɴᴅ ᴛʜᴇ ʟɪɴᴋ ᴀɢᴀɪɴ!</i>\n"
            "💡 <b>ꜰᴇᴀᴛᴜʀᴇꜱ:</b>\n"
            "• ꜰᴀꜱᴛ ᴅɪʀᴇᴄᴛ ᴅᴏᴡɴʟᴏᴀᴅ ꜰʀᴏᴍ ᴛᴇʀᴀʙᴏx\n"
            "• ᴀᴜᴛᴏ ꜰɪʟᴇ/ᴠɪᴅᴇᴏ ᴅᴇʟᴇᴛɪᴏɴ ꜰᴏʀ ʏᴏᴜʀ ꜱᴀꜟᴇᴛʏ\n"
            "• ᴍɪʀʀᴏʀ ᴛᴏ ᴄʜᴀɴɴᴇʟ (ɪꜰ ᴇɴᴀʙʟᴇᴅ)\n"
            "<blockquote>"
            "👨‍💻 <b>ʙᴏᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ:</b> <a href='https://t.me/Mr_Pbail'>ᴘʙᴀɪʟ</a>"
            "</blockquote>"
        )
    else:  
        return (
            f"👋 <b>ʜᴇʟʟᴏ {user.first_name}!</b>\n\n"
            "ᴊᴜꜱᴛ ꜱᴇɴᴅ ᴀ <b>ᴛᴇʀᴀʙᴏx ʟɪɴᴋ</b> ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ʏᴏᴜʀ ꜰɪʟᴇ ᴏʀ ᴠɪᴅᴇᴏ ɪɴꜱᴛᴀɴᴛʟʏ!\n\n"
            "ᴜꜱᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴꜱ ʙᴇʟᴏᴡ ꜰᴏʀ ᴜᴘᴅᴀᴛᴇꜱ, ʜᴇʟᴘ, ᴏʀ ɪɴꜰᴏ ᴀʙᴏᴜᴛ ᴛʜᴇ ʙᴏᴛ."
        )

def get_keyboard(section: str) -> list:
    if section == "about_bot":
        return [
            [Button.inline("🏠 ʜᴏᴍᴇ", data="home"),
             Button.inline("🦮 ʜᴇʟᴘ", data="help_again")],
            [Button.url("✨ ᴜᴘᴅᴀᴛᴇꜱ ᴄʜᴀɴɴᴇʟ", url=f"https://t.me/{CHANNEL_USER}")]
        ]
    elif section == "help_again":
        return [
            [Button.inline("🏠 ʜᴏᴍᴇ", data="home"),
             Button.inline("🧊 ᴀʙᴏᴜᴛ", data="about_bot")],
            [Button.url("✨ ᴜᴘᴅᴀᴛᴇꜱ ᴄʜᴀɴɴᴇʟ", url=f"https://t.me/{CHANNEL_USER}")]
        ]
    else:  # home section
        return [
            [Button.inline("🧊 ᴀʙᴏᴜᴛ", data="about_bot"),
             Button.inline("🦮 ʜᴇʟᴘ", data="help_again")],
            [Button.url("✨ ᴜᴘᴅᴀᴛᴇꜱ ᴄʜᴀɴɴᴇʟ", url=f"https://t.me/{CHANNEL_USER}")]
        ]

# Modify the start command
async def start(event):
    user = await event.get_sender()
    existing_user = await users_collection.find_one({"_id": user.id})
    
    if not existing_user:
        await users_collection.insert_one({
            "_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "join_date": datetime.datetime.now(),
            "download_count": 0
        })
        await stats_collection.update_one({}, {"$inc": {"total_users": 1}})
        
        if LOG_CHANNEL_ID:
            try:
                await event.client.send_message(
                    LOG_CHANNEL_ID,
                    f"👤 ɴᴇᴡ ᴜꜱᴇʀ: [{user.first_name}](tg://user?id={user.id}) (`{user.id}`)",
                    parse_mode="md"
                )
            except Exception as e:
                logger.error(f"Error sending new user log: {e}")
    
    try:
        await event.client.send_file(
            event.chat_id,
            START_IMAGE,
            caption=get_caption("home", user),
            parse_mode='html',
            buttons=get_keyboard("home")
        )
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await event.reply(
            get_caption("home", user),
            parse_mode='html',
            buttons=get_keyboard("home")
        )

# Add menu callback handler
async def menu_callback(event):
    data = event.data.decode('utf-8')
    user = await event.get_sender()
    
    try:
        await event.edit(
            get_caption(data, user),
            parse_mode='html',
            buttons=get_keyboard(data)
        )
    except Exception as e:
        logger.error(f"Menu callback error: {e}")
        await event.answer("Failed to update menu. Please try again.", alert=True)

async def download_file_with_progress(url, file_path, event, msg, filename, filesize, cancel_event):
    downloaded = 0
    last_update = 0
    last_progress = 0
    chunk_size = 50 * 1024 * 1024  # 5MB chunks
    
    try:
        timeout_value = max(1800, filesize // (1024 * 1024))
        logger.info(f"Downloading {filename} with timeout: {timeout_value}s")
        
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=timeout_value)) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"HTTP Error {response.status}")
                
                content_length = int(response.headers.get('Content-Length', 0))
                if content_length and content_length != filesize:
                    logger.warning(f"Content-Length mismatch: API={human_size(filesize)} Actual={human_size(content_length)}")
                    filesize = content_length
                
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type:
                    chunk = await response.content.read(4096)
                    if b"<html" in chunk.lower() or b"<!doctype" in chunk.lower():
                        raise Exception("Received HTML content instead of file")
                    async with aiofiles.open(file_path, 'wb') as f:
                        await f.write(chunk)
                        downloaded += len(chunk)
                
                async with aiofiles.open(file_path, 'ab') as f:
                    start_time = time.time()
                    async for chunk in response.content.iter_chunked(chunk_size):
                        if not chunk:
                            continue
                            
                        # Check for cancellation
                        if cancel_event.is_set():
                            raise Exception("Download canceled by user")
                            
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        elapsed = time.time() - start_time
                        speed = downloaded / elapsed if elapsed > 0 else 0
                        progress = downloaded / filesize * 100
                        current_progress = int(progress)
                        
                        if current_progress > last_progress or time.time() - last_update > 5:
                            try:
                                # Create progress bar
                                bar = progress_bar(progress)
                                progress_text = (
                                    f"⬇️ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ\n\n"
                                    f"ғɪʟᴇ ɴᴀᴍᴇ: **{filename}**\n"
                                    f"sɪᴢᴇ: {human_size(filesize)}\n\n"
                                    f"ᴘʀᴏᴄᴇss:\n"
                                    f"{bar} {progress:.1f}%\n"
                                    f"sᴘᴇᴇᴅ: {human_size(speed)}/s"
                                )
                                await event.client.edit_message(
                                    event.chat_id,
                                    msg.id,
                                    progress_text,
                                    buttons=[[Button.inline("❌ ᴄᴀɴᴄᴇʟ", f"cancel_{event.sender_id}")]]
                                )
                                last_update = time.time()
                                last_progress = current_progress
                            except Exception as e:
                                if "Message is not modified" not in str(e):
                                    logger.warning(f"Progress update error: {e}")
        
        if os.path.exists(file_path):
            actual_size = os.path.getsize(file_path)
            if actual_size != filesize:
                raise Exception(f"Size mismatch: Expected {human_size(filesize)}, got {human_size(actual_size)}")
        
        return content_type
    except asyncio.TimeoutError:
        raise Exception(f"Download timed out after {timeout_value} seconds")
    except Exception as e:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        raise e

async def upload_file(client, chat_id, file_path, thumb_path, caption, is_video, width=None, height=None, progress_callback=None):
    try:
        if is_video:
            return await client.send_file(
                chat_id,
                file_path,
                caption=caption,
                supports_streaming=True,
                thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None,
                attributes=[],
                video_note=False,
                progress_callback=progress_callback,
                timeout=UPLOAD_TIMEOUT
            )
        else:
            return await client.send_file(
                chat_id,
                file_path,
                caption=caption,
                thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None,
                attributes=[],
                force_document=True,
                progress_callback=progress_callback,
                timeout=UPLOAD_TIMEOUT
            )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise

async def broadcast_command(event):
    user = await event.get_sender()
    if user.id != OWNER_ID:
        await event.reply("❌ This command is restricted to the bot owner only.")
        return
    
    if not event.is_reply:
        await event.reply("❌ Please reply to a message with /broadcast to broadcast it.")
        return
    
    broadcast_msg = await event.get_reply_message()
    users = users_collection.find()
    total_users = 0
    success_count = 0
    failed_count = 0
    blocked_count = 0
    
    status_msg = await event.reply("📤 sᴛᴀʀᴛɪɴɢ ʙʀᴏᴀᴅᴄᴀsᴛ...")
    
    async for user_doc in users:
        total_users += 1
        user_id = user_doc["_id"]
        
        if await blocked_users_collection.find_one({"user_id": user_id}):
            blocked_count += 1
            continue
        
        try:
            await event.client.forward_messages(user_id, broadcast_msg)
            success_count += 1
        except Exception as e:
            if "bot was blocked" in str(e).lower():
                blocked_count += 1
                await blocked_users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"blocked_at": datetime.datetime.now()}},
                    upsert=True
                )
            else:
                failed_count += 1
                logger.error(f"Failed to send to {user_id}: {str(e)}")
        
        if total_users % 10 == 0:
            await event.client.edit_message(
                status_msg.chat_id,
                status_msg.id,
                f"📤 ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ...\nᴛᴏᴛᴀʟ: {total_users}\nsᴜᴄᴄᴇss: {success_count}\nғᴀɪʟᴇᴅ: {failed_count}\nʙʟᴏᴄᴋᴇᴅ: {blocked_count}"
            )
    
    await event.client.edit_message(
        status_msg.chat_id,
        status_msg.id,
        f"✅ ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!\nᴛᴏᴛᴀʟ: {total_users}\nsᴜᴄᴄᴇss: {success_count}\nғᴀɪʟᴇᴅ: {failed_count}\nʙʟᴏᴄᴋᴇᴅ: {blocked_count}"
    )
    
    if LOG_CHANNEL_ID:
        try:
            await event.client.send_message(
                LOG_CHANNEL_ID,
                f"📢 ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇᴅ:\nᴛᴏᴛᴀʟ: {total_users}, sᴜᴄᴄᴇss: {success_count}, ғᴀɪʟᴇᴅ: {failed_count}, ʙʟᴏᴄᴋᴇᴅ: {blocked_count}"
            )
        except Exception as e:
            logger.error(f"Failed to log broadcast: {e}")

async def status_command(event):
    stats = await stats_collection.find_one({})
    if not stats:
        await event.reply("📊 ʙᴏᴛ sᴛᴀᴛɪsᴛɪᴄs ɴᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ ʏᴇᴛ.")
        return
    
    total_users = stats.get("total_users", 0)
    total_downloads = stats.get("total_downloads", 0)
    successful_downloads = stats.get("successful_downloads", 0)
    failed_downloads = stats.get("failed_downloads", 0)
    
    success_rate = (successful_downloads / total_downloads * 100) if total_downloads > 0 else 0
    
    response = (
        "✨ ᴛᴇʀᴀʙᴏᴛ ꜱᴛᴀᴛᴜꜱ ᴘᴀɴᴇʟ ✨\n\n"
        f"👤 ᴛᴏᴛᴀʟ ᴜꜱᴇʀꜱ: {total_users}\n"
        f"⬇️ ᴛᴏᴛᴀʟ ᴅᴏᴡɴʟᴏᴀᴅꜱ: {total_downloads}\n"
        f"✅ ᴜᴘʟᴏᴀᴅᴇᴅ: {successful_downloads}\n"
        f"❌ ꜰᴀɪʟᴇᴅ: {failed_downloads}\n"
        f"📈 ꜱᴜᴄᴄᴇꜱꜱ ʀᴀᴛᴇ: {success_rate:.2f}%\n"
        f"⏱️ ᴜᴘᴛɪᴍᴇ: {get_uptime()}"
        "\n🚀 ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴛᴇʀᴀʙᴏt"
    )
    
    await event.reply(response)

async def astatus_command(event):
    user = await event.get_sender()
    if user.id != OWNER_ID:
        await event.reply("❌ ᴛʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴛʜᴇ ʙᴏᴛ ᴏᴡɴᴇʀ ᴏɴʟʏ.")
        return
    
    stats = await stats_collection.find_one({})
    if not stats:
        await event.reply("📊 ʙᴏᴛ sᴛᴀᴛɪsᴛɪᴄs ɴᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ ʏᴇᴛ.")
        return
    
    total_users = stats.get("total_users", 0)
    total_downloads = stats.get("total_downloads", 0)
    successful_downloads = stats.get("successful_downloads", 0)
    failed_downloads = stats.get("failed_downloads", 0)
    
    success_rate = (successful_downloads / total_downloads * 100) if total_downloads > 0 else 0
    
    most_active_users = await users_collection.find().sort("download_count", -1).limit(5).to_list(length=2)
    active_users_text = "\n".join(
        [f"{i+1}. {user['name']} ({user['_id']}) - {user.get('download_count', 0)} downloads" 
         for i, user in enumerate(most_active_users)]
    )
    
    blocked_users = await blocked_users_collection.find().sort("blocked_at", -1).limit(10).to_list(length=10)
    blocked_users_text = "\n".join(
        [f"• ᴜsᴇʀ ɪᴅ: {user['user_id']} - ʙʟᴏᴄᴋᴇᴅ ᴀᴛ: {user['blocked_at'].strftime('%Y-%m-%d %H:%M')}"
         for user in blocked_users]
    ) if blocked_users else "No blocked users"
    
    response = (
        f"🔒 ᴀᴅᴍɪɴ ꜱᴛᴀᴛᴜꜱ:\n\n"
        f"📊 ɢᴇɴᴇʀᴀʟ ꜱᴛᴀᴛꜱ:\n"
        f"👥 ᴛᴏᴛᴀʟ ᴜꜱᴇʀꜱ: {total_users}\n"
        f"⬇️ ᴛᴏᴛᴀʟ ᴅᴏᴡɴʟᴏᴀᴅꜱ: {total_downloads}\n"
        f"✅ ᴜᴘʟᴏᴀᴅᴇᴅ: {successful_downloads}\n"
        f"❌ ꜰᴀɪʟᴇᴅ: {failed_downloads}\n"
        f"📈 ꜱᴜᴄᴄᴇꜱꜱ ʀᴀᴛᴇ: {success_rate:.2f}%\n"
        f"🆙 ᴜᴘᴛɪᴍᴇ: {get_uptime()}\n\n"
        f"⭐ ᴛᴏᴘ ᴀᴄᴛɪᴠᴇ ᴜꜱᴇʀꜱ:\n{active_users_text}\n\n"
        f"🚫 ʙʟᴏᴄᴋᴇᴅ ᴜꜱᴇʀꜱ:\n{blocked_users_text}"
    )
    
    await event.reply(response)

# Track bot startup time for uptime calculation
START_TIME = time.time()

def get_uptime():
    uptime_seconds = int(time.time() - START_TIME)
    days, uptime_seconds = divmod(uptime_seconds, 86400)
    hours, uptime_seconds = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(uptime_seconds, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"

async def fetch_alt_api(link):
    try:
        timeout = aiohttp.ClientTimeout(total=120)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache"
        }
        
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(f"https://lavdya.ninja1.workers.dev/?url={link}") as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict) and (data.get('direct_link') or data.get('link')):
                        return data
                return None
    except Exception as e:
        logger.warning(f"Alternative API error: {str(e)}")
        return None

async def handle_message(event):
    text = event.raw_text.strip()
    if not TERABOX_LINK_REGEX.search(text):
        return

    if not await check_membership(event):
        buttons = [[Button.url("ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ", f"https://t.me/{CHANNEL_USER}")]]
        try:
            await event.reply(
                "🔒 ʏᴏᴜ ᴍᴜsᴛ ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴜsᴇ ᴛʜɪs ʙᴏᴛ.",
                buttons=buttons)
        except Exception as e:
            logger.error(f"Membership check reply error: {e}")
        return

    user = await event.get_sender()
    try:
        # Create cancel button
        cancel_button = Button.inline("❌ ᴄᴀɴᴄᴇʟ", f"cancel_{user.id}")
        msg = await event.reply("🔗 ɢᴇᴛᴛɪɴɢ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋ...", buttons=[[cancel_button]])
    except Exception as e:
        logger.error(f"Error sending initial message: {e}")
        return

    # Create cancel event for this specific download
    cancel_event = asyncio.Event()
    user_id = user.id
    
    # Add to active downloads
    active_downloads[user_id] = cancel_event

    async def download_task():
        nonlocal msg
        try:
            last_error = None
            folder_data = None
            use_alt_api = False

            # First, try alternative API
            alt_api_data = await fetch_alt_api(text)
            if alt_api_data:
                folder_data = [{
                    "file_name": alt_api_data['file_name'],
                    "direct_link": alt_api_data.get('direct_link', ''),
                    "link": alt_api_data.get('link', ''),
                    "thumbnail": alt_api_data.get('thumb', ''),
                    "size": alt_api_data.get('size', ''),
                    "sizebytes": alt_api_data.get('sizebytes', 0)
                }]
                use_alt_api = True
                total_files = 1
                await msg.edit("🔗 sᴛᴀʀᴛɪɴɢ ᴅᴏᴡɴʟᴏᴀᴅ...", buttons=[[cancel_button]])
            else:
                # Then try RapidAPI
                for api_key in API_KEYS:
                    for retry in range(10):  # Added retry loop
                        try:
                            # Check if canceled
                            if cancel_event.is_set():
                                await msg.edit("❌ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴀɴᴄᴇʟᴇᴅ.", buttons=None)
                                return
                            if retry > 0:
                                await msg.edit(f"🔗 ʀᴇᴛʀʏɪɴɢ ({retry+1}/10) ᴡɪᴛʜ ᴀᴘɪ ᴋᴇʏ...", buttons=[[cancel_button]])

                            res = requests.get(
                                f"https://{RAPIDAPI_HOST}/url",
                                params={"url": text},
                                headers={
                                    "X-RapidAPI-Key": api_key,
                                    "X-RapidAPI-Host": RAPIDAPI_HOST
                                },
                                timeout=60
                            )
                            res.raise_for_status()
                            resp_json = res.json()
                            
                            if not resp_json or not isinstance(resp_json, list) or len(resp_json) == 0:
                                raise Exception("Invalid API response")
                            
                            folder_data = resp_json
                            total_files = len(folder_data)
                            if total_files > MAX_FOLDER_FILES:
                                folder_data = folder_data[:MAX_FOLDER_FILES]
                                total_files = MAX_FOLDER_FILES
                                await msg.edit(f"⚠️ ғᴏʟᴅᴇʀ ʜᴀs ᴍᴏʀᴇ ᴛʜᴀɴ {MAX_FOLDER_FILES} ғɪʟᴇs. ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ғɪʀsᴛ {MAX_FOLDER_FILES} ғɪʟᴇs.", buttons=[[cancel_button]])
                            
                            await msg.edit(f"📁 ғᴏᴜɴᴅ {total_files} ғɪʟᴇs. sᴛᴀʀᴛɪɴɢ ᴅᴏᴡɴʟᴏᴀᴅ...", buttons=[[cancel_button]])
                            break
                        except Exception as e:
                            last_error = e
                            logger.error(f"API key {api_key} failed (attempt {retry+1}): {str(e)}")
                            if retry < 2:  # Only sleep if we'll retry again
                                await asyncio.sleep(5)  # Short delay before retry
                            else:
                                continue  

            if not folder_data:
                error_msg = f"❌ ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋs"
                await stats_collection.update_one({}, {
                    "$inc": {
                        "total_downloads": 1,
                        "failed_downloads": 1
                    }
                })
                await msg.edit(error_msg, buttons=None)
                return

            successful_files = 0
            failed_files = 0
            skipped_files = 0
            file_path = None
            thumb_path = None
            
            if LINK_CHANNEL_ID:
                try:
                    await event.client.send_message(
                        LINK_CHANNEL_ID,
                        f"🌐 ɴᴇᴡ ʟɪɴᴋ: {text} \nby {user.first_name}",
                        parse_mode="md"
                    )
                except Exception as e:
                    logger.error(f"Link channel error: {e}")
            
            async with download_semaphore:
                for file_index, file_data in enumerate(folder_data, 1):
                    # Reset cancellation for each new file
                    if cancel_event.is_set():
                        cancel_event.clear()
                        
                    # Check if entire process was canceled
                    if user_id not in active_downloads:
                        await msg.edit("❌ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴀɴᴄᴇʟᴇᴅ.", buttons=None)
                        return
                        
                    filename = file_data.get("file_name", "file")
                    dlink = file_data.get("direct_link") or file_data.get("link")
                    alt_link = file_data.get("link")
                    filesize = int(file_data.get("sizebytes", 0))
                    thumb_url = file_data.get("thumbnail")

                    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
                    if '.' not in filename:
                        if "video" in filename.lower():
                            filename += ".mp4"
                        elif "image" in filename.lower() or "photo" in filename.lower():
                            filename += ".jpg"
                        else:
                            filename += ".bin"

                    file_path = f"{user.id}_{filename}"
                    thumb_path = f"{file_path}.jpg"

                    await msg.edit(f"📁 ᴘʀᴏᴄᴇssɪɴɢ ғɪʟᴇ {file_index}/{len(folder_data)}: {filename}", buttons=[[Button.inline("❌ ᴄᴀɴᴄᴇʟ", f"cancel_{user.id}")]])

                    download_success = False
                    download_urls = set()
                    
                    if dlink:
                        download_urls.add(dlink)
                    if alt_link and alt_link != dlink:
                        download_urls.add(alt_link)
                        
                    for download_url in download_urls:
                        try:
                            if cancel_event.is_set():
                                # Skip this file but continue with next
                                await msg.edit(f"⏭️ sᴋɪᴘᴘᴇᴅ ғɪʟᴇ {file_index}: {filename}", buttons=[[Button.inline("❌ ᴄᴀɴᴄᴇʟ", f"cancel_{user.id}")]])
                                skipped_files += 1
                                break
                                
                            content_type = await download_file_with_progress(
                                download_url, 
                                file_path, 
                                event, 
                                msg, 
                                filename, 
                                filesize,
                                cancel_event
                            )
                            download_success = True
                            break
                        except Exception as e:
                            if "Download canceled" in str(e):
                                # Skip this file but continue with next
                                await msg.edit(f"⏭️ sᴋɪᴘᴘᴇᴅ ғɪʟᴇ {file_index}: {filename}", buttons=[[Button.inline("❌ ᴄᴀɴᴄᴇʟ", f"cancel_{user.id}")]])
                                skipped_files += 1
                                break
                            else:
                                last_error = e
                                logger.warning(f"Download failed from {download_url[:50]}...: {e}")
                                if os.path.exists(file_path):
                                    try:
                                        os.remove(file_path)
                                    except:
                                        pass
                    
                    if not download_success and not cancel_event.is_set():
                        # Only count as failed if not canceled by user
                        await stats_collection.update_one({}, {
                            "$inc": {
                                "total_downloads": 1,
                                "failed_downloads": 1
                            }
                        })
                        failed_files += 1
                        continue
                    
                    # If canceled during download, skip to next file
                    if cancel_event.is_set():
                        cancel_event.clear()
                        skipped_files += 1
                        continue
                    
                    mime_type = detect_file_type(file_path)
                    logger.info(f"Detected MIME type: {mime_type} for {file_path}")

                    caption = f"🎬ғɪʟᴇ ɴᴀᴍᴇ: {filename}\n\n📦 sɪᴢᴇ: {human_size(filesize)}"

                    try:
                        # Remove cancel button before upload
                        await msg.edit(f"✅ ғɪʟᴇ {file_index}/{len(folder_data)} ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ! sᴛᴀʀᴛɪɴɢ ᴜᴘʟᴏᴀᴅ...", buttons=None)
                        await asyncio.sleep(2)
                    except:
                        pass
                    
                    is_video = mime_type.startswith("video/")
                    width, height = (None, None)
                    
                    if is_video:
                        await asyncio.to_thread(generate_thumbnail, file_path, thumb_path)
                        width, height = await asyncio.to_thread(get_video_dimensions, file_path)
                        logger.info(f"Video dimensions: {width}x{height}")

                    # Create upload status message with progress bar
                    upload_msg = await event.reply(f"📤 ᴜᴘʟᴏᴀᴅɪɴɢ ғɪʟᴇ {file_index}/{len(folder_data)}:\n\nғɪʟᴇ ɴᴀᴍᴇ: {filename}\n\nᴘʀᴏᴄᴇss:\n{progress_bar(0)} 0%")
                    last_progress_update = time.time()
                    last_percent_sent = 0
                    
                    # Progress callback for upload
                    def progress_callback(current, total):
                        nonlocal last_progress_update, last_percent_sent
                        percent = current / total * 100
                        current_percent = int(percent)
                        
                        # Only update if progress changed by at least 1% or 5 seconds passed
                        if current_percent > last_percent_sent or time.time() - last_progress_update > 5:
                            try:
                                bar = progress_bar(percent)
                                asyncio.create_task(event.client.edit_message(
                                    upload_msg.chat_id,
                                    upload_msg.id,
                                    f"📤 ᴜᴘʟᴏᴀᴅɪɴɢ ғɪʟᴇ {file_index}/{len(folder_data)}:\n\nғɪʟᴇ ɴᴀᴍᴇ: {filename}\n\nᴘʀᴏᴄᴇss:\n{bar} {percent:.1f}%"
                                ))
                                last_progress_update = time.time()
                                last_percent_sent = current_percent
                            except Exception:
                                pass  # Avoid flooding errors
                    
                    try:
                        # Upload to user with progress callback
                        sent_message = await upload_file(
                            client=event.client,
                            chat_id=event.chat_id,
                            file_path=file_path,
                            thumb_path=thumb_path,
                            caption=caption,
                            is_video=is_video,
                            width=width,
                            height=height,
                            progress_callback=progress_callback
                        )
                        asyncio.create_task(
                            delete_message_after_delay(
                                event.client,
                                event.chat_id,
                                sent_message.id,
                                1800  # 30 minutes
                            )
                        )
                        
                        # Update upload message to completion
                        await event.client.edit_message(
                            upload_msg.chat_id,
                            upload_msg.id,
                            f"✅ ғɪʟᴇ {file_index}/{len(folder_data)} ᴜᴘʟᴏᴀᴅᴇᴅ!"
                        )
                        await asyncio.sleep(2)
                        try:
                            await upload_msg.delete()
                        except:
                            pass
                        
                        # Update stats
                        await stats_collection.update_one({}, {
                            "$inc": {
                                "total_downloads": 1,
                                "successful_downloads": 1
                            }
                        })
                        
                        # Update user download count
                        await users_collection.update_one(
                            {"_id": user.id},
                            {"$inc": {"download_count": 1}}
                        )
                        
                        successful_files += 1

                        # Mirror to channel by forwarding without forward tag
                        if MIRROR_CHANNEL_ID:
                            try:
        # Forward the message directly to mirror channel
                                await event.client.forward_messages(
                                    entity=MIRROR_CHANNEL_ID,
                                    messages=sent_message,
                                    drop_author=True
                                )
                            except Exception as e:
                                logger.error(f"Mirror error: {e}")
                                if LOG_CHANNEL_ID:
                                    try:
                                        await event.client.send_message(
                                            LOG_CHANNEL_ID,
                                            f"❌ Mirror failed for {filename}\nError: {str(e)}"
                                        )
                                    except:
                                        pass

                    except Exception as e:
                        await event.client.edit_message(
                            upload_msg.chat_id,
                            upload_msg.id,
                            f"❌ Upload failed: {str(e)}"
                        )
                        await stats_collection.update_one({}, {
                            "$inc": {
                                "total_downloads": 1,
                                "failed_downloads": 1
                            }
                        })
                        failed_files += 1

                    # Cleanup files after upload
                    for path in [file_path, thumb_path]:
                        if path and os.path.exists(path):
                            try:
                                os.remove(path)
                            except Exception as e:
                                logger.error(f"Error deleting file {path}: {e}")

            # Final folder status
            if successful_files > 0 or failed_files > 0 or skipped_files > 0:
                status_msg = f"✅ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴘʟᴇᴛᴇ!\n\nsᴜᴄᴄᴇss: {successful_files}\nғᴀɪʟᴇᴅ: {failed_files}\nsᴋɪᴘᴘᴇᴅ: {skipped_files}"
                await msg.edit(status_msg, buttons=None)
            else:
                await msg.edit("❌ ᴀʟʟ ᴅᴏᴡɴʟᴏᴀᴅs ғᴀɪʟᴇᴅ", buttons=None)

        except Exception as e:
            logger.error(f"Download task failed: {e}")
            for path in [file_path, thumb_path]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except:
                        pass
            try:
                await msg.edit(f"❌ ᴅᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {str(e)[:200]}", buttons=None)
            except:
                pass
        finally:
            # Clear from active downloads
            if user_id in active_downloads:
                del active_downloads[user_id]

    # Start download task
    asyncio.create_task(download_task())

async def cancel_handler(event):
    try:
        user_id = int(event.data.decode('utf-8').split('_')[1])
    except:
        await event.answer("Invalid request!")
        return
        
    if event.sender_id != user_id:
        await event.answer("ʏᴏᴜ ᴄᴀɴ ᴏɴʟʏ ᴄᴀɴᴄᴇʟ ʏᴏᴜʀ ᴏᴡɴ ᴅᴏᴡɴʟᴏᴀᴅs!")
        return
        
    if user_id in active_downloads:
        active_downloads[user_id].set()
        await event.answer("ᴄᴜʀʀᴇɴᴛ ғɪʟᴇ ᴄᴀɴᴄᴇʟʟᴀᴛɪᴏɴ ʀᴇǫᴜᴇsᴛᴇᴅ!")
    else:
        await event.answer("ɴᴏ ᴀᴄᴛɪᴠᴇ ᴅᴏᴡɴʟᴏᴀᴅ ᴛᴏ ᴄᴀɴᴄᴇʟ!")

async def main():
    mimetypes.init()
    client = TelegramClient('bot_session', API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    await init_database()
    logger.info("Database initialized successfully")
    
    client.add_event_handler(start, events.NewMessage(pattern='/start'))
    client.add_event_handler(broadcast_command, events.NewMessage(pattern='/broadcast'))
    client.add_event_handler(status_command, events.NewMessage(pattern='/status'))
    client.add_event_handler(astatus_command, events.NewMessage(pattern='/astatus'))
    client.add_event_handler(handle_message, events.NewMessage())
    client.add_event_handler(cancel_handler, events.CallbackQuery(pattern=r'cancel_\d+'))
    # Add menu callback handler
    client.add_event_handler(menu_callback, events.CallbackQuery(pattern=r'home|about_bot|help_again'))
    
    logger.info("Bot is running...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    asyncio.run(main())
