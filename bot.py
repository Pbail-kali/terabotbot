import os
import time
import requests
import mimetypes
import subprocess
import asyncio
import motor.motor_asyncio
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
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

# Increase resource limits
resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

# Environment variables
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
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

# MongoDB setup
mongo_client = None
db = None
users_collection = None
stats_collection = None
api_stats_collection = None
blocked_users_collection = None

# Will be initialized later
TERABOX_LINK_REGEX = None

# Download queue system
class DownloadQueue:
    def __init__(self):
        self.queue = deque()
        self.lock = threading.Lock()
        self.active_downloads = 0
        self.max_concurrent = 3  # Max concurrent downloads
        self.processing = False
        self.cancel_requests = set()
        
    def add_task(self, user_id, task):
        with self.lock:
            self.queue.append((user_id, task))
            if not self.processing:
                self.processing = True
                asyncio.create_task(self.process_queue())
    
    async def process_queue(self):
        while True:
            with self.lock:
                if not self.queue or self.active_downloads >= self.max_concurrent:
                    if not self.queue:
                        self.processing = False
                    return
                
                user_id, task = self.queue.popleft()
                self.active_downloads += 1
                
            try:
                # Check if canceled before starting
                if user_id in self.cancel_requests:
                    self.cancel_requests.discard(user_id)
                    logger.info(f"Skipping canceled task for user {user_id}")
                else:
                    await task
            except Exception as e:
                logger.error(f"Queue task failed: {e}")
            finally:
                with self.lock:
                    self.active_downloads -= 1
                    # Process next item immediately
                    asyncio.create_task(self.process_queue())
                    
    def get_queue_position(self, user_id):
        with self.lock:
            for i, (uid, _) in enumerate(self.queue):
                if uid == user_id:
                    return i + 1
            return 0
            
    def cancel_task(self, user_id):
        with self.lock:
            self.cancel_requests.add(user_id)
            # Remove from queue
            for i, (uid, task) in enumerate(self.queue):
                if uid == user_id:
                    self.queue.remove((uid, task))
                    return True
            return False

# Create global queue instance
DOWNLOAD_QUEUE = DownloadQueue()

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
    global mongo_client, db, users_collection, stats_collection, api_stats_collection, blocked_users_collection, TERABOX_LINK_REGEX
    
    # Initialize MongoDB connection
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = mongo_client["terabot"]
    users_collection = db["users"]
    stats_collection = db["stats"]
    api_stats_collection = db["api_stats"]
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
    
    # Initialize API stats
    for api_key in API_KEYS:
        if not await api_stats_collection.find_one({"api_key": api_key}):
            await api_stats_collection.insert_one({
                "api_key": api_key,
                "success_count": 0,
                "failure_count": 0,
                "last_used": None
            })

async def check_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, update.effective_user.id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    existing_user = await users_collection.find_one({"_id": user.id})
    
    if not existing_user:
        await users_collection.insert_one({
            "_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "join_date": datetime.datetime.now(),
            "download_count": 0
        })
        # Update total user count
        await stats_collection.update_one({}, {"$inc": {"total_users": 1}})
        
        if LOG_CHANNEL_ID:
            try:
                await context.bot.send_message(
                    chat_id=LOG_CHANNEL_ID,
                    text=f"👤 New user: [{user.first_name}](tg://user?id={user.id}) (`{user.id}`)",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Error sending new user log: {e}")
    
    keyboard = [[InlineKeyboardButton("Updates", url=f"https://t.me/{CHANNEL_USER}")]]
    try:
        await update.message.reply_photo(
            photo=START_IMAGE,
            caption=f"👋 Hello {user.first_name}!\n\nSend a Terabox link to download.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await update.message.reply_text(
            f"👋 Hello {user.first_name}!\n\nSend a Terabox link to download."
        )

async def download_file_with_progress(url, file_path, msg, filename, filesize):
    downloaded = 0
    last_update = 0
    last_progress = 0
    # Increase chunk size for larger files
    chunk_size = 5 * 1024 * 1024  # 5MB chunks
    
    try:
        # Increase timeout for larger files
        timeout_value = max(600, filesize // (1024 * 1024))  # 1 sec per MB + buffer
        logger.info(f"Downloading {filename} with timeout: {timeout_value}s")
        
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=timeout_value)) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"HTTP Error {response.status}")
                
                # Verify content length matches expected size
                content_length = int(response.headers.get('Content-Length', 0))
                if content_length and content_length != filesize:
                    logger.warning(f"Content-Length mismatch: API={human_size(filesize)} Actual={human_size(content_length)}")
                    filesize = content_length  # Use actual size for progress
                
                # Check for HTML content
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' in content_type:
                    chunk = await response.content.read(4096)
                    if b"<html" in chunk.lower() or b"<!doctype" in chunk.lower():
                        raise Exception("Received HTML content instead of file")
                    # Write the initial chunk
                    async with aiofiles.open(file_path, 'wb') as f:
                        await f.write(chunk)
                        downloaded += len(chunk)
                
                # Download the file in chunks
                async with aiofiles.open(file_path, 'ab') as f:
                    start_time = time.time()
                    async for chunk in response.content.iter_chunked(chunk_size):
                        if not chunk:
                            continue
                            
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Calculate speed
                        elapsed = time.time() - start_time
                        speed = downloaded / elapsed if elapsed > 0 else 0
                        
                        # Calculate progress
                        progress = downloaded / filesize * 100
                        current_progress = int(progress)
                        
                        # Only update if progress changed by at least 1% or 5 seconds passed
                        if current_progress > last_progress or time.time() - last_update > 5:
                            try:
                                progress_text = (
                                    f"⬇ Downloading\n\n**{filename}**\n"
                                    f"Size: {human_size(filesize)}\n"
                                    f"Progress: {progress:.1f}%\n"
                                    f"Speed: {human_size(speed)}/s"
                                )
                                
                                await msg.edit_text(progress_text)
                                last_update = time.time()
                                last_progress = current_progress
                            except Exception as e:
                                if "Message is not modified" not in str(e):
                                    logger.warning(f"Progress update error: {e}")
        
        # Verify download completed fully
        if os.path.exists(file_path):
            actual_size = os.path.getsize(file_path)
            if actual_size != filesize:
                raise Exception(f"Size mismatch: Expected {human_size(filesize)}, got {human_size(actual_size)}")
        
        return content_type
    except asyncio.TimeoutError:
        raise Exception(f"Download timed out after {timeout_value} seconds")
    except Exception as e:
        # Clean up partially downloaded file
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        raise e

async def upload_file(context, chat_id, file_path, thumb_path, caption, is_video, width=None, height=None):
    """Upload file with better large file handling"""
    try:
        # Open files
        file_handle = open(file_path, 'rb')
        thumb_handle = None
        
        if thumb_path and os.path.exists(thumb_path):
            thumb_handle = open(thumb_path, 'rb')
        
        # Send with timeout settings
        if is_video:
            return await context.bot.send_video(
                chat_id=chat_id,
                video=file_handle,
                caption=caption,
                width=width,
                height=height,
                supports_streaming=True,
                thumbnail=thumb_handle,
                api_kwargs={'timeout': UPLOAD_TIMEOUT}
            )
        else:
            return await context.bot.send_document(
                chat_id=chat_id,
                document=file_handle,
                caption=caption,
                thumbnail=thumb_handle,
                api_kwargs={'timeout': UPLOAD_TIMEOUT}
            )
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise
    finally:
        # Ensure files are closed
        if 'file_handle' in locals():
            file_handle.close()
        if 'thumb_handle' in locals() and thumb_handle:
            thumb_handle.close()

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users in database (owner only)"""
    user = update.effective_user
    
    # Check if user is owner
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ This command is restricted to the bot owner only.")
        return
    
    # Check if command is a reply
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Please reply to a message with /broadcast to broadcast it.")
        return
    
    # Get the message to broadcast
    broadcast_msg = update.message.reply_to_message
    
    # Get all users from database
    users = users_collection.find()
    total_users = 0
    success_count = 0
    failed_count = 0
    blocked_count = 0
    blocked_users = []
    
    # Create status message
    status_msg = await update.message.reply_text("📤 Starting broadcast...")
    
    async for user_doc in users:
        total_users += 1
        user_id = user_doc["_id"]
        
        # Skip blocked users
        if await blocked_users_collection.find_one({"user_id": user_id}):
            blocked_count += 1
            continue
        
        try:
            # Forward the message to the user
            await broadcast_msg.forward(chat_id=user_id)
            success_count += 1
        except Exception as e:
            # Handle specific errors
            if "bot was blocked" in str(e).lower():
                blocked_count += 1
                blocked_users.append(user_id)
                # Add to blocked users collection
                await blocked_users_collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"blocked_at": datetime.datetime.now()}},
                    upsert=True
                )
            else:
                failed_count += 1
                logger.error(f"Failed to send to {user_id}: {str(e)}")
        
        # Update status every 10 messages
        if total_users % 10 == 0:
            await status_msg.edit_text(
                f"📤 Broadcasting...\n"
                f"Total: {total_users}\n"
                f"Success: {success_count}\n"
                f"Failed: {failed_count}\n"
                f"Blocked: {blocked_count}"
            )
    
    # Final status update
    await status_msg.edit_text(
        f"✅ Broadcast completed!\n"
        f"Total users: {total_users}\n"
        f"Successfully sent: {success_count}\n"
        f"Failed to send: {failed_count}\n"
        f"Blocked users: {blocked_count}"
    )
    
    # Log to log channel
    if LOG_CHANNEL_ID:
        try:
            await context.bot.send_message(
                chat_id=LOG_CHANNEL_ID,
                text=f"📢 Broadcast sent by owner:\n"
                     f"Total: {total_users}, Success: {success_count}, "
                     f"Failed: {failed_count}, Blocked: {blocked_count}"
            )
        except Exception as e:
            logger.error(f"Failed to log broadcast: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics to any user"""
    stats = await stats_collection.find_one({})
    if not stats:
        await update.message.reply_text("📊 Bot statistics not available yet.")
        return
    
    total_users = stats.get("total_users", 0)
    total_downloads = stats.get("total_downloads", 0)
    successful_downloads = stats.get("successful_downloads", 0)
    failed_downloads = stats.get("failed_downloads", 0)
    
    # Calculate success rate
    success_rate = (successful_downloads / total_downloads * 100) if total_downloads > 0 else 0
    
    response = (
        f"🤖 Bot Status:\n"
        f"👥 Total Users: {total_users}\n"
        f"⬇️ Total Downloads: {total_downloads}\n"
        f"✅ Successful Downloads: {successful_downloads}\n"
        f"❌ Failed Downloads: {failed_downloads}\n"
        f"📈 Success Rate: {success_rate:.2f}%\n"
        f"🆙 Uptime: {get_uptime()}"
    )
    
    await update.message.reply_text(response)

async def astatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show advanced statistics to owner only"""
    user = update.effective_user
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ This command is restricted to the bot owner only.")
        return
    
    # Get general stats
    stats = await stats_collection.find_one({})
    if not stats:
        await update.message.reply_text("📊 Bot statistics not available yet.")
        return
    
    total_users = stats.get("total_users", 0)
    total_downloads = stats.get("total_downloads", 0)
    successful_downloads = stats.get("successful_downloads", 0)
    failed_downloads = stats.get("failed_downloads", 0)
    
    # Calculate success rate
    success_rate = (successful_downloads / total_downloads * 100) if total_downloads > 0 else 0
    
    # Get most active users
    most_active_users = await users_collection.find().sort("download_count", -1).limit(5).to_list(length=5)
    active_users_text = "\n".join(
        [f"{i+1}. {user['name']} ({user['_id']}) - {user.get('download_count', 0)} downloads" 
         for i, user in enumerate(most_active_users)]
    )
    
    # Get API key stats
    api_stats = await api_stats_collection.find().sort("success_count", -1).to_list(length=None)
    api_stats_text = "\n".join(
        [f"• Key {i+1}: ✅ {stat['success_count']} | ❌ {stat['failure_count']} | Last used: {stat['last_used'].strftime('%Y-%m-%d %H:%M') if stat['last_used'] else 'Never'}"
         for i, stat in enumerate(api_stats)]
    )
    
    # Get blocked users
    blocked_users = await blocked_users_collection.find().sort("blocked_at", -1).limit(10).to_list(length=10)
    blocked_users_text = "\n".join(
        [f"• User ID: {user['user_id']} - Blocked at: {user['blocked_at'].strftime('%Y-%m-%d %H:%M')}"
         for user in blocked_users]
    ) if blocked_users else "No blocked users"
    
    response = (
        f"🔒 Admin Status:\n\n"
        f"📊 General Stats:\n"
        f"👥 Total Users: {total_users}\n"
        f"⬇️ Total Downloads: {total_downloads}\n"
        f"✅ Successful Downloads: {successful_downloads}\n"
        f"❌ Failed Downloads: {failed_downloads}\n"
        f"📈 Success Rate: {success_rate:.2f}%\n"
        f"🆙 Uptime: {get_uptime()}\n\n"
        f"⭐ Top Active Users:\n{active_users_text}\n\n"
        f"🔑 API Key Statistics:\n{api_stats_text}\n\n"
        f"🚫 Blocked Users:\n{blocked_users_text}"
    )
    
    await update.message.reply_text(response)

# Track bot startup time for uptime calculation
START_TIME = time.time()

def get_uptime():
    """Calculate and format bot uptime"""
    uptime_seconds = int(time.time() - START_TIME)
    days, uptime_seconds = divmod(uptime_seconds, 86400)
    hours, uptime_seconds = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(uptime_seconds, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not TERABOX_LINK_REGEX.search(text):
        return

    # Membership check
    if not await check_membership(update, context):
        keyboard = [[InlineKeyboardButton("Join Channel", url=f"https://t.me/{CHANNEL_USER}")]]
        try:
            await update.message.reply_text(
                "🔒 You must join our channel to use this bot.",
                reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Membership check reply error: {e}")
        return

    user = update.effective_user
    try:
        msg = await update.message.reply_text("🔗 Getting download link...")
    except Exception as e:
        logger.error(f"Error sending initial message: {e}")
        return

    # Add cancel button
    cancel_button = InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{user.id}")
    cancel_keyboard = InlineKeyboardMarkup([[cancel_button]])
    try:
        await msg.edit_reply_markup(reply_markup=cancel_keyboard)
    except Exception as e:
        logger.warning(f"Couldn't add cancel button: {e}")

    # Create task function
    async def download_task():
        nonlocal msg
        try:
            last_error = None
            data = None
            success = False
            file_path = None
            thumb_path = None

            for api_key in API_KEYS:
                try:
                    # Check if canceled
                    if user.id in DOWNLOAD_QUEUE.cancel_requests:
                        DOWNLOAD_QUEUE.cancel_requests.discard(user.id)
                        await msg.edit_text("❌ Download canceled by user.")
                        return

                    # API request
                    res = requests.get(
                        f"https://{RAPIDAPI_HOST}/url",
                        params={"url": text},
                        headers={
                            "X-RapidAPI-Key": api_key,
                            "X-RapidAPI-Host": RAPIDAPI_HOST
                        },
                        timeout=20
                    )
                    res.raise_for_status()
                    resp_json = res.json()
                    
                    if not resp_json or not isinstance(resp_json, list) or len(resp_json) == 0:
                        raise Exception("Invalid API response")
                    
                    data = resp_json[0]
                    filename = data.get("file_name", "file")
                    dlink = data.get("direct_link") or data.get("link")
                    alt_link = data.get("link")
                    filesize = int(data.get("sizebytes", 0))
                    thumb_url = data.get("thumbnail")

                    # Sanitize filename
                    filename = re.sub(r'[\\/*?:"<>|]', "_", filename).replace(" ", "_")
                    if '.' not in filename:
                        # Try to guess extension from common patterns
                        if "video" in filename.lower():
                            filename += ".mp4"
                        elif "image" in filename.lower() or "photo" in filename.lower():
                            filename += ".jpg"
                        else:
                            filename += ".bin"

                    file_path = f"{user.id}_{filename}"
                    thumb_path = f"{file_path}.jpg"


                    download_success = False
                    download_urls = set()
                    
                    # Add both links if available
                    if dlink:
                        download_urls.add(dlink)
                    if alt_link and alt_link != dlink:
                        download_urls.add(alt_link)
                        
                    # Try each URL until one works
                    for download_url in download_urls:
                        try:
                            # Check if canceled
                            if user.id in DOWNLOAD_QUEUE.cancel_requests:
                                DOWNLOAD_QUEUE.cancel_requests.discard(user.id)
                                await msg.edit_text("❌ Download canceled by user.")
                                return
                                
                            # Download file and get content type from headers
                            content_type = await download_file_with_progress(download_url, file_path, msg, filename, filesize)
                            download_success = True
                            break
                        except Exception as e:
                            last_error = e
                            logger.warning(f"Download failed from {download_url[:50]}...: {e}")
                            # Clean up partially downloaded file
                            if os.path.exists(file_path):
                                try:
                                    os.remove(file_path)
                                except:
                                    pass
                    
                    if not download_success:
                        raise Exception(f"All download attempts failed: {last_error}")
                    
                    # Detect file type using multiple methods
                    mime_type = detect_file_type(file_path)
                    logger.info(f"Detected MIME type: {mime_type} for {file_path}")

                    # Prepare caption
                    caption = f"🎬 {filename}\n\n📦 Size: {human_size(filesize)}"

                    # Upload to Telegram
                    try:
                        await msg.edit_text("✅ Download complete! Starting upload...")
                        await asyncio.sleep(2)
                        await msg.delete()
                    except:
                        pass
                    
                    # Determine if it's a video
                    is_video = mime_type.startswith("video/")
                    width, height = (None, None)
                    
                    # Generate thumbnail for videos
                    if is_video:
                        # Run in thread to prevent blocking
                        await asyncio.to_thread(generate_thumbnail, file_path, thumb_path)
                        width, height = await asyncio.to_thread(get_video_dimensions, file_path)
                        logger.info(f"Video dimensions: {width}x{height}")

                    # Upload progress message
                    upload_msg = await update.message.reply_text("📤 Uploading... 0%")
                    last_progress_update = time.time()
                    last_percent_sent = 0
                    
                    # Create a progress callback
                    def progress_callback(current, total):
                        nonlocal last_progress_update, last_percent_sent
                        percent = int(current * 100 / total)
                        
                        # Only update if percentage changed or 5 seconds passed
                        if percent > last_percent_sent or time.time() - last_progress_update > 5:
                            try:
                                asyncio.create_task(upload_msg.edit_text(f"📤 Uploading... {percent}%"))
                                last_progress_update = time.time()
                                last_percent_sent = percent
                            except Exception:
                                pass  # Avoid flooding errors
                    
                    # Upload to user
                    try:
                        # Upload file using our custom function
                        await upload_file(
                            context=context,
                            chat_id=update.effective_chat.id,
                            file_path=file_path,
                            thumb_path=thumb_path,
                            caption=caption,
                            is_video=is_video,
                            width=width,
                            height=height
                        )
                        
                        # Final update
                        await upload_msg.edit_text("✅ Upload complete!")
                        await asyncio.sleep(2)
                        await upload_msg.delete()
                        
                        # Update download stats
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
                        
                        # Update API key stats
                        await api_stats_collection.update_one(
                            {"api_key": api_key},
                            {
                                "$inc": {"success_count": 1},
                                "$set": {"last_used": datetime.datetime.now()}
                            }
                        )

                    except Exception as e:
                        await upload_msg.edit_text(f"❌ Upload failed: {str(e)}")
                        # Update stats for failed download
                        await stats_collection.update_one({}, {
                            "$inc": {
                                "total_downloads": 1,
                                "failed_downloads": 1
                            }
                        })
                        
                        # Update API key stats
                        await api_stats_collection.update_one(
                            {"api_key": api_key},
                            {
                                "$inc": {"failure_count": 1},
                                "$set": {"last_used": datetime.datetime.now()}
                            }
                        )
                        raise

                    # Mirror to channel
                    if MIRROR_CHANNEL_ID:
                        mirror_caption = f"{caption}"
                        try:
                            await upload_file(
                                context=context,
                                chat_id=MIRROR_CHANNEL_ID,
                                file_path=file_path,
                                thumb_path=thumb_path,
                                caption=mirror_caption,
                                is_video=is_video,
                                width=width,
                                height=height
                            )
                        except Exception as e:
                            logger.error(f"Mirror error: {e}")
                            if LOG_CHANNEL_ID:
                                try:
                                    await context.bot.send_message(
                                        LOG_CHANNEL_ID,
                                        f"❌ Mirror failed for {filename}\nError: {str(e)}"
                                    )
                                except:
                                    pass

                    # Send to link channel
                    if LINK_CHANNEL_ID:
                        try:
                            await context.bot.send_message(
                                LINK_CHANNEL_ID,
                                f"🌐 New link: [{filename}]({text}) \nby {user.first_name}",
                                parse_mode="Markdown"
                            )
                        except Exception as e:
                            logger.error(f"Link channel error: {e}")

                    success = True
                    break

                except Exception as e:
                    last_error = e
                    logger.error(f"API key {api_key} failed: {str(e)}")
                    continue
                finally:
                    # Cleanup files
                    for path in [file_path, thumb_path]:
                        if path and os.path.exists(path):
                            try:
                                os.remove(path)
                            except Exception as e:
                                logger.error(f"Error deleting file {path}: {e}")

            # Handle failure after trying all keys
            if not success:
                error_msg = f"❌ Download failed: {str(last_error)[:200]}"
                # Update stats for failed download
                await stats_collection.update_one({}, {
                    "$inc": {
                        "total_downloads": 1,
                        "failed_downloads": 1
                    }
                })
                
                if data:
                    filename = data.get("file_name", "file")
                    filesize = int(data.get("sizebytes", 0))
                    dlink = data.get("direct_link")
                    alt_link = data.get("link")
                    
                    caption = f"❌ Failed to download\n\n🎬 {filename}\n\n📦 Size: {human_size(filesize)}\n\nTry these links:"
                    buttons = []
                    
                    if dlink:
                        buttons.append([InlineKeyboardButton("Direct Link", url=dlink)])
                    if alt_link and alt_link != dlink:
                        buttons.append([InlineKeyboardButton("Alt Link", url=alt_link)])
                    
                    # Add alternative services
                    buttons.append([InlineKeyboardButton("Try Terabox.tel", url=f"https://terabox.tel/?url={text}")])
                    buttons.append([InlineKeyboardButton("File Downloader Bot", url="https://t.me/File2LinkOfficialBot")])
                    
                    keyboard = InlineKeyboardMarkup(buttons) if buttons else None
                    
                    try:
                        thumb_url = data.get("thumbnail") or START_IMAGE
                        await msg.edit_media(
                            media=InputMediaPhoto(media=thumb_url, caption=caption),
                            reply_markup=keyboard
                        )
                    except:
                        try:
                            await msg.edit_text(caption, reply_markup=keyboard)
                        except Exception as e:
                            logger.error(f"Failed to edit message: {e}")
                else:
                    try:
                        await msg.edit_text(error_msg)
                    except:
                        pass
        except Exception as e:
            logger.error(f"Download task failed: {e}")
        finally:
            # Remove cancel button
            try:
                await msg.edit_reply_markup(reply_markup=None)
            except:
                pass

    # Add to queue
    DOWNLOAD_QUEUE.add_task(user.id, download_task())
    
    # Notify user about queue position
    position = DOWNLOAD_QUEUE.get_queue_position(user.id)
    if position > 0:
        queue_msg = f"⏳ Your download is in queue.\nPosition: #{position}"
        try:
            await msg.edit_text(f"{msg.text}\n\n{queue_msg}", reply_markup=cancel_keyboard)
        except Exception as e:
            logger.warning(f"Queue notification failed: {e}")

# Cancel download handler
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = int(query.data.split('_')[1])
    except:
        await query.answer("Invalid request!")
        return
        
    if query.from_user.id != user_id:
        await query.answer("You can only cancel your own downloads!")
        return
        
    # Cancel the download
    canceled = DOWNLOAD_QUEUE.cancel_task(user_id)
    if canceled:
        await query.edit_message_text("❌ Download canceled by user.")
    else:
        await query.answer("Download not found or already started!")

# Set up telegram bot application
def main():
    # Initialize mimetypes
    mimetypes.init()
    
    # Create application
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("astatus", astatus_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(cancel_handler, pattern=r"^cancel_\d+$"))
    
    # Create a single event loop for all operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Initialize database
        loop.run_until_complete(init_database())
        
        # Start the bot
        logger.info("Bot is running...")
        loop.run_until_complete(app.run_polling())
    except Exception as e:
        logger.critical(f"Bot crashed: {e}")
        if LOG_CHANNEL_ID:
            try:
                loop.run_until_complete(app.bot.send_message(
                    chat_id=LOG_CHANNEL_ID,
                    text=f"🔥 Bot crashed with error:\n```\n{str(e)[:1000]}\n```",
                    parse_mode="Markdown"
                ))
            except Exception as inner_e:
                logger.error(f"Failed to send crash report: {inner_e}")
        raise
    finally:
        # Properly close the event loop
        loop.close()

if __name__ == "__main__":
    main()
