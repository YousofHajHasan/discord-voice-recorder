import discord
from discord.ext import commands, voice_recv
import os
import time
import asyncio
import subprocess
import glob
import datetime

# --- CONFIGURATION ---
# Try to load .env for local testing, but skip if missing (Docker friendly)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv('DISCORD_TOKEN')
CHUNK_TIME = int(os.getenv('CHUNK_TIME', 300))  # Default: 5 minutes
BASE_DIR = os.getenv('BASE_DIR', 'Recordings')
COOLDOWN_SECONDS = 10

# Parse Allowed Channels (e.g., "12345,67890") into a list of integers
raw_allowed = os.getenv('ALLOWED_CHANNELS', '')
if raw_allowed:
    ALLOWED_CHANNELS = [int(x.strip()) for x in raw_allowed.split(',') if x.strip().isdigit()]
else:
    ALLOWED_CHANNELS = [] # Empty list = Allow All Channels

print(f"✅ Configuration Loaded:")
print(f"   - Chunk Time: {CHUNK_TIME}s")
print(f"   - Whitelist: {ALLOWED_CHANNELS if ALLOWED_CHANNELS else 'ALL CHANNELS'}")

# --- SETUP ---
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True  # CRITICAL for tracking users

bot = commands.Bot(command_prefix="!", intents=intents)

# State Management
user_sessions = {}
last_action_time = 0
processing_lock = False  # Prevents joining while merging files

# --- HELPER FUNCTIONS ---

def ensure_folder(user):
    """Creates a folder for the user: Recordings/Username_ID"""
    safe_name = "".join(x for x in user.name if x.isalnum())
    folder_path = os.path.join(BASE_DIR, f"{safe_name}_{user.id}")
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

def convert_and_delete_pcm(pcm_filename):
    """Background task: Convert PCM -> MP3, then delete PCM"""
    if not os.path.exists(pcm_filename): return
    mp3_filename = pcm_filename.replace('.pcm', '.mp3')
    
    # Command: Convert AND (&&) Delete only if successful
    cmd = (
        f"ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"{pcm_filename}\" \"{mp3_filename}\" "
        f"-loglevel error && rm \"{pcm_filename}\""
    )
    subprocess.Popen(cmd, shell=True)

def merge_user_audio(folder_path):
    """Merges all chunk files into one Full_Recording.mp3"""
    chunks = sorted(glob.glob(os.path.join(folder_path, "*_part*.mp3")))
    if not chunks: return

    list_file = os.path.join(folder_path, "files_to_merge.txt")
    with open(list_file, 'w') as f:
        for chunk in chunks:
            f.write(f"file '{os.path.abspath(chunk)}'\n")

    output_path = os.path.join(folder_path, "Full_Recording.mp3")
    print(f"Merge: Joining {len(chunks)} files in {folder_path}...")
    
    subprocess.run([
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0', 
        '-i', list_file, '-c', 'copy', output_path
    ], stderr=subprocess.DEVNULL)
    
    os.remove(list_file)

# --- RECORDING CALLBACK ---

def callback_function(user, data: voice_recv.VoiceData):
    """Runs continuously receiving audio packets"""
    if not data.pcm: return
    
    user_id = user.id
    current_time = time.time()
    
    # 1. Start Session
    if user_id not in user_sessions:
        folder = ensure_folder(user)
        filename = os.path.join(folder, f"{user.name}_part1.pcm")
        user_sessions[user_id] = {
            'start_time': current_time, 
            'chunk_num': 1,
            'current_file': filename,
            'folder': folder,
            'user_name': user.name
        }

    session = user_sessions[user_id]

    # 2. Check Chunk Timer
    if current_time - session['start_time'] > CHUNK_TIME:
        old_file = session['current_file']
        
        # Rotate File
        session['chunk_num'] += 1
        session['start_time'] = current_time
        new_filename = os.path.join(session['folder'], f"{session['user_name']}_part{session['chunk_num']}.pcm")
        session['current_file'] = new_filename
        
        # Convert Old Chunk
        convert_and_delete_pcm(old_file)

    # 3. Write Audio
    with open(session['current_file'], 'ab') as f:
        f.write(data.pcm)

async def scan_and_join_active_channel(guild):
    """
    Scans the server for any 'interesting' channels and joins the first one found.
    Used to catch users who undeafened while the bot was busy cleaning up.
    """
    for channel in guild.voice_channels:
        if is_channel_interesting(channel):
            print(f"🔄 Rescan found active channel: {channel.name}. Joining...")
            try:
                vc = await channel.connect(cls=voice_recv.VoiceRecvClient)
                vc.listen(voice_recv.BasicSink(callback_function))
                return # Stop after joining one
            except Exception as e:
                print(f"❌ Rescan join failed: {e}")


async def stop_recording_and_cleanup(guild):
    global processing_lock, last_action_time
    
    if not guild.voice_client: return
    
    print(f"🛑 Stopping recording in {guild.name}...")
    processing_lock = True 
    
    guild.voice_client.stop()
    await guild.voice_client.disconnect()

    # 1. Convert remaining PCMs
    folders_to_merge = set()
    for uid, session in user_sessions.items():
        convert_and_delete_pcm(session['current_file'])
        folders_to_merge.add(session['folder'])
    
    user_sessions.clear()

    # 2. Wait for ffmpeg to finish
    await asyncio.sleep(2) 

    # 3. Merge files
    for folder in folders_to_merge:
        merge_user_audio(folder)
    
    print(f"✅ Cleanup done. Cooldown started ({COOLDOWN_SECONDS}s)...")
    
    # 4. Release Lock after cooldown
    await asyncio.sleep(COOLDOWN_SECONDS)
    last_action_time = time.time()
    processing_lock = False
    
    # --- NEW FIX STARTS HERE ---
    print("🟢 Bot ready. Scanning for missed events...")
    # Immediately check if anyone is waiting (e.g., they undeafened during cleanup)
    await scan_and_join_active_channel(guild)

# --- SENTRY & WHITELIST LOGIC ---
def is_channel_interesting(channel):
    """Returns True if channel has active (undeafened) humans and is allowed."""
    if not channel or not channel.members: return False
    
    # 1. Whitelist Check
    if ALLOWED_CHANNELS and channel.id not in ALLOWED_CHANNELS:
        return False

    # 2. Activity Check
    for member in channel.members:
        if member.bot: continue
        if not member.voice.self_deaf and not member.voice.deaf:
            return True # Found a valid target!
            
    return False

@bot.event
async def on_voice_state_update(member, before, after):
    """Triggered on join/leave/mute/deafen."""
    if member.bot: return
    if processing_lock: return 

    guild = member.guild
    vc = guild.voice_client

    # --- SCENARIO 1: Bot is IDLE (Not in VC) ---
    if not vc:
        if after.channel and is_channel_interesting(after.channel):
            if time.time() - last_action_time < COOLDOWN_SECONDS:
                return

            print(f"👀 Detected active VC: {after.channel.name}. Joining...")
            try:
                vc = await after.channel.connect(cls=voice_recv.VoiceRecvClient)
                vc.listen(voice_recv.BasicSink(callback_function))
            except Exception as e:
                print(f"❌ Failed to join: {e}")

    # --- SCENARIO 2: Bot is ACTIVE (Already in VC) ---
    elif vc:
        # Check if current channel became empty/boring
        if not is_channel_interesting(vc.channel):
            print(f"zzz Channel {vc.channel.name} is empty/deafened. Leaving...")
            await stop_recording_and_cleanup(guild)
            
            # --- SCENARIO 3: Scan for other active channels ---
            # After leaving, check if there is ANOTHER allowed channel to join
            # The next event loop will pick it up if we release the lock
            pass

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("Sentry Mode Active: Watching for undeafened humans...")

# --- RUN ---
if TOKEN:
    bot.run(TOKEN)
else:
    print("Error: DISCORD_TOKEN not found.")