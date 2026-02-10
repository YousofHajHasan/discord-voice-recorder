import discord
from discord.ext import commands, voice_recv, tasks
import os
import time
import asyncio
import subprocess
import glob

# --- CONFIGURATION ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv('DISCORD_TOKEN')
CHUNK_TIME = int(os.getenv('CHUNK_TIME', 300))
BASE_DIR = os.getenv('BASE_DIR', 'Recordings')
COOLDOWN_SECONDS = 10
CHECK_INTERVAL = 3  # Check every 3 seconds

raw_allowed = os.getenv('ALLOWED_CHANNELS', '')
if raw_allowed:
    ALLOWED_CHANNELS = [int(x.strip()) for x in raw_allowed.split(',') if x.strip().isdigit()]
else:
    ALLOWED_CHANNELS = []

print(f"✅ Configuration Loaded:")
print(f"   - Chunk Time: {CHUNK_TIME}s")
print(f"   - Check Interval: {CHECK_INTERVAL}s")
print(f"   - Whitelist: {ALLOWED_CHANNELS if ALLOWED_CHANNELS else 'ALL CHANNELS'}")

# --- SETUP ---
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# State Management
user_sessions = {}
guild_cooldowns = {}  # When each guild can rejoin
last_packet_time = {}

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
    
    cmd = (
        f"ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"{pcm_filename}\" \"{mp3_filename}\" "
        f"-loglevel error && rm \"{pcm_filename}\""
    )
    subprocess.Popen(cmd, shell=True)

def merge_user_audio(folder_path):
    """Merges all chunk files into one timestamped Full_Recording.mp3"""
    chunks = sorted(glob.glob(os.path.join(folder_path, "*_part*.mp3")))
    if not chunks: return

    list_file = os.path.join(folder_path, "files_to_merge.txt")
    with open(list_file, 'w') as f:
        for chunk in chunks:
            f.write(f"file '{os.path.abspath(chunk)}'\n")

    # Create timestamped filename
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(folder_path, f"Full_Recording_{timestamp}.mp3")
    
    print(f"🔀 Merging {len(chunks)} files into {output_path}...")
    
    subprocess.run([
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0', 
        '-i', list_file, '-c', 'copy', output_path
    ], stderr=subprocess.DEVNULL)
    
    os.remove(list_file)
    
    # Optional: Delete chunk files after merging to save space
    for chunk in chunks:
        os.remove(chunk)
        
# --- RECORDING CALLBACK ---

def callback_function(user, data: voice_recv.VoiceData):
    """Runs continuously receiving audio packets"""
    if not data.pcm: return
    
    # Update last packet timestamp
    if hasattr(user, 'guild') and user.guild:
        last_packet_time[user.guild.id] = time.time()
    
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
        
        session['chunk_num'] += 1
        session['start_time'] = current_time
        new_filename = os.path.join(session['folder'], f"{session['user_name']}_part{session['chunk_num']}.pcm")
        session['current_file'] = new_filename
        
        convert_and_delete_pcm(old_file)

    # 3. Write Audio
    with open(session['current_file'], 'ab') as f:
        f.write(data.pcm)

# --- CHANNEL LOGIC ---

def is_channel_interesting(channel):
    """Returns True if channel has active (undeafened) humans and is allowed."""
    if not channel or not channel.members: 
        return False
    
    # Whitelist Check
    if ALLOWED_CHANNELS and channel.id not in ALLOWED_CHANNELS:
        return False

    # Activity Check: Any undeafened human?
    for member in channel.members:
        if member.bot: 
            continue
        if not member.voice.self_deaf and not member.voice.deaf:
            return True
            
    return False

def find_interesting_channel(guild):
    """Finds first interesting channel in guild, or None"""
    for channel in guild.voice_channels:
        if is_channel_interesting(channel):
            return channel
    return None

async def join_channel(channel):
    """Joins a voice channel and starts recording"""
    try:
        print(f"🎤 Joining {channel.name} in {channel.guild.name}")
        vc = await channel.connect(cls=voice_recv.VoiceRecvClient)
        vc.listen(voice_recv.BasicSink(callback_function))
        last_packet_time[channel.guild.id] = time.time()
        return True
    except Exception as e:
        print(f"❌ Failed to join {channel.name}: {e}")
        return False

async def leave_and_cleanup(guild):
    """Leaves voice channel and processes recordings"""
    vc = guild.voice_client
    if not vc:
        return
    
    print(f"🛑 Leaving {vc.channel.name} in {guild.name}")
    
    # Stop and disconnect
    vc.stop()
    await vc.disconnect()
    
    # Clear packet timer
    if guild.id in last_packet_time:
        del last_packet_time[guild.id]
    
    # Convert remaining PCMs
    folders_to_merge = set()
    for uid, session in user_sessions.items():
        convert_and_delete_pcm(session['current_file'])
        folders_to_merge.add(session['folder'])
    
    user_sessions.clear()
    
    # Wait for conversions
    await asyncio.sleep(2)
    
    # Merge files
    for folder in folders_to_merge:
        merge_user_audio(folder)
    
    # Set cooldown
    guild_cooldowns[guild.id] = time.time() + COOLDOWN_SECONDS
    print(f"✅ Cleanup done. Cooldown: {COOLDOWN_SECONDS}s")

# --- MAIN POLLING LOOP ---

@tasks.loop(seconds=CHECK_INTERVAL)
async def monitor_channels():
    """Continuously checks all guilds for interesting channels"""
    
    for guild in bot.guilds:
        vc = guild.voice_client
        guild_id = guild.id
        current_time = time.time()
        
        # Check if in cooldown
        in_cooldown = guild_id in guild_cooldowns and current_time < guild_cooldowns[guild_id]
        
        # Find interesting channel
        target_channel = find_interesting_channel(guild)
        
        # --- SCENARIO 1: Bot is connected ---
        if vc and vc.channel:
            # Check if current channel is still interesting
            if not is_channel_interesting(vc.channel):
                print(f"💤 {vc.channel.name} became inactive")
                await leave_and_cleanup(guild)
            
            # Check if recording is working (packet health)
            elif guild_id in last_packet_time:
                silence_duration = current_time - last_packet_time[guild_id]
                if silence_duration > 10:
                    print(f"⚠️ No packets for {silence_duration:.0f}s, restarting listener...")
                    try:
                        vc.stop()
                        await asyncio.sleep(0.5)
                        vc.listen(voice_recv.BasicSink(callback_function))
                        last_packet_time[guild_id] = current_time
                        print("✅ Listener restarted")
                    except Exception as e:
                        print(f"❌ Restart failed: {e}, reconnecting...")
                        await leave_and_cleanup(guild)
                        if not in_cooldown:
                            await join_channel(target_channel)
        
        # --- SCENARIO 2: Bot is idle ---
        elif target_channel and not in_cooldown:
            print(f"🔍 Found active channel: {target_channel.name}")
            await join_channel(target_channel)
        
        # Clean up expired cooldowns
        if in_cooldown and current_time >= guild_cooldowns[guild_id]:
            print(f"⏰ Cooldown expired for {guild.name}")
            del guild_cooldowns[guild_id]

@bot.event
async def on_ready():
    print(f"🤖 Logged in as {bot.user}")
    print(f"📡 Monitoring {len(bot.guilds)} servers every {CHECK_INTERVAL}s")
    monitor_channels.start()

# --- RUN ---
if TOKEN:
    bot.run(TOKEN)
else:
    print("❌ Error: DISCORD_TOKEN not found")