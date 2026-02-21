import discord
from discord.ext import commands, voice_recv, tasks
import os
import time
import asyncio
import subprocess
import glob

# Trigger auto build

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
CHECK_INTERVAL = 3      # Check every 3 seconds
SILENCE_THRESHOLD = 30  # Seconds of no packets before attempting a listener restart
RESTART_COOLDOWN = 20   # Seconds to wait between listener restarts (prevents in/out loop)

raw_allowed = os.getenv('ALLOWED_CHANNELS', '')
if raw_allowed:
    ALLOWED_CHANNELS = [int(x.strip()) for x in raw_allowed.split(',') if x.strip().isdigit()]
else:
    ALLOWED_CHANNELS = []

# User Whitelist: Only record these user IDs (empty = record no one)
raw_allowed_users = os.getenv('ALLOWED_USERS', '')
if raw_allowed_users:
    ALLOWED_USERS = set(int(x.strip()) for x in raw_allowed_users.split(',') if x.strip().isdigit())
else:
    ALLOWED_USERS = set()

print(f"✅ Configuration Loaded:")
print(f"   - Chunk Time: {CHUNK_TIME}s")
print(f"   - Check Interval: {CHECK_INTERVAL}s")
print(f"   - Silence Threshold: {SILENCE_THRESHOLD}s")
print(f"   - Restart Cooldown: {RESTART_COOLDOWN}s")
print(f"   - Channel Whitelist: {ALLOWED_CHANNELS if ALLOWED_CHANNELS else 'ALL CHANNELS'}")
print(f"   - User Whitelist: {ALLOWED_USERS if ALLOWED_USERS else 'EMPTY (no one will be recorded)'}")

# --- SETUP ---
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# State Management
user_sessions = {}
guild_cooldowns = {}    # When each guild can rejoin after leaving
last_packet_time = {}   # Last audio packet received per guild
last_restart_time = {}  # Last listener restart per guild (enforces RESTART_COOLDOWN)

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

def startup_cleanup():
    """
    Runs once on bot startup before joining any VC.
    - Creates Recordings/ folder if missing
    - Scans all user subfolders for leftover .pcm and unmerged _part*.mp3 files
    - Converts any .pcm files to .mp3 (blocking, so they're ready to merge)
    - Merges all chunks into Full_Recording.mp3
    - Leaves every folder in a clean state: only Full_Recording.mp3
    """
    print(f"🔍 Running startup cleanup on '{BASE_DIR}'...")

    os.makedirs(BASE_DIR, exist_ok=True)

    user_folders = [
        os.path.join(BASE_DIR, d)
        for d in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, d))
    ]

    if not user_folders:
        print("   No existing user folders found. Starting fresh.")
        return

    for folder in user_folders:
        folder_name = os.path.basename(folder)

        # Step 1: Convert any leftover .pcm files to .mp3 (blocking)
        pcm_files = glob.glob(os.path.join(folder, "*.pcm"))
        for pcm in pcm_files:
            mp3 = pcm.replace('.pcm', '.mp3')
            print(f"   Converting leftover PCM: {os.path.basename(pcm)}")
            result = subprocess.run(
                f"ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"{pcm}\" \"{mp3}\" -loglevel error",
                shell=True
            )
            if result.returncode == 0 and os.path.exists(mp3):
                os.remove(pcm)
            else:
                print(f"   ⚠️ Failed to convert {os.path.basename(pcm)}, skipping.")

        # Step 2: Merge any _part*.mp3 chunks into Full_Recording.mp3
        chunks = sorted(glob.glob(os.path.join(folder, "*_part*.mp3")))
        if chunks:
            print(f"   Merging {len(chunks)} chunk(s) in '{folder_name}'...")
            merge_user_audio(folder)
            print(f"   ✅ '{folder_name}' cleaned up.")
        else:
            print(f"   ✅ '{folder_name}' already clean.")

    print("✅ Startup cleanup complete. Ready to join VCs.")

def merge_user_audio(folder_path):
    """Appends all new chunk files into a single persistent Full_Recording.mp3"""
    chunks = sorted(glob.glob(os.path.join(folder_path, "*_part*.mp3")))
    if not chunks: return

    master_file = os.path.join(folder_path, "Full_Recording.mp3")
    list_file = os.path.join(folder_path, "files_to_merge.txt")

    files_to_concat = []
    if os.path.exists(master_file):
        temp_master = os.path.join(folder_path, "_temp_master.mp3")
        os.rename(master_file, temp_master)
        files_to_concat.append(temp_master)
    else:
        temp_master = None

    files_to_concat.extend(chunks)

    with open(list_file, 'w') as f:
        for filepath in files_to_concat:
            f.write(f"file '{os.path.abspath(filepath)}'\n")

    print(f"Appending {len(chunks)} chunk(s) into {master_file}...")

    subprocess.run([
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0',
        '-i', list_file, '-c', 'copy', master_file
    ], stderr=subprocess.DEVNULL)

    os.remove(list_file)

    if temp_master and os.path.exists(temp_master):
        os.remove(temp_master)
    for chunk in chunks:
        if os.path.exists(chunk):
            os.remove(chunk)

# --- RECORDING CALLBACK ---

def callback_function(user, data: voice_recv.VoiceData):
    """Runs continuously receiving audio packets"""
    if not data.pcm: return

    # Guard: user may be None if Discord hasn't resolved the member yet
    if user is None: return

    # User whitelist check — if whitelist is empty or user not in it, skip
    if not ALLOWED_USERS or user.id not in ALLOWED_USERS:
        return

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
    """Returns True if channel has an undeafened whitelisted user and is allowed."""
    if not channel or not channel.members:
        return False

    # Don't join if whitelist is empty
    if not ALLOWED_USERS:
        return False

    # Channel whitelist check
    if ALLOWED_CHANNELS and channel.id not in ALLOWED_CHANNELS:
        return False

    # Only join if at least one whitelisted user is present and undeafened
    for member in channel.members:
        if member.bot:
            continue
        if member.id in ALLOWED_USERS and not member.voice.self_deaf and not member.voice.deaf:
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
        print(f"Joining {channel.name} in {channel.guild.name}")
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

    print(f"Leaving {vc.channel.name} in {guild.name}")

    vc.stop()
    await vc.disconnect()

    # Clear timers
    last_packet_time.pop(guild.id, None)
    last_restart_time.pop(guild.id, None)

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

        in_cooldown = guild_id in guild_cooldowns and current_time < guild_cooldowns[guild_id]
        target_channel = find_interesting_channel(guild)

        # --- SCENARIO 1: Bot is connected ---
        if vc and vc.channel:

            # Leave if no whitelisted user is present anymore
            if not is_channel_interesting(vc.channel):
                print(f"No whitelisted users in {vc.channel.name}, leaving...")
                await leave_and_cleanup(guild)

            # Check packet health — only restart if silence AND no whitelisted user is physically present
            elif guild_id in last_packet_time:
                silence_duration = current_time - last_packet_time[guild_id]

                if silence_duration > SILENCE_THRESHOLD:
                    # Double-check: are whitelisted users still in the channel?
                    # If yes, they're just quiet — don't restart, that's normal
                    whitelisted_present = any(
                        m.id in ALLOWED_USERS and not m.bot
                        for m in vc.channel.members
                    )

                    if whitelisted_present:
                        # Users are present but quiet — reset timer and wait
                        last_packet_time[guild_id] = current_time
                        print(f"⏳ Silence for {silence_duration:.0f}s but whitelisted users still present, resetting timer...")
                    else:
                        # No whitelisted users physically in channel — attempt listener restart
                        last_restart = last_restart_time.get(guild_id, 0)
                        if current_time - last_restart < RESTART_COOLDOWN:
                            remaining = RESTART_COOLDOWN - (current_time - last_restart)
                            print(f"⏳ Silence detected but restart cooldown active ({remaining:.0f}s remaining), waiting...")
                        else:
                            print(f"No packets for {silence_duration:.0f}s and no whitelisted users present, restarting listener...")
                            last_restart_time[guild_id] = current_time
                            try:
                                vc.stop()
                                await asyncio.sleep(0.5)
                                vc.listen(voice_recv.BasicSink(callback_function))
                                last_packet_time[guild_id] = current_time
                                print("✅ Listener restarted")
                            except Exception as e:
                                print(f"❌ Restart failed: {e}, reconnecting...")
                                await leave_and_cleanup(guild)
                                if target_channel and not in_cooldown:
                                    await join_channel(target_channel)

        # --- SCENARIO 2: Bot is idle ---
        elif target_channel and not in_cooldown:
            print(f"Found active channel: {target_channel.name}")
            await join_channel(target_channel)

        # Clean up expired cooldowns
        if in_cooldown and current_time >= guild_cooldowns[guild_id]:
            print(f"Cooldown expired for {guild.name}")
            del guild_cooldowns[guild_id]

# --- WHITELIST COMMANDS ---

def save_whitelist_to_env():
    """Rewrites the ALLOWED_USERS line in the .env file to persist the current whitelist."""
    env_path = '.env'
    new_line = f"ALLOWED_USERS={','.join(str(uid) for uid in ALLOWED_USERS)}\n"

    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            lines = f.readlines()
    else:
        lines = []

    found = False
    for i, line in enumerate(lines):
        if line.startswith('ALLOWED_USERS='):
            lines[i] = new_line
            found = True
            break

    if not found:
        lines.append(new_line)

    with open(env_path, 'w') as f:
        f.writelines(lines)

@bot.command()
async def allow(ctx, user_id: int):
    """Add a user ID to the recording whitelist. Usage: !allow 123456789"""
    ALLOWED_USERS.add(user_id)
    save_whitelist_to_env()
    await ctx.send(f"✅ User `{user_id}` added to the recording whitelist and saved to .env. ({len(ALLOWED_USERS)} user(s) tracked)")

@bot.command()
async def unallow(ctx, user_id: int):
    """Remove a user ID from the recording whitelist. Usage: !unallow 123456789"""
    ALLOWED_USERS.discard(user_id)
    save_whitelist_to_env()
    if ALLOWED_USERS:
        await ctx.send(f"🗑️ User `{user_id}` removed and .env updated. ({len(ALLOWED_USERS)} user(s) remaining)")
    else:
        await ctx.send(f"🗑️ User `{user_id}` removed and .env updated. Whitelist is now empty — **no one** will be recorded.")

@bot.command()
async def whitelist(ctx):
    """Show the current user recording whitelist. Usage: !whitelist"""
    if not ALLOWED_USERS:
        await ctx.send("📋 User whitelist is empty — **no one** is being recorded. Use `!allow <user_id>` to add someone.")
    else:
        ids = '\n'.join(f"• `{uid}`" for uid in ALLOWED_USERS)
        await ctx.send(f"📋 **Currently recording only these users ({len(ALLOWED_USERS)} total):**\n{ids}")

# --- BOT EVENTS ---

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    print(f"Monitoring {len(bot.guilds)} servers every {CHECK_INTERVAL}s")
    startup_cleanup()
    monitor_channels.start()

# --- RUN ---
if TOKEN:
    bot.run(TOKEN)
else:
    print("❌ Error: DISCORD_TOKEN not found")