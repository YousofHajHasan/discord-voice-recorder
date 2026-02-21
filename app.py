import discord
from discord.ext import commands, tasks
from discord.sinks import MP3Sink, Filters, AudioData
import os
import time
import asyncio
import subprocess
import glob
import io

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
CHECK_INTERVAL = 3

raw_allowed = os.getenv('ALLOWED_CHANNELS', '')
if raw_allowed:
    ALLOWED_CHANNELS = [int(x.strip()) for x in raw_allowed.split(',') if x.strip().isdigit()]
else:
    ALLOWED_CHANNELS = []

raw_allowed_users = os.getenv('ALLOWED_USERS', '')
if raw_allowed_users:
    ALLOWED_USERS = set(int(x.strip()) for x in raw_allowed_users.split(',') if x.strip().isdigit())
else:
    ALLOWED_USERS = set()

print(f"✅ Configuration Loaded:")
print(f"   - Chunk Time: {CHUNK_TIME}s")
print(f"   - Check Interval: {CHECK_INTERVAL}s")
print(f"   - Channel Whitelist: {ALLOWED_CHANNELS if ALLOWED_CHANNELS else 'ALL CHANNELS'}")
print(f"   - User Whitelist: {ALLOWED_USERS if ALLOWED_USERS else 'EMPTY (no one will be recorded)'}")

# --- SETUP ---
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# State Management
guild_cooldowns = {}
guild_chunk_tasks = {}  # Tracks the periodic chunk task per guild

# --- HELPER FUNCTIONS ---

def ensure_folder(user_id, user_name):
    """Creates a folder for the user: Recordings/Username_ID"""
    safe_name = "".join(x for x in user_name if x.isalnum())
    folder_path = os.path.join(BASE_DIR, f"{safe_name}_{user_id}")
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

def merge_user_audio(folder_path):
    """Appends all _part*.mp3 chunk files into a single persistent Full_Recording.mp3"""
    chunks = sorted(glob.glob(os.path.join(folder_path, "*_part*.mp3")))
    if not chunks:
        return

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

    print(f"   Merging {len(chunks)} chunk(s) into {master_file}...")
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

def startup_cleanup():
    """
    Runs once on startup before joining any VC.
    - Creates Recordings/ if missing
    - Converts leftover .pcm files to .mp3
    - Merges unmerged chunks into Full_Recording.mp3
    """
    print(f"🔍 Running startup cleanup on '{BASE_DIR}'...")
    os.makedirs(BASE_DIR, exist_ok=True)

    user_folders = [
        os.path.join(BASE_DIR, d)
        for d in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, d))
    ]

    if not user_folders:
        print("   No existing folders found. Starting fresh.")
        return

    for folder in user_folders:
        folder_name = os.path.basename(folder)

        # Convert any leftover .pcm files (from old code)
        for pcm in glob.glob(os.path.join(folder, "*.pcm")):
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

        chunks = sorted(glob.glob(os.path.join(folder, "*_part*.mp3")))
        if chunks:
            print(f"   Merging chunks in '{folder_name}'...")
            merge_user_audio(folder)
            print(f"   ✅ '{folder_name}' cleaned up.")
        else:
            print(f"   ✅ '{folder_name}' already clean.")

    print("✅ Startup cleanup complete. Ready to join VCs.")

# --- CUSTOM SINK ---

class WhitelistMP3Sink(MP3Sink):
    """
    Subclass of Pycord's MP3Sink that only writes audio data
    for users in the ALLOWED_USERS whitelist.
    All other users' audio is silently discarded.
    """

    @Filters.container
    def write(self, data, user):
        # user here is a user ID integer
        if not ALLOWED_USERS or user not in ALLOWED_USERS:
            return  # Silently discard non-whitelisted users

        if user not in self.audio_data:
            self.audio_data[user] = AudioData(io.BytesIO())

        self.audio_data[user].write(data)

# --- RECORDING CALLBACK ---

async def recording_finished(sink: WhitelistMP3Sink, guild: discord.Guild, chunk_num_map: dict):
    """
    Called by Pycord when stop_recording() is invoked.
    By this point, Pycord's cleanup() has already called format_audio() on all
    audio data — so the audio in sink.audio_data is already formatted MP3 bytes.
    We just need to read and save each user's file.
    """
    print(f"📼 Processing recordings for {guild.name}...")

    folders_to_merge = set()

    for user_id, audio in sink.audio_data.items():
        member = guild.get_member(user_id)
        user_name = member.name if member else str(user_id)

        folder = ensure_folder(user_id, user_name)
        chunk_num = chunk_num_map.get(user_id, 1)
        chunk_file = os.path.join(folder, f"{user_name}_part{chunk_num}.mp3")

        # Audio is already formatted MP3 — just seek and save
        try:
            audio.file.seek(0)
            data = audio.file.read()
            if not data:
                print(f"   ⚠️ Empty audio for {user_name}, skipping.")
                continue
            with open(chunk_file, 'wb') as f:
                f.write(data)
            print(f"   ✅ Saved chunk for {user_name}: {os.path.basename(chunk_file)}")
        except Exception as e:
            print(f"   ❌ Failed to save audio for {user_name}: {e}")
            continue

        folders_to_merge.add(folder)

    # Merge chunks into Full_Recording.mp3
    for folder in folders_to_merge:
        merge_user_audio(folder)

    print(f"✅ Recording processing done for {guild.name}")

# --- CHANNEL LOGIC ---

def is_channel_interesting(channel):
    """Returns True if a whitelisted undeafened user is present and channel is allowed."""
    if not channel or not channel.members:
        return False
    if not ALLOWED_USERS:
        return False
    if ALLOWED_CHANNELS and channel.id not in ALLOWED_CHANNELS:
        return False
    for member in channel.members:
        if member.bot:
            continue
        if member.id in ALLOWED_USERS and not member.voice.self_deaf and not member.voice.deaf:
            return True
    return False

def find_interesting_channel(guild):
    """Finds first interesting voice channel in guild, or None."""
    for channel in guild.voice_channels:
        if is_channel_interesting(channel):
            return channel
    return None

async def join_and_record(channel: discord.VoiceChannel):
    """Joins a voice channel and starts recording."""
    try:
        print(f"Joining {channel.name} in {channel.guild.name}")
        vc = await channel.connect()

        # chunk_num_map tracks which chunk number each user is on
        chunk_num_map = {}

        async def on_recording_done(sink, *args):
            await recording_finished(sink, channel.guild, chunk_num_map)

        vc.start_recording(
            WhitelistMP3Sink(),
            on_recording_done,
            sync_start=True
        )

        # Store chunk_num_map on the vc object so monitor_channels can access it
        vc._chunk_num_map = chunk_num_map
        vc._chunk_start_time = time.time()

        print(f"✅ Recording started in {channel.name}")
        return True
    except Exception as e:
        print(f"❌ Failed to join {channel.name}: {e}")
        return False

async def leave_and_cleanup(guild: discord.Guild):
    """Stops recording, saves files, and disconnects."""
    vc = guild.voice_client
    if not vc:
        return

    print(f"Leaving {vc.channel.name} in {guild.name}")

    try:
        vc.stop_recording()  # Triggers recording_finished callback
    except Exception as e:
        print(f"⚠️ Error stopping recording: {e}")

    await asyncio.sleep(3)  # Give callback time to finish saving files

    try:
        await vc.disconnect()
    except Exception:
        pass

    guild_cooldowns[guild.id] = time.time() + COOLDOWN_SECONDS
    print(f"✅ Left {guild.name}. Cooldown: {COOLDOWN_SECONDS}s")

# --- CHUNK ROTATION ---

async def rotate_chunk(guild: discord.Guild):
    """
    Stops current recording, waits for the callback to finish saving the chunk,
    then starts a fresh recording immediately. Does not disconnect.
    """
    vc = guild.voice_client
    if not vc or not vc.recording:
        return

    print(f"🔄 Rotating chunk for {guild.name}...")

    old_chunk_num_map = getattr(vc, '_chunk_num_map', {})

    # Next chunk numbers = current + 1 for each known user
    new_chunk_num_map = {uid: num + 1 for uid, num in old_chunk_num_map.items()}

    # Use an event to know when the callback is fully done before starting next chunk
    chunk_done_event = asyncio.Event()

    async def on_chunk_done(sink, *args):
        await recording_finished(sink, guild, old_chunk_num_map)
        chunk_done_event.set()

    try:
        vc.stop_recording()

        # Wait for callback to fully finish (up to 30s)
        try:
            await asyncio.wait_for(chunk_done_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            print(f"⚠️ Chunk callback timed out for {guild.name}, continuing anyway...")

        # Start fresh recording for next chunk
        vc._chunk_num_map = new_chunk_num_map
        vc._chunk_start_time = time.time()

        async def on_recording_done(sink, *args):
            await recording_finished(sink, guild, vc._chunk_num_map)

        vc.start_recording(
            WhitelistMP3Sink(),
            on_recording_done,
            sync_start=True
        )
        print(f"✅ Chunk rotated for {guild.name}")
    except Exception as e:
        print(f"❌ Chunk rotation failed for {guild.name}: {e}")

# --- MAIN POLLING LOOP ---

@tasks.loop(seconds=CHECK_INTERVAL)
async def monitor_channels():
    """Continuously checks all guilds for interesting channels."""
    for guild in bot.guilds:
        vc = guild.voice_client
        guild_id = guild.id
        current_time = time.time()

        in_cooldown = guild_id in guild_cooldowns and current_time < guild_cooldowns[guild_id]

        # Clean up expired cooldowns
        if in_cooldown and current_time >= guild_cooldowns.get(guild_id, 0):
            del guild_cooldowns[guild_id]
            in_cooldown = False

        target_channel = find_interesting_channel(guild)

        # --- SCENARIO 1: Bot is connected ---
        if vc and vc.channel:
            if not is_channel_interesting(vc.channel):
                print(f"No whitelisted users in {vc.channel.name}, leaving...")
                await leave_and_cleanup(guild)

            # Check if it's time to rotate the chunk
            elif hasattr(vc, '_chunk_start_time'):
                elapsed = current_time - vc._chunk_start_time
                if elapsed >= CHUNK_TIME:
                    await rotate_chunk(guild)

        # --- SCENARIO 2: Bot is idle ---
        elif target_channel and not in_cooldown:
            print(f"Found active channel: {target_channel.name}")
            await join_and_record(target_channel)

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
    await ctx.send(f"✅ User `{user_id}` added to whitelist and saved to .env. ({len(ALLOWED_USERS)} user(s) tracked)")

@bot.command()
async def unallow(ctx, user_id: int):
    """Remove a user ID from the recording whitelist. Usage: !unallow 123456789"""
    ALLOWED_USERS.discard(user_id)
    save_whitelist_to_env()
    if ALLOWED_USERS:
        await ctx.send(f"🗑️ User `{user_id}` removed and .env updated. ({len(ALLOWED_USERS)} user(s) remaining)")
    else:
        await ctx.send(f"🗑️ User `{user_id}` removed. Whitelist is now empty — **no one** will be recorded.")

@bot.command()
async def whitelist(ctx):
    """Show the current user recording whitelist. Usage: !whitelist"""
    if not ALLOWED_USERS:
        await ctx.send("📋 Whitelist is empty — **no one** is being recorded. Use `!allow <user_id>` to add someone.")
    else:
        ids = '\n'.join(f"• `{uid}`" for uid in ALLOWED_USERS)
        await ctx.send(f"📋 **Recording only these users ({len(ALLOWED_USERS)} total):**\n{ids}")

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