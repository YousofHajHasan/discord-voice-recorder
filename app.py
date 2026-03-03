import discord
from discord.ext import commands, tasks
from discord.sinks import MP3Sink, Filters, AudioData
import os
import sys
import time
import asyncio
import subprocess
import glob
import io
from datetime import datetime
from enum import Enum, auto

import logging
logging.basicConfig(level=logging.DEBUG)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN           = os.getenv('DISCORD_TOKEN')
CHUNK_SECONDS   = int(os.getenv('CHUNK_TIME', 300))
BASE_DIR        = os.getenv('BASE_DIR', 'recordings')
CHECK_INTERVAL  = 3   # seconds between monitor ticks
LEAVE_COOLDOWN  = 10  # seconds before rejoining after a leave

raw_channels = os.getenv('ALLOWED_CHANNELS', '')
ALLOWED_CHANNELS: list = (
    [int(x.strip()) for x in raw_channels.split(',') if x.strip().isdigit()]
    if raw_channels else []
)

raw_users = os.getenv('ALLOWED_USERS', '')
ALLOWED_USERS: set = (
    {int(x.strip()) for x in raw_users.split(',') if x.strip().isdigit()}
    if raw_users else set()
)

print("✅ Configuration loaded:")
print(f"   Chunk time     : {CHUNK_SECONDS}s")
print(f"   Check interval : {CHECK_INTERVAL}s")
print(f"   Leave cooldown : {LEAVE_COOLDOWN}s")
print(f"   Channel filter : {ALLOWED_CHANNELS or 'ALL'}")
print(f"   User whitelist : {ALLOWED_USERS or 'EMPTY — no one will be recorded'}")

# ==============================================================================
# BOT STATE
# ==============================================================================

class State(Enum):
    IDLE      = auto()   # Not in any VC. Monitor loop scans for a target.
    RECORDING = auto()   # In a VC and actively recording.
    SAVING    = auto()   # stop_recording() fired; waiting for callback to finish.

# Per-guild state. Keyed by guild_id.
guild_state:       dict = {}   # guild_id -> State
guild_cooldown:    dict = {}   # guild_id -> timestamp when cooldown expires
user_paused_until: dict = {}   # user_id  -> timestamp when pause expires

def get_state(guild_id: int) -> State:
    return guild_state.get(guild_id, State.IDLE)

def set_state(guild_id: int, state: State):
    guild_state[guild_id] = state
    print(f"   [state] guild {guild_id} → {state.name}")

# ==============================================================================
# DISCORD BOT
# ==============================================================================

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ==============================================================================
# PAUSE HELPERS
# ==============================================================================

def is_user_paused(user_id: int) -> bool:
    expiry = user_paused_until.get(user_id)
    if expiry is None:
        return False
    if time.time() < expiry:
        return True
    del user_paused_until[user_id]
    return False

def pause_user(user_id: int, minutes: int):
    user_paused_until[user_id] = time.time() + minutes * 60

def unpause_user(user_id: int):
    user_paused_until.pop(user_id, None)

# ==============================================================================
# FILE HELPERS
# ==============================================================================

def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def user_folder(user_id: int, user_name: str) -> str:
    safe = "".join(c for c in user_name if c.isalnum())
    path = os.path.join(BASE_DIR, f"{safe}_{user_id}")
    os.makedirs(path, exist_ok=True)
    return path

def merge_chunks(folder: str, date_str: str):
    """
    Concatenates all *_part*.mp3 files in folder into YYYY-MM-DD.mp3,
    then deletes the part files.
    """
    chunks = sorted(glob.glob(os.path.join(folder, "*_part*.mp3")))
    if not chunks:
        return

    daily_file = os.path.join(folder, f"{date_str}.mp3")
    list_file  = os.path.join(folder, "_merge_list.txt")
    temp_file  = os.path.join(folder, f"_temp_{date_str}.mp3")

    # If a daily file already exists, prepend it
    concat_inputs = []
    if os.path.exists(daily_file):
        os.rename(daily_file, temp_file)
        concat_inputs.append(temp_file)
    concat_inputs.extend(chunks)

    with open(list_file, 'w') as f:
        for fp in concat_inputs:
            f.write(f"file '{os.path.abspath(fp)}'\n")

    print(f"   Merging {len(chunks)} chunk(s) into {os.path.basename(daily_file)}")
    subprocess.run(
        ['ffmpeg', '-y', '-f', 'concat', '-safe', '0',
         '-i', list_file, '-c', 'copy', daily_file],
        stderr=subprocess.DEVNULL
    )

    os.remove(list_file)
    if os.path.exists(temp_file):
        os.remove(temp_file)
    for chunk in chunks:
        if os.path.exists(chunk):
            os.remove(chunk)

def startup_cleanup():
    """
    On startup: convert leftover .pcm files and merge any unmerged chunks.
    Runs before the monitor loop starts.
    """
    print(f"🔍 Startup cleanup on '{BASE_DIR}'...")
    os.makedirs(BASE_DIR, exist_ok=True)

    folders = [
        os.path.join(BASE_DIR, d)
        for d in os.listdir(BASE_DIR)
        if os.path.isdir(os.path.join(BASE_DIR, d))
    ]
    if not folders:
        print("   No existing folders. Starting fresh.")
        return

    for folder in folders:
        name = os.path.basename(folder)

        # Convert leftover raw PCM
        for pcm in glob.glob(os.path.join(folder, "*.pcm")):
            mp3 = pcm.replace('.pcm', '.mp3')
            print(f"   Converting leftover PCM: {os.path.basename(pcm)}")
            result = subprocess.run(
                f'ffmpeg -y -f s16le -ar 48000 -ac 2 -i "{pcm}" "{mp3}" -loglevel error',
                shell=True
            )
            if result.returncode == 0 and os.path.exists(mp3):
                os.remove(pcm)
            else:
                print(f"   ⚠️  Failed to convert {os.path.basename(pcm)}, skipping.")

        # Merge leftover chunks
        if glob.glob(os.path.join(folder, "*_part*.mp3")):
            print(f"   Merging chunks in '{name}'...")
            merge_chunks(folder, today_str())
            print(f"   ✅ '{name}' cleaned up.")
        else:
            print(f"   ✅ '{name}' already clean.")

    print("✅ Startup cleanup done.")

# ==============================================================================
# CUSTOM SINK  (whitelist + pause filter)
# ==============================================================================

class WhitelistMP3Sink(MP3Sink):
    """Records only whitelisted, non-paused users."""

    @Filters.container
    def write(self, data, user):
        if user not in ALLOWED_USERS:
            return
        if is_user_paused(user):
            return
        if user not in self.audio_data:
            self.audio_data[user] = AudioData(io.BytesIO())
        self.audio_data[user].write(data)

# ==============================================================================
# RECORDING SESSION
#
# Encapsulates everything about one continuous stay in a VC.
# Created when the bot joins, destroyed when the bot leaves.
# The monitor loop is the ONLY place that creates a RecordingSession.
# ==============================================================================

class RecordingSession:

    def __init__(self, vc: discord.VoiceClient):
        self.vc          = vc
        self.guild       = vc.guild
        self.chunk_num   = 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

    # ------------------------------------------------------------------
    # Save all audio in a finished sink to disk, then merge per user.
    # ------------------------------------------------------------------
    def _save_sink(self, sink: WhitelistMP3Sink):
        saved_folders = set()

        for user_id, audio in sink.audio_data.items():
            member    = self.guild.get_member(user_id)
            user_name = member.name if member else str(user_id)
            folder    = user_folder(user_id, user_name)

            try:
                audio.file.seek(0)
                data = audio.file.read()
                if not data:
                    print(f"   ⚠️  Empty audio for {user_name}, skipping.")
                    continue

                chunk_file = os.path.join(folder, f"{user_name}_part{self.chunk_num}.mp3")
                with open(chunk_file, 'wb') as f:
                    f.write(data)
                print(f"   ✅ Saved chunk {self.chunk_num} for {user_name}")
                saved_folders.add(folder)

            except Exception as e:
                print(f"   ❌ Failed to save audio for {user_name}: {e}")

        for folder in saved_folders:
            merge_chunks(folder, self.date_str)

    # ------------------------------------------------------------------
    # Begin recording on the voice client.
    # ------------------------------------------------------------------
    def start(self):
        self.chunk_start = time.time()
        self.date_str    = today_str()

        async def _callback(sink: WhitelistMP3Sink, *args):
            await self._on_stop(sink)

        self.vc.start_recording(
            WhitelistMP3Sink(),
            _callback,
            sync_start=True
        )
        print(f"▶️  Recording started — '{self.vc.channel.name}' chunk {self.chunk_num}")

    # ------------------------------------------------------------------
    # Called by pycord when stop_recording() finishes.
    # Saves the chunk, then transitions state back to IDLE.
    # The monitor loop takes over from IDLE.
    # ------------------------------------------------------------------
    async def _on_stop(self, sink: WhitelistMP3Sink):
        print(f"💾 Processing chunk {self.chunk_num} for '{self.guild.name}'...")
        self._save_sink(sink)
        print(f"✅ Chunk {self.chunk_num} done.")
        set_state(self.guild.id, State.IDLE)

    # ------------------------------------------------------------------
    # Rotate: stop current chunk, save it, start next chunk immediately.
    # Uses a dedicated callback so the next chunk begins inside the save.
    # ------------------------------------------------------------------
    async def rotate(self):
        if not self.vc.recording:
            return

        current_chunk = self.chunk_num
        print(f"🔄 Rotating chunk {current_chunk} in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        async def _rotate_callback(sink: WhitelistMP3Sink, *args):
            print(f"💾 Saving chunk {current_chunk} for '{self.guild.name}'...")
            self._save_sink(sink)
            print(f"✅ Chunk {current_chunk} saved.")

            self.chunk_num  += 1
            self.chunk_start = time.time()
            self.date_str    = today_str()

            if self.vc.is_connected():
                # Start next chunk with the regular stop callback
                async def _next_callback(s: WhitelistMP3Sink, *a):
                    await self._on_stop(s)

                self.vc.start_recording(
                    WhitelistMP3Sink(),
                    _next_callback,
                    sync_start=True
                )
                set_state(self.guild.id, State.RECORDING)
                print(f"▶️  Chunk {self.chunk_num} started in '{self.vc.channel.name}'")
            else:
                # Lost connection during save — go idle, monitor will rejoin
                set_state(self.guild.id, State.IDLE)

        # Swap the active callback by starting fresh with _rotate_callback.
        # We have to stop first, then the callback fires.
        try:
            # Patch: pycord's start_recording stores callback on the voice client.
            # Calling stop_recording() fires whatever callback was registered last.
            # So we re-register _rotate_callback before stopping.
            self.vc._recording_finished = _rotate_callback  # internal pycord attribute
        except Exception:
            pass  # If patching fails we fall back gracefully

        try:
            self.vc.stop_recording()
        except Exception as e:
            print(f"❌ rotate() failed: {e}")
            set_state(self.guild.id, State.RECORDING)

    # ------------------------------------------------------------------
    # Stop: save chunk, disconnect, go IDLE.
    # Called by !stop command and on_voice_state_update (kick).
    # ------------------------------------------------------------------
    async def stop(self, reason: str = "manual"):
        print(f"⏹️  Stopping session ({reason}) in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        if self.vc.recording:
            try:
                self.vc.stop_recording()
                # _on_stop callback will fire and set state to IDLE
            except Exception as e:
                print(f"❌ stop_recording() error: {e}")
                set_state(self.guild.id, State.IDLE)
        else:
            set_state(self.guild.id, State.IDLE)

        await asyncio.sleep(1)

        if self.vc.is_connected():
            try:
                await self.vc.disconnect(force=True)
            except Exception as e:
                print(f"⚠️  disconnect() error: {e}")

        guild_cooldown[self.guild.id] = time.time() + LEAVE_COOLDOWN
        print(f"✅ Session ended in '{self.guild.name}'. Cooldown {LEAVE_COOLDOWN}s.")


# Active sessions: guild_id -> RecordingSession
active_sessions: dict = {}

# ==============================================================================
# CHANNEL HELPERS
# ==============================================================================

def channel_has_target(channel: discord.VoiceChannel) -> bool:
    """True if at least one whitelisted, undeafened, non-paused user is present."""
    if not ALLOWED_USERS:
        return False
    if ALLOWED_CHANNELS and channel.id not in ALLOWED_CHANNELS:
        return False
    for member in channel.members:
        if member.bot:
            continue
        vs = member.voice
        if (
            member.id in ALLOWED_USERS
            and vs
            and not vs.self_deaf
            and not vs.deaf
            and not is_user_paused(member.id)
        ):
            return True
    return False

def find_target_channel(guild: discord.Guild):
    for ch in guild.voice_channels:
        if channel_has_target(ch):
            return ch
    return None

# ==============================================================================
# MONITOR LOOP
#
# The single source of truth for joining and leaving voice channels.
# Every CHECK_INTERVAL seconds it evaluates each guild's state and acts.
#
#   IDLE    → look for a target channel → join → RECORDING
#   SAVING  → do nothing, wait for callback
#   RECORDING → check channel still has targets → rotate if chunk time reached
# ==============================================================================

@tasks.loop(seconds=CHECK_INTERVAL)
async def monitor():
    for guild in bot.guilds:
        gid   = guild.id
        state = get_state(gid)
        vc    = guild.voice_client
        now   = time.time()

        # Expire cooldown
        if gid in guild_cooldown and now >= guild_cooldown[gid]:
            del guild_cooldown[gid]

        # Saving: wait for pycord callback to finish — do nothing
        if state == State.SAVING:
            continue

        # --------------------------------------------------------------
        # RECORDING
        # --------------------------------------------------------------
        if state == State.RECORDING:
            session = active_sessions.get(gid)

            # Pycord dropped the connection silently
            if not vc or not vc.is_connected():
                print(f"⚠️  Silent disconnect detected in '{guild.name}' — going IDLE.")
                active_sessions.pop(gid, None)
                set_state(gid, State.IDLE)
                guild_cooldown[gid] = now + LEAVE_COOLDOWN
                continue

            # No more targets → leave
            if not channel_has_target(vc.channel):
                print(f"No targets left in '{vc.channel.name}' — leaving.")
                active_sessions.pop(gid, None)
                if session:
                    await session.stop(reason="no targets")
                continue

            # Chunk rotation time?
            if session and (now - session.chunk_start) >= CHUNK_SECONDS:
                await session.rotate()

        # --------------------------------------------------------------
        # IDLE
        # --------------------------------------------------------------
        elif state == State.IDLE:
            if gid in guild_cooldown:
                continue  # Still cooling down

            target = find_target_channel(guild)
            if not target:
                continue  # Nothing interesting

            # Clean up any stale voice client
            if vc and vc.is_connected():
                try:
                    await vc.disconnect(force=True)
                except Exception:
                    pass
                await asyncio.sleep(1)

            print(f"🎯 Joining '{target.name}' in '{guild.name}'...")
            try:
                new_vc  = await target.connect()
                session = RecordingSession(new_vc)
                session.start()
                active_sessions[gid] = session
                set_state(gid, State.RECORDING)
            except Exception as e:
                print(f"❌ Failed to join '{target.name}': {e}")
                guild_cooldown[gid] = now + LEAVE_COOLDOWN

# ==============================================================================
# DISCORD EVENTS
# ==============================================================================

@bot.event
async def on_ready():
    print(f"\n🤖 Logged in as {bot.user}")
    print(f"   Monitoring {len(bot.guilds)} server(s) every {CHECK_INTERVAL}s\n")
    startup_cleanup()
    monitor.start()

@bot.event
async def on_voice_state_update(member, before, after):
    """
    Handles the bot being forcibly kicked.
    Saves current chunk then goes IDLE.
    The monitor loop handles rejoining naturally.
    """
    if member.id != bot.user.id:
        return
    print(f"[DEBUG] before.channel={before.channel} after.channel={after.channel} state={get_state(member.guild.id).name}")
    if not (before.channel and not after.channel):
        return  # Not a disconnect

    guild = before.channel.guild
    gid   = guild.id

    print(f"⚠️  Bot kicked from '{before.channel.name}' in '{guild.name}'")

    session = active_sessions.pop(gid, None)

    if session and get_state(gid) == State.RECORDING:
        await session.stop(reason="kicked")
    else:
        set_state(gid, State.IDLE)
        guild_cooldown[gid] = time.time() + LEAVE_COOLDOWN

# ==============================================================================
# COMMANDS
# ==============================================================================

def _whitelisted(ctx) -> bool:
    return ctx.author.id in ALLOWED_USERS

# --- !stop ---

@bot.command()
async def stop(ctx):
    """Save current chunk and go idle. Bot rejoins when targets are available."""
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return

    gid   = ctx.guild.id
    state = get_state(gid)

    if state != State.RECORDING:
        await ctx.send("⚠️ I'm not currently recording.")
        return

    session = active_sessions.pop(gid, None)
    await ctx.send("⏹️ Saving chunk... I'll rejoin automatically when you're back.")

    if session:
        await session.stop(reason="!stop command")
    else:
        set_state(gid, State.IDLE)

# --- !pause ---

@bot.command()
async def pause(ctx, minutes: int = None):
    """Pause your own recording for N minutes. Usage: !pause 30"""
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    if not minutes or minutes <= 0:
        await ctx.send("⚠️ Usage: `!pause <minutes>` — e.g. `!pause 30`")
        return

    user_id   = ctx.author.id
    user_name = "".join(c for c in ctx.author.name if c.isalnum())

    await _flush_user_audio(ctx.guild, user_id, user_name)
    pause_user(user_id, minutes)

    resume_at = datetime.fromtimestamp(user_paused_until[user_id]).strftime("%H:%M:%S")
    await ctx.send(
        f"⏸️ **{ctx.author.display_name}** — paused for **{minutes}min**. "
        f"Resumes at **{resume_at}**. Use `!unpause` to resume early."
    )
    print(f"⏸️  {ctx.author.name} paused for {minutes}min.")

# --- !unpause / !continue ---

@bot.command()
async def unpause(ctx):
    """Resume recording before the pause expires."""
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    if not is_user_paused(ctx.author.id):
        await ctx.send(f"▶️ **{ctx.author.display_name}** — you are not paused.")
        return
    unpause_user(ctx.author.id)
    await ctx.send(f"▶️ **{ctx.author.display_name}** — recording resumed.")
    print(f"▶️  {ctx.author.name} unpaused.")

@bot.command(name="continue")
async def cmd_continue(ctx):
    """Alias for !unpause."""
    await unpause(ctx)

# --- !whitelist / !allow / !unallow ---

@bot.command()
async def whitelist(ctx):
    """Show the recording whitelist."""
    if not ALLOWED_USERS:
        await ctx.send("📋 Whitelist is empty. Use `!allow <user_id>` to add someone.")
    else:
        lines = "\n".join(f"• `{uid}`" for uid in ALLOWED_USERS)
        await ctx.send(f"📋 **Recording {len(ALLOWED_USERS)} user(s):**\n{lines}")

@bot.command()
async def allow(ctx, user_id: int):
    """Add a user to the recording whitelist. Usage: !allow <user_id>"""
    ALLOWED_USERS.add(user_id)
    _save_whitelist()
    await ctx.send(f"✅ `{user_id}` added. ({len(ALLOWED_USERS)} tracked)")

@bot.command()
async def unallow(ctx, user_id: int):
    """Remove a user from the recording whitelist. Usage: !unallow <user_id>"""
    ALLOWED_USERS.discard(user_id)
    _save_whitelist()
    msg = f"({len(ALLOWED_USERS)} remaining)" if ALLOWED_USERS else "Whitelist is now **empty**."
    await ctx.send(f"🗑️ `{user_id}` removed. {msg}")

def _save_whitelist():
    env_path = '.env'
    new_line = f"ALLOWED_USERS={','.join(str(uid) for uid in ALLOWED_USERS)}\n"
    lines = open(env_path).readlines() if os.path.exists(env_path) else []
    for i, line in enumerate(lines):
        if line.startswith('ALLOWED_USERS='):
            lines[i] = new_line
            break
    else:
        lines.append(new_line)
    with open(env_path, 'w') as f:
        f.writelines(lines)

# --- !restart ---

@bot.command()
async def restart(ctx):
    """Save recordings and restart the bot process. (Whitelisted only)"""
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    await ctx.send("🔄 Saving and restarting...")
    await _shutdown_all()
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ==============================================================================
# FLUSH HELPER  (used by !pause to preserve buffered audio before pausing)
# ==============================================================================

async def _flush_user_audio(guild: discord.Guild, user_id: int, user_name: str):
    session = active_sessions.get(guild.id)
    if not session:
        return
    vc = session.vc
    if not vc or not vc.is_connected() or not vc.recording:
        return
    sink = vc.sink
    if not sink or user_id not in sink.audio_data:
        return

    audio = sink.audio_data[user_id]
    try:
        audio.file.seek(0)
        data = audio.file.read()
        if not data:
            return

        folder     = user_folder(user_id, user_name)
        chunk_file = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.mp3")
        with open(chunk_file, 'wb') as f:
            f.write(data)
        print(f"   💾 Pre-pause flush saved for {user_name}")

        merge_chunks(folder, today_str())

        # Reset buffer and bump chunk number so next write doesn't collide
        sink.audio_data[user_id] = AudioData(io.BytesIO())
        session.chunk_num += 1

    except Exception as e:
        print(f"   ❌ Pre-pause flush failed for {user_name}: {e}")

# ==============================================================================
# GRACEFUL SHUTDOWN
# ==============================================================================

async def _shutdown_all():
    print("🛑 Saving all recordings before shutdown...")
    for gid, session in list(active_sessions.items()):
        vc = session.vc
        if vc and vc.is_connected():
            print(f"   Saving '{session.guild.name}'...")
            if vc.recording:
                try:
                    vc.stop_recording()
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"   ⚠️  stop_recording error: {e}")
            try:
                await vc.disconnect(force=True)
            except Exception:
                pass
    active_sessions.clear()
    print("✅ Shutdown complete.")

# ==============================================================================
# ENTRY POINT
# ==============================================================================

if TOKEN:
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        asyncio.run(_shutdown_all())
else:
    print("❌ DISCORD_TOKEN not set.")
