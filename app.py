"""
Discord Voice Recording Bot
Compatible with: py-cord DA-344/feat/voice-rewrite-and-fixes (DAVE support branch)
Install:
    pip install "py-cord[voice] @ git+https://github.com/DA-344/pycord.git@feat/voice-rewrite-and-fixes"
    pip install davey
"""

import discord
from discord.ext import commands, tasks
from discord.sinks import Sink, AudioData, Filters
import os
import sys
import time
import asyncio
import subprocess
import glob
import io
from datetime import datetime
from enum import Enum, auto

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
CHECK_INTERVAL  = 3
LEAVE_COOLDOWN  = 10

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

guild_state:       dict = {}
guild_cooldown:    dict = {}
user_paused_until: dict = {}

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
    """Concatenates all *_part*.mp3 files into YYYY-MM-DD.mp3, then deletes parts."""
    chunks = sorted(glob.glob(os.path.join(folder, "*_part*.mp3")))
    if not chunks:
        return

    daily_file = os.path.join(folder, f"{date_str}.mp3")
    list_file  = os.path.join(folder, "_merge_list.txt")
    temp_file  = os.path.join(folder, f"_temp_{date_str}.mp3")

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
    """On startup: merge any unmerged chunks from previous sessions."""
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

class WhitelistMP3Sink(Sink):
    """
    Records only whitelisted, non-paused users.
    Extends the base Sink class from the new voice rewrite branch.
    Audio data arrives already decrypted (including DAVE) by the time
    write() is called — no changes needed here for DAVE support.
    """

    @Filters.container
    def write(self, data, user):
        if user not in ALLOWED_USERS:
            return
        if is_user_paused(user):
            return
        if user not in self.audio_data:
            self.audio_data[user] = AudioData(io.BytesIO())
        self.audio_data[user].write(data)

    def format_audio(self, audio: AudioData):
        """
        Called by the base Sink during cleanup() for each AudioData.
        Converts raw PCM stored in the BytesIO to MP3 via ffmpeg.
        """
        audio.file.seek(0)
        raw = audio.file.read()
        if not raw:
            return

        process = subprocess.Popen(
            ['ffmpeg', '-y',
             '-f', 's16le', '-ar', '48000', '-ac', '2',
             '-i', 'pipe:0',
             '-f', 'mp3', 'pipe:1'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )
        mp3_data, _ = process.communicate(input=raw)

        audio.file = io.BytesIO(mp3_data)
        audio.file.seek(0)

# ==============================================================================
# RECORDING SESSION
# ==============================================================================

class RecordingSession:
    """
    Encapsulates one continuous stay in a VC.
    Created when the bot joins, destroyed when it leaves.
    The monitor loop is the ONLY place that creates a RecordingSession.

    New API changes (DA-344 branch):
    - start_recording(sink, callback) — callback is now `def cb(error)`, sink NOT passed
    - stop_recording() — raises RecordingException if not recording
    - vc.is_recording() — replaces vc.recording property
    - Sink is stored on self.sink so we can access it in the callback
    """

    def __init__(self, vc: discord.VoiceClient):
        self.vc          = vc
        self.guild       = vc.guild
        self.sink        = WhitelistMP3Sink()   # stored here — callback no longer receives it
        self.chunk_num   = 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

    # ------------------------------------------------------------------
    # Save all audio from a finished sink to disk, then merge per user.
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
        self.sink        = WhitelistMP3Sink()
        self.chunk_start = time.time()
        self.date_str    = today_str()

        # Callback now only receives `error` — sink accessed via self.sink
        def _callback(error: Exception | None):
            if error:
                print(f"   ❌ Recording error: {error}")
            asyncio.run_coroutine_threadsafe(
                self._on_stop(), bot.loop
            )

        self.vc.start_recording(self.sink, _callback)
        print(f"▶️  Recording started — '{self.vc.channel.name}' chunk {self.chunk_num}")

    # ------------------------------------------------------------------
    # Called after stop_recording() finishes — saves chunk, sets IDLE.
    # ------------------------------------------------------------------
    async def _on_stop(self):
        print(f"💾 Processing chunk {self.chunk_num} for '{self.guild.name}'...")
        self._save_sink(self.sink)
        print(f"✅ Chunk {self.chunk_num} done.")
        set_state(self.guild.id, State.IDLE)

    # ------------------------------------------------------------------
    # Rotate: stop current chunk, save, start next chunk immediately.
    # ------------------------------------------------------------------
    async def rotate(self):
        if not self.vc.is_recording():
            return

        current_chunk    = self.chunk_num
        current_sink     = self.sink
        print(f"🔄 Rotating chunk {current_chunk} in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        def _rotate_callback(error: Exception | None):
            if error:
                print(f"   ❌ Rotate recording error: {error}")
            asyncio.run_coroutine_threadsafe(
                self._on_rotate_done(current_sink, current_chunk), bot.loop
            )

        # Prepare next sink before stopping so it's ready immediately
        self.chunk_num  += 1
        self.chunk_start = time.time()
        self.date_str    = today_str()
        self.sink        = WhitelistMP3Sink()

        try:
            self.vc.stop_recording()
        except Exception as e:
            print(f"❌ rotate() stop_recording failed: {e}")
            # Rollback
            self.chunk_num  -= 1
            self.chunk_start = time.time()
            self.sink        = current_sink
            set_state(self.guild.id, State.RECORDING)
            return

        # Immediately start next chunk
        if self.vc.is_connected():
            self.vc.start_recording(self.sink, _rotate_callback)
            set_state(self.guild.id, State.RECORDING)
            print(f"▶️  Chunk {self.chunk_num} started in '{self.vc.channel.name}'")

    async def _on_rotate_done(self, old_sink: WhitelistMP3Sink, chunk_num: int):
        print(f"💾 Saving chunk {chunk_num} for '{self.guild.name}'...")
        # Temporarily set chunk_num back to save with correct number
        real_chunk_num  = self.chunk_num
        self.chunk_num  = chunk_num
        self._save_sink(old_sink)
        self.chunk_num  = real_chunk_num
        print(f"✅ Chunk {chunk_num} saved.")

    # ------------------------------------------------------------------
    # Stop: save chunk, disconnect, go IDLE.
    # ------------------------------------------------------------------
    async def stop(self, reason: str = "manual"):
        print(f"⏹️  Stopping session ({reason}) in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        if self.vc.is_recording():
            try:
                self.vc.stop_recording()
                # _on_stop() callback will fire and set state to IDLE
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

        # Saving: wait for callback to finish
        if state == State.SAVING:
            continue

        # --------------------------------------------------------------
        # RECORDING
        # --------------------------------------------------------------
        if state == State.RECORDING:
            session = active_sessions.get(gid)

            # Silent disconnect
            if not vc or not vc.is_connected():
                print(f"⚠️  Silent disconnect in '{guild.name}' — going IDLE.")
                active_sessions.pop(gid, None)
                set_state(gid, State.IDLE)
                guild_cooldown[gid] = now + LEAVE_COOLDOWN
                continue

            # No more targets
            if not channel_has_target(vc.channel):
                print(f"No targets left in '{vc.channel.name}' — leaving.")
                active_sessions.pop(gid, None)
                if session:
                    await session.stop(reason="no targets")
                continue

            # Chunk rotation
            if session and (now - session.chunk_start) >= CHUNK_SECONDS:
                await session.rotate()

        # --------------------------------------------------------------
        # IDLE
        # --------------------------------------------------------------
        elif state == State.IDLE:
            if gid in guild_cooldown:
                continue

            target = find_target_channel(guild)
            if not target:
                continue

            # Clean up stale voice client
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
    Only handles the bot being forcibly kicked.
    Ignored if state is IDLE (normal join handshake events).
    """
    if member.id != bot.user.id:
        return
    if not (before.channel and not after.channel):
        return

    guild = before.channel.guild
    gid   = guild.id
    state = get_state(gid)

    # Ignore disconnect events during IDLE — these are join handshake artifacts
    if state == State.IDLE:
        return

    print(f"⚠️  Bot disconnected from '{before.channel.name}' in '{guild.name}'")

    session = active_sessions.pop(gid, None)

    if session and state == State.RECORDING:
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
    """Save recordings and restart the bot process."""
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    await ctx.send("🔄 Saving and restarting...")
    await _shutdown_all()
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ==============================================================================
# FLUSH HELPER  (used by !pause to preserve buffered audio)
# ==============================================================================

async def _flush_user_audio(guild: discord.Guild, user_id: int, user_name: str):
    session = active_sessions.get(guild.id)
    if not session:
        return
    vc = session.vc
    if not vc or not vc.is_connected() or not vc.is_recording():
        return
    sink = session.sink
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
            if vc.is_recording():
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
