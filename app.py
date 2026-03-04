import discord
from discord.ext import commands, tasks
from discord import opus as discord_opus
import os
import sys
import time
import asyncio
import subprocess
import glob
import io
import struct
import threading
import logging
from datetime import datetime
from enum import Enum, auto

import nacl.secret
import nacl.utils

_log = logging.getLogger(__name__)

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
    SAVING    = auto()   # Saving in progress; waiting to finish.

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
    print(f"�� Startup cleanup on '{BASE_DIR}'...")
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
# VOICE RECEIVE INFRASTRUCTURE  (replaces pycord's Sink system)
# ==============================================================================

class AudioBuffer:
    """Holds raw PCM audio data for one user, thread-safe."""

    def __init__(self):
        self._buf = io.BytesIO()
        self._lock = threading.Lock()

    def write(self, pcm_data: bytes):
        with self._lock:
            self._buf.write(pcm_data)

    def read_all(self) -> bytes:
        with self._lock:
            self._buf.seek(0)
            data = self._buf.read()
            return data

    def reset(self):
        with self._lock:
            self._buf = io.BytesIO()

    @property
    def empty(self) -> bool:
        with self._lock:
            pos = self._buf.tell()
            return pos == 0


def pcm_to_mp3(pcm_data: bytes) -> bytes:
    """Convert raw 16-bit 48kHz stereo PCM to MP3 via ffmpeg."""
    if not pcm_data:
        return b''
    process = subprocess.Popen(
        ['ffmpeg', '-y',
         '-f', 's16le', '-ar', '48000', '-ac', '2',
         '-i', 'pipe:0',
         '-f', 'mp3', 'pipe:1'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    mp3_data, _ = process.communicate(input=pcm_data)
    return mp3_data


class VoiceReceiver:
    """
    Receives voice data from a discord.py VoiceClient using SocketReader callbacks.

    The SocketReader delivers raw encrypted UDP packets. We:
      1. Parse the 12-byte RTP header to extract SSRC
      2. Strip the RTP header and encryption nonce/tag
      3. Decrypt the payload using the connection's secret key + mode
      4. DAVE-decrypt if DAVE session is active
      5. Decode Opus -> PCM
      6. Write PCM into per-user AudioBuffer (filtered by whitelist + pause)

    SSRC->user_id mapping comes from the voice websocket's SPEAKING event (opcode 5).
    We hook into this by monkey-patching the voice websocket's received_message handler.
    """

    SILENCE_FRAME = b'\xf8\xff\xfe'  # Opus silence

    def __init__(self, vc: discord.VoiceClient):
        self.vc = vc
        self._connection = vc._connection  # VoiceConnectionState
        self.audio_data: dict[int, AudioBuffer] = {}  # user_id -> AudioBuffer
        self._ssrc_to_user: dict[int, int] = {}       # ssrc -> user_id
        self._decoders: dict = {}                      # ssrc -> OpusDecoder
        self._lock = threading.Lock()
        self._recording = False

    def start(self):
        """Start receiving voice packets."""
        self._recording = True
        # Register our callback with the SocketReader
        self._connection.add_socket_listener(self._on_packet)
        # Hook into voice websocket to capture SPEAKING events for SSRC mapping
        self._hook_voice_ws()
        print(f"   🎙️ VoiceReceiver started")

    def stop(self):
        """Stop receiving voice packets."""
        self._recording = False
        self._connection.remove_socket_listener(self._on_packet)
        self._unhook_voice_ws()
        print(f"   🎙️ VoiceReceiver stopped")

    @property
    def is_recording(self) -> bool:
        return self._recording

    def _hook_voice_ws(self):
        """
        Monkey-patch the voice websocket's received_message to intercept
        SPEAKING (op 5), CLIENT_CONNECT (op 12), and CLIENT_DISCONNECT (op 13)
        events, which provide the SSRC -> user_id mapping.
        """
        try:
            ws = self._connection.ws
        except Exception:
            _log.debug('Cannot hook voice WS - not available yet')
            return

        if hasattr(ws, '__voice_receiver_hooked__'):
            return

        original = ws.received_message
        receiver = self

        async def patched_received_message(msg: dict):
            # Intercept before passing to original handler
            try:
                op = msg.get('op')
                data = msg.get('d', {})
                if op == 5:  # SPEAKING
                    ssrc = data.get('ssrc')
                    user_id = data.get('user_id')
                    if ssrc is not None and user_id is not None:
                        user_id = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = user_id
                        _log.debug('SPEAKING: ssrc=%s -> user_id=%s', ssrc, user_id)
                elif op == 12:  # CLIENT_CONNECT
                    ssrc = data.get('audio_ssrc')
                    user_id = data.get('user_id')
                    if ssrc is not None and user_id is not None:
                        user_id = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = user_id
                elif op == 13:  # CLIENT_DISCONNECT
                    user_id = data.get('user_id')
                    if user_id is not None:
                        user_id = int(user_id)
                        with receiver._lock:
                            to_remove = [s for s, u in receiver._ssrc_to_user.items() if u == user_id]
                            for s in to_remove:
                                del receiver._ssrc_to_user[s]
                                receiver._decoders.pop(s, None)
            except Exception:
                _log.debug('Error in patched_received_message intercept', exc_info=True)

            return await original(msg)

        ws.__voice_receiver_hooked__ = True
        ws.__original_received_message__ = original
        ws.received_message = patched_received_message

    def _unhook_voice_ws(self):
        """Restore the original received_message handler."""
        try:
            ws = self._connection.ws
            if hasattr(ws, '__original_received_message__'):
                ws.received_message = ws.__original_received_message__
                del ws.__original_received_message__
            if hasattr(ws, '__voice_receiver_hooked__'):
                del ws.__voice_receiver_hooked__
        except Exception:
            pass

    def _get_decoder(self, ssrc: int):
        """Get or create an Opus decoder for an SSRC."""
        if ssrc not in self._decoders:
            self._decoders[ssrc] = discord_opus.Decoder()
        return self._decoders[ssrc]

    def _decrypt_packet(self, data: bytes):
        """
        Parse RTP header, decrypt the voice payload.
        Returns (ssrc, opus_data) or None on failure.
        """
        if len(data) < 12:
            return None

        # Parse RTP header (12 bytes)
        # Byte 0: V=2, P, X, CC
        # Byte 1: M, PT (0x78 = 120 for Discord voice)
        # Bytes 2-3: sequence number (big-endian)
        # Bytes 4-7: timestamp (big-endian)
        # Bytes 8-11: SSRC (big-endian)
        header = data[:12]
        ssrc = struct.unpack_from('>I', header, 8)[0]

        # Skip our own SSRC
        try:
            if ssrc == self._connection.ssrc:
                return None
        except Exception:
            pass

        # Handle RTP header extensions and CSRC
        cc = header[0] & 0x0F  # CSRC count
        has_extension = bool(header[0] & 0x10)
        header_size = 12 + cc * 4

        if has_extension and len(data) > header_size + 4:
            ext_length = struct.unpack_from('>H', data, header_size + 2)[0]
            header_size += 4 + ext_length * 4

        encrypted_data = data[header_size:]
        if not encrypted_data:
            return None

        # Decrypt based on encryption mode
        try:
            mode = self._connection.mode
            secret_key = bytes(self._connection.secret_key)

            if mode == 'aead_xchacha20_poly1305_rtpsize':
                opus_data = self._decrypt_aead_xchacha20(data[:12], data[12:], secret_key)
            elif mode == 'xsalsa20_poly1305_lite':
                opus_data = self._decrypt_xsalsa20_lite(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305_suffix':
                opus_data = self._decrypt_xsalsa20_suffix(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305':
                opus_data = self._decrypt_xsalsa20(data[:12], encrypted_data, secret_key)
            else:
                return None
        except Exception:
            return None

        if not opus_data:
            return None

        # DAVE decryption if session is active
        try:
            dave_session = self._connection.dave_session
            if dave_session and self._connection.dave_protocol_version > 0:
                opus_data = dave_session.decrypt_opus(opus_data, ssrc)
        except Exception:
            return None

        return (ssrc, opus_data)

    def _decrypt_aead_xchacha20(self, header: bytes, after_header: bytes, secret_key: bytes) -> bytes:
        """Decrypt aead_xchacha20_poly1305_rtpsize mode."""
        nonce_bytes = after_header[-4:]
        encrypted = after_header[:-4]
        nonce = bytearray(24)
        nonce[:4] = nonce_bytes
        box = nacl.secret.Aead(secret_key)
        return box.decrypt(bytes(encrypted), bytes(header[:12]), bytes(nonce))

    def _decrypt_xsalsa20(self, header: bytes, encrypted_data: bytes, secret_key: bytes) -> bytes:
        """Decrypt xsalsa20_poly1305 mode."""
        box = nacl.secret.SecretBox(secret_key)
        nonce = bytearray(24)
        nonce[:12] = header[:12]
        return box.decrypt(bytes(encrypted_data), bytes(nonce))

    def _decrypt_xsalsa20_suffix(self, encrypted_data: bytes, secret_key: bytes) -> bytes:
        """Decrypt xsalsa20_poly1305_suffix mode."""
        box = nacl.secret.SecretBox(secret_key)
        nonce = encrypted_data[-24:]
        encrypted = encrypted_data[:-24]
        return box.decrypt(bytes(encrypted), bytes(nonce))

    def _decrypt_xsalsa20_lite(self, encrypted_data: bytes, secret_key: bytes) -> bytes:
        """Decrypt xsalsa20_poly1305_lite mode."""
        box = nacl.secret.SecretBox(secret_key)
        nonce = bytearray(24)
        nonce[:4] = encrypted_data[-4:]
        encrypted = encrypted_data[:-4]
        return box.decrypt(bytes(encrypted), bytes(nonce))

    def _on_packet(self, data: bytes):
        """
        SocketReader callback - runs in the reader thread.
        Receives raw encrypted UDP packets from the voice socket.
        """
        if not self._recording:
            return

        result = self._decrypt_packet(data)
        if result is None:
            return

        ssrc, opus_data = result

        # Map SSRC to user_id
        with self._lock:
            user_id = self._ssrc_to_user.get(ssrc)

        if user_id is None:
            return

        # Filter: only whitelisted, non-paused users
        if user_id not in ALLOWED_USERS:
            return
        if is_user_paused(user_id):
            return

        # Skip silence frames
        if opus_data == self.SILENCE_FRAME:
            return

        # Decode Opus to PCM
        try:
            decoder = self._get_decoder(ssrc)
            pcm = decoder.decode(opus_data, fec=False)
        except Exception:
            return

        # Store in per-user buffer
        if user_id not in self.audio_data:
            self.audio_data[user_id] = AudioBuffer()
        self.audio_data[user_id].write(pcm)


# ==============================================================================
# RECORDING SESSION
# ==============================================================================

class RecordingSession:
    """
    Encapsulates one continuous stay in a VC.
    Created when the bot joins, destroyed when it leaves.
    The monitor loop is the ONLY place that creates a RecordingSession.

    Uses VoiceReceiver (SocketReader-based) instead of pycord's Sink system.
    """

    def __init__(self, vc: discord.VoiceClient):
        self.vc          = vc
        self.guild       = vc.guild
        self.receiver    = VoiceReceiver(vc)
        self.chunk_num   = 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

    # ------------------------------------------------------------------
    # Save all audio from a finished receiver to disk, then merge per user.
    # ------------------------------------------------------------------
    def _save_receiver(self, audio_data: dict, chunk_num: int):
        saved_folders = set()

        for user_id, buffer in audio_data.items():
            member    = self.guild.get_member(user_id)
            user_name = member.name if member else str(user_id)
            folder    = user_folder(user_id, user_name)

            try:
                pcm_data = buffer.read_all()
                if not pcm_data:
                    print(f"   ⚠️  Empty audio for {user_name}, skipping.")
                    continue

                # Convert PCM to MP3
                mp3_data = pcm_to_mp3(pcm_data)
                if not mp3_data:
                    print(f"   ⚠️  MP3 conversion failed for {user_name}, saving raw PCM.")
                    chunk_file = os.path.join(folder, f"{user_name}_part{chunk_num}.pcm")
                    with open(chunk_file, 'wb') as f:
                        f.write(pcm_data)
                    continue

                chunk_file = os.path.join(folder, f"{user_name}_part{chunk_num}.mp3")
                with open(chunk_file, 'wb') as f:
                    f.write(mp3_data)
                print(f"   ✅ Saved chunk {chunk_num} for {user_name}")
                saved_folders.add(folder)

            except Exception as e:
                print(f"   ❌ Failed to save audio for {user_name}: {e}")

        for folder in saved_folders:
            merge_chunks(folder, self.date_str)

    # ------------------------------------------------------------------
    # Begin recording on the voice client.
    # ------------------------------------------------------------------
    def start(self):
        self.receiver    = VoiceReceiver(self.vc)
        self.chunk_start = time.time()
        self.date_str    = today_str()
        self.receiver.start()
        print(f"▶️  Recording started — '{self.vc.channel.name}' chunk {self.chunk_num}")

    # ------------------------------------------------------------------
    # Rotate: stop current chunk, save, start next chunk immediately.
    # ------------------------------------------------------------------
    async def rotate(self):
        if not self.receiver.is_recording:
            return

        current_chunk    = self.chunk_num
        current_audio    = self.receiver.audio_data
        current_date     = self.date_str
        print(f"🔄 Rotating chunk {current_chunk} in '{self.guild.name}'...")

        # Stop the current receiver
        self.receiver.stop()

        # Start a new chunk immediately
        self.chunk_num  += 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

        if self.vc.is_connected():
            self.receiver = VoiceReceiver(self.vc)
            self.receiver.start()
            print(f"▶️  Chunk {self.chunk_num} started in '{self.vc.channel.name}'")
        else:
            set_state(self.guild.id, State.IDLE)
            return

        # Save old chunk in background (CPU-bound ffmpeg work)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, self._save_receiver, current_audio, current_chunk
        )
        print(f"✅ Chunk {current_chunk} saved.")

    # ------------------------------------------------------------------
    # Stop: save chunk, disconnect, go IDLE.
    # ------------------------------------------------------------------
    async def stop(self, reason: str = "manual"):
        print(f"⏹️  Stopping session ({reason}) in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        if self.receiver.is_recording:
            self.receiver.stop()

        # Save remaining audio
        audio_data = self.receiver.audio_data
        chunk_num  = self.chunk_num
        if audio_data:
            print(f"💾 Processing chunk {chunk_num} for '{self.guild.name}'...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, self._save_receiver, audio_data, chunk_num
            )
            print(f"✅ Chunk {chunk_num} done.")

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

        # Saving: wait for save to finish
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
                if session and session.receiver.is_recording:
                    session.receiver.stop()
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
    if not vc or not vc.is_connected() or not session.receiver.is_recording:
        return
    receiver = session.receiver
    if user_id not in receiver.audio_data:
        return

    buffer = receiver.audio_data[user_id]
    try:
        pcm_data = buffer.read_all()
        if not pcm_data:
            return

        folder   = user_folder(user_id, user_name)
        mp3_data = pcm_to_mp3(pcm_data)

        if mp3_data:
            chunk_file = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.mp3")
            with open(chunk_file, 'wb') as f:
                f.write(mp3_data)
        else:
            chunk_file = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.pcm")
            with open(chunk_file, 'wb') as f:
                f.write(pcm_data)

        print(f"   💾 Pre-pause flush saved for {user_name}")

        merge_chunks(folder, today_str())

        # Reset the user's buffer
        buffer.reset()
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
            if session.receiver.is_recording:
                session.receiver.stop()
                audio_data = session.receiver.audio_data
                if audio_data:
                    session._save_receiver(audio_data, session.chunk_num)
                await asyncio.sleep(1)
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
