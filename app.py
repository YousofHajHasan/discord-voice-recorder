"""
Discord Voice Recorder
======================
Records whitelisted users from Discord voice channels to per-user MP3 files.

Architecture:
  - discord.py master branch (from git) handles the full DAVE/MLS handshake
    internally in its voice_state machinery. We do NOT intercept it.
  - We register a raw socket listener to receive encrypted UDP voice packets.
  - Transport decryption (nacl) happens in VoiceReceiver._decrypt_transport().
  - DAVE frame decryption uses vc._connection.dave_session which discord.py
    populates after the MLS handshake completes.
  - SSRC→user_id mapping comes from the voice WS SPEAKING/CLIENT_CONNECT ops,
    intercepted via a lightweight monkey-patch on received_message (JSON only).
  - Decoded PCM is buffered per user and flushed to MP3 on chunk rotate/stop.
"""

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

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

print("Configuration loaded:")
print(f"   Chunk time     : {CHUNK_SECONDS}s")
print(f"   Check interval : {CHECK_INTERVAL}s")
print(f"   Leave cooldown : {LEAVE_COOLDOWN}s")
print(f"   Channel filter : {ALLOWED_CHANNELS or 'ALL'}")
print(f"   User whitelist : {ALLOWED_USERS or 'EMPTY - no one will be recorded'}")

# ==============================================================================
# BOT STATE
# ==============================================================================

class State(Enum):
    IDLE      = auto()
    RECORDING = auto()
    SAVING    = auto()

guild_state:       dict = {}
guild_cooldown:    dict = {}
user_paused_until: dict = {}

def get_state(guild_id: int) -> State:
    return guild_state.get(guild_id, State.IDLE)

def set_state(guild_id: int, state: State):
    guild_state[guild_id] = state
    print(f"   [state] guild {guild_id} -> {state.name}")

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
    print(f"Startup cleanup on '{BASE_DIR}'...")
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
            print(f"   '{name}' cleaned up.")
        else:
            print(f"   '{name}' already clean.")
    print("Startup cleanup done.")

# ==============================================================================
# AUDIO BUFFER
# ==============================================================================

class AudioBuffer:
    def __init__(self):
        self._buf  = io.BytesIO()
        self._lock = threading.Lock()

    def write(self, pcm_data: bytes):
        with self._lock:
            self._buf.write(pcm_data)

    def read_all(self) -> bytes:
        with self._lock:
            self._buf.seek(0)
            return self._buf.read()

    def reset(self):
        with self._lock:
            self._buf = io.BytesIO()

    @property
    def empty(self) -> bool:
        with self._lock:
            return self._buf.tell() == 0


def pcm_to_mp3(pcm_data: bytes) -> bytes:
    if not pcm_data:
        return b''
    process = subprocess.Popen(
        ['ffmpeg', '-y',
         '-f', 's16le', '-ar', '48000', '-ac', '2', '-i', 'pipe:0',
         '-f', 'mp3', 'pipe:1'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    mp3_data, _ = process.communicate(input=pcm_data)
    return mp3_data

# ==============================================================================
# VOICE RECEIVER
# ==============================================================================

class VoiceReceiver:
    """
    Receives raw UDP voice packets via discord.py's SocketReader.

    Decryption pipeline per packet:
      1. Parse 12-byte fixed RTP header + CSRC/ext to find encrypted payload offset
      2. Transport decrypt (nacl) using mode negotiated by discord.py
      3. DAVE frame decrypt using _connection.dave_session (managed by discord.py)
      4. Opus decode -> PCM -> per-user AudioBuffer

    SSRC->user_id mapping: monkey-patch voice WS received_message for JSON ops only.
    Binary frames (DAVE MLS opcodes) are passed through untouched so discord.py
    handles them in poll_event.
    """

    SILENCE_FRAME = b'\xf8\xff\xfe'

    def __init__(self, vc: discord.VoiceClient):
        self.vc             = vc
        self._conn          = vc._connection
        self.audio_data:    dict = {}
        self._ssrc_to_user: dict = {}
        self._decoders:     dict = {}
        self._lock          = threading.Lock()
        self._recording     = False
        self._hooked_ws     = None
        # debug counters
        self._dbg_total           = 0
        self._dbg_ok              = 0
        self._dbg_decrypt_fail    = 0
        self._dbg_no_ssrc         = 0
        self._dbg_not_whitelisted = 0
        self._dbg_paused          = 0
        self._dbg_silence         = 0
        self._dbg_decode_fail     = 0
        self._dbg_dave_fail       = 0
        self._dbg_dave_skip       = 0
        self._dbg_last_log        = 0
        self._dbg_dave_api_logged = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        self._recording = True
        self._conn.add_socket_listener(self._on_packet)
        self._hook_speaking()
        try:
            mode = self._conn.mode
        except Exception:
            mode = "unknown"
        print(f"   VoiceReceiver started (mode={mode})")

    def stop(self):
        self._recording = False
        try:
            self._conn.remove_socket_listener(self._on_packet)
        except Exception:
            pass
        self._unhook_speaking()
        print("   VoiceReceiver stopped")

    @property
    def is_recording(self) -> bool:
        return self._recording

    def rehook_if_needed(self):
        """Re-apply the SPEAKING hook if discord.py replaced the WS after a reconnect."""
        try:
            ws = self._conn.ws
        except Exception:
            return
        if ws is None or getattr(ws, '__speaking_hooked__', False):
            return
        print("   Re-hooking SPEAKING events after WS replacement")
        self._unhook_speaking()
        self._hook_speaking()

    # ------------------------------------------------------------------
    # SPEAKING hook (JSON messages only — op 5/12/13)
    # ------------------------------------------------------------------

    def _hook_speaking(self):
        try:
            ws = self._conn.ws
            if ws is None:
                print("   WARNING: Voice WS is None, cannot hook SPEAKING")
                return
        except Exception as e:
            print(f"   WARNING: Cannot access voice WS: {e}")
            return

        if getattr(ws, '__speaking_hooked__', False):
            return

        original = ws.received_message
        receiver = self
        print(f"   Hooking SPEAKING events on {type(ws).__name__} id={id(ws)}")

        async def patched(msg):
            # Only inspect JSON dicts — leave bytes alone for discord.py DAVE handler
            if isinstance(msg, dict):
                op   = msg.get('op')
                data = msg.get('d') or {}
                if op == 5:    # SPEAKING
                    ssrc    = data.get('ssrc')
                    user_id = data.get('user_id')
                    if ssrc is not None and user_id is not None:
                        uid = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = uid
                        print(f"   SPEAKING: ssrc={ssrc} -> user_id={uid}")
                elif op == 12:  # CLIENT_CONNECT
                    ssrc    = data.get('audio_ssrc')
                    user_id = data.get('user_id')
                    if ssrc and user_id:
                        uid = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = uid
                        print(f"   CLIENT_CONNECT: ssrc={ssrc} -> user_id={uid}")
                elif op == 13:  # CLIENT_DISCONNECT
                    user_id = data.get('user_id')
                    if user_id:
                        uid = int(user_id)
                        with receiver._lock:
                            gone = [s for s, u in receiver._ssrc_to_user.items() if u == uid]
                            for s in gone:
                                del receiver._ssrc_to_user[s]
                                receiver._decoders.pop(s, None)
                        print(f"   CLIENT_DISCONNECT: user_id={uid}")
            return await original(msg)

        ws.__speaking_hooked__           = True
        ws.__speaking_original_handler__ = original
        ws.received_message              = patched
        self._hooked_ws = ws

    def _unhook_speaking(self):
        ws = self._hooked_ws
        if ws is None:
            return
        if hasattr(ws, '__speaking_original_handler__'):
            try:
                ws.received_message = ws.__speaking_original_handler__
                del ws.__speaking_original_handler__
                del ws.__speaking_hooked__
            except Exception:
                pass
        self._hooked_ws = None

    # ------------------------------------------------------------------
    # DAVE decryption — uses discord.py's built-in dave_session
    # ------------------------------------------------------------------

    def _dave_decrypt(self, opus_data: bytes, user_id: int) -> bytes:
        """
        Apply DAVE E2EE decryption using discord.py's dave_session.
        discord.py master completes the MLS handshake automatically and
        exposes the session as _connection.dave_session.
        Returns decrypted bytes, or original bytes as fallback.
        """
        try:
            dave_session = getattr(self._conn, 'dave_session', None)
            if dave_session is None:
                self._dbg_dave_skip += 1
                return opus_data

            # Log available methods once
            if not self._dbg_dave_api_logged:
                self._dbg_dave_api_logged = True
                methods = [m for m in dir(dave_session) if not m.startswith('_')]
                print(f"   dave_session type={type(dave_session).__name__} methods={methods}")

            # discord.py master uses decrypt_frame(data, user_id_int)
            if hasattr(dave_session, 'decrypt_frame'):
                return dave_session.decrypt_frame(opus_data, user_id)
            elif hasattr(dave_session, 'decrypt_media'):
                return dave_session.decrypt_media(opus_data, user_id)
            elif hasattr(dave_session, 'decrypt'):
                return dave_session.decrypt(opus_data, user_id)
            else:
                self._dbg_dave_skip += 1
                return opus_data

        except Exception as e:
            self._dbg_dave_fail += 1
            if self._dbg_dave_fail <= 10 or self._dbg_dave_fail % 500 == 0:
                print(f"   DAVE decrypt failed user={user_id} #{self._dbg_dave_fail}: {type(e).__name__}: {e}")
            return opus_data

    # ------------------------------------------------------------------
    # Packet pipeline
    # ------------------------------------------------------------------

    def _on_packet(self, data: bytes):
        if not self._recording:
            return

        self._dbg_total += 1
        result = self._decrypt_transport(data)
        if result is None:
            self._dbg_decrypt_fail += 1
            self._maybe_log()
            return

        ssrc, opus_data = result

        with self._lock:
            user_id = self._ssrc_to_user.get(ssrc)

        if user_id is None:
            self._dbg_no_ssrc += 1
            self._maybe_log()
            return

        if user_id not in ALLOWED_USERS:
            self._dbg_not_whitelisted += 1
            self._maybe_log()
            return

        if is_user_paused(user_id):
            self._dbg_paused += 1
            self._maybe_log()
            return

        if opus_data == self.SILENCE_FRAME:
            self._dbg_silence += 1
            self._maybe_log()
            return

        # DAVE E2EE layer (discord.py handles MLS handshake; we just call decrypt)
        opus_data = self._dave_decrypt(opus_data, user_id)

        # Opus -> PCM
        try:
            decoder = self._get_decoder(ssrc)
            pcm     = decoder.decode(opus_data, fec=False)
        except Exception as e:
            self._dbg_decode_fail += 1
            if self._dbg_decode_fail <= 5:
                print(f"   Opus decode error ssrc={ssrc} user={user_id}: {type(e).__name__}: {e}")
            self._maybe_log()
            return

        self._dbg_ok += 1

        if user_id not in self.audio_data:
            self.audio_data[user_id] = AudioBuffer()
            print(f"   First audio stored for user {user_id}")
        self.audio_data[user_id].write(pcm)
        self._maybe_log()

    def _get_decoder(self, ssrc: int):
        if ssrc not in self._decoders:
            self._decoders[ssrc] = discord_opus.Decoder()
        return self._decoders[ssrc]

    # ------------------------------------------------------------------
    # Transport decryption
    # ------------------------------------------------------------------

    def _decrypt_transport(self, data: bytes):
        """
        Parse RTP and apply transport-layer decryption.
        Returns (ssrc, opus_bytes) or None.

        For aead_xchacha20_poly1305_rtpsize:
          - AAD = exactly the fixed 12-byte RTP header (NOT CSRC/extension bytes)
          - Encrypted payload starts at payload_offset (after all header extensions)
          - Nonce = last 4 bytes of packet, zero-padded to 24 bytes
        """
        if len(data) < 12:
            return None

        # Filter RTCP control packets (PT 200-204)
        pt = data[1] & 0x7F
        if pt >= 200:
            return None

        ssrc = struct.unpack_from('>I', data, 8)[0]

        try:
            if ssrc == self._conn.ssrc:
                return None
        except Exception:
            pass

        # Calculate full RTP header size (12 bytes + 4*CC CSRC entries + optional extension)
        byte0   = data[0]
        cc      = byte0 & 0x0F   # CSRC count
        has_ext = byte0 & 0x10   # extension bit
        header_size = 12 + 4 * cc
        if has_ext and len(data) > header_size + 3:
            # Extension: [profile(2)] [length_in_words(2)] [data(length*4)]
            ext_len_words = struct.unpack_from('>H', data, header_size + 2)[0]
            header_size += 4 + ext_len_words * 4

        if len(data) <= header_size:
            return None

        # Full RTP header (including CSRC) is the AAD for AEAD modes
        # Matches discord.py's encrypt: box.encrypt(data, aad=bytes(header), ...) where header includes CSRC
        full_header    = bytes(data[:header_size])
        encrypted_data = data[header_size:]

        if not getattr(self, '_dbg_hdr_logged', False):
            self._dbg_hdr_logged = True
            raw = bytes(data)
            print(
                f"   First RTP pkt: byte0=0x{byte0:02x} CC={cc} "
                f"ext={bool(has_ext)} header_size={header_size} total={len(raw)}"
            )
            # Full hex dump so we can verify AAD / nonce / ciphertext boundaries
            print(f"   PKT_HEX: {raw.hex()}")
            print(f"   HDR_HEX: {raw[:header_size].hex()}  ({header_size} bytes AAD)")
            print(f"   NON_HEX: {raw[-4:].hex()}  (nonce suffix)")
            print(f"   CIP_HEX: {raw[header_size:-4].hex()}  (ciphertext, {len(raw)-header_size-4} bytes)")

        try:
            mode       = self._conn.mode
            secret_key = bytes(self._conn.secret_key)

            if mode == 'aead_xchacha20_poly1305_rtpsize':
                opus = self._decrypt_aead_xchacha20(full_header, encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305_lite':
                opus = self._decrypt_xsalsa20_lite(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305_suffix':
                opus = self._decrypt_xsalsa20_suffix(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305':
                opus = self._decrypt_xsalsa20(full_header, encrypted_data, secret_key)
            else:
                if not getattr(self, '_unknown_mode_warned', False):
                    print(f"   WARNING: Unknown transport mode: {mode}")
                    self._unknown_mode_warned = True
                return None

        except Exception as e:
            if not hasattr(self, '_transport_err_count'):
                self._transport_err_count = 0
            self._transport_err_count += 1
            if self._transport_err_count <= 5:
                mode_str = getattr(self._conn, 'mode', '?')
                print(
                    f"   Transport decrypt #{self._transport_err_count}: "
                    f"{type(e).__name__}: {e} "
                    f"(mode={mode_str} payload_offset={payload_offset} "
                    f"enc_len={len(encrypted_data)} total={len(data)})"
                )
            return None

        if not opus:
            return None

        return (ssrc, opus)

    def _decrypt_aead_xchacha20(self, fixed_header: bytes, encrypted_payload: bytes, key: bytes) -> bytes:
        """
        aead_xchacha20_poly1305_rtpsize packet layout (from discord.py voice_client.py source):
          On-wire: [full RTP header (12+4*CC bytes)] [nacl ciphertext+MAC] [4-byte nonce]

          So given encrypted_payload = data[payload_offset:]:
            nonce   = last 4 bytes of encrypted_payload, zero-padded to 24 bytes
            cipher  = encrypted_payload[:-4]   (nacl ciphertext including 16-byte MAC)
            AAD     = the full RTP header passed as `fixed_header` (all payload_offset bytes,
                      including CSRC list — this is what discord.py passes as `header` to encrypt)
        """
        nonce_bytes = encrypted_payload[-4:]
        ciphertext  = encrypted_payload[:-4]
        nonce       = bytearray(24)
        nonce[:4]   = nonce_bytes
        box = nacl.secret.Aead(key)
        return box.decrypt(bytes(ciphertext), bytes(fixed_header), bytes(nonce))

    def _decrypt_xsalsa20(self, fixed_header: bytes, encrypted: bytes, key: bytes) -> bytes:
        box   = nacl.secret.SecretBox(key)
        nonce = bytearray(24)
        nonce[:12] = fixed_header[:12]
        return box.decrypt(bytes(encrypted), bytes(nonce))

    def _decrypt_xsalsa20_suffix(self, encrypted: bytes, key: bytes) -> bytes:
        box   = nacl.secret.SecretBox(key)
        nonce = encrypted[-24:]
        data  = encrypted[:-24]
        return box.decrypt(bytes(data), bytes(nonce))

    def _decrypt_xsalsa20_lite(self, encrypted: bytes, key: bytes) -> bytes:
        box   = nacl.secret.SecretBox(key)
        nonce = bytearray(24)
        nonce[:4] = encrypted[-4:]
        data  = encrypted[:-4]
        return box.decrypt(bytes(data), bytes(nonce))

    # ------------------------------------------------------------------
    # Debug stats
    # ------------------------------------------------------------------

    def _maybe_log(self):
        now = time.time()
        if now - self._dbg_last_log < 30:
            return
        self._dbg_last_log = now

        try:
            dave_session = getattr(self._conn, 'dave_session', None)
            dave_info = type(dave_session).__name__ if dave_session else "None"
        except Exception:
            dave_info = "error"

        with self._lock:
            ssrc_map = dict(self._ssrc_to_user)

        print(
            f"   STATS total={self._dbg_total} ok={self._dbg_ok} "
            f"transport_fail={self._dbg_decrypt_fail} no_ssrc={self._dbg_no_ssrc} "
            f"!whitelist={self._dbg_not_whitelisted} paused={self._dbg_paused} "
            f"silence={self._dbg_silence} opus_fail={self._dbg_decode_fail} "
            f"dave_fail={self._dbg_dave_fail} dave_skip={self._dbg_dave_skip} "
            f"dave_session={dave_info} ssrc_map={ssrc_map}"
        )


# ==============================================================================
# RECORDING SESSION
# ==============================================================================

class RecordingSession:
    def __init__(self, vc: discord.VoiceClient):
        self.vc          = vc
        self.guild       = vc.guild
        self.chunk_num   = 1
        self.chunk_start = time.time()
        self.date_str    = today_str()
        self.receiver    = None

    def _save_receiver(self, audio_data: dict, chunk_num: int):
        saved_folders = set()
        for user_id, buffer in audio_data.items():
            member    = self.guild.get_member(user_id)
            user_name = member.name if member else str(user_id)
            folder    = user_folder(user_id, user_name)
            try:
                pcm_data = buffer.read_all()
                if not pcm_data:
                    print(f"   Empty audio for {user_name}, skipping.")
                    continue
                mp3_data = pcm_to_mp3(pcm_data)
                if not mp3_data:
                    print(f"   MP3 conversion failed for {user_name}, saving PCM.")
                    fname = os.path.join(folder, f"{user_name}_part{chunk_num}.pcm")
                    with open(fname, 'wb') as f:
                        f.write(pcm_data)
                    continue
                fname = os.path.join(folder, f"{user_name}_part{chunk_num}.mp3")
                with open(fname, 'wb') as f:
                    f.write(mp3_data)
                print(f"   Saved chunk {chunk_num} for {user_name} ({len(mp3_data)//1024}KB)")
                saved_folders.add(folder)
            except Exception as e:
                print(f"   Failed to save audio for {user_name}: {e}")

        for folder in saved_folders:
            merge_chunks(folder, self.date_str)

    def start(self):
        self.receiver    = VoiceReceiver(self.vc)
        self.chunk_start = time.time()
        self.date_str    = today_str()
        self.receiver.start()
        print(f"Recording started - '{self.vc.channel.name}' chunk {self.chunk_num}")

    async def rotate(self):
        if not self.receiver or not self.receiver.is_recording:
            return
        current_chunk = self.chunk_num
        current_audio = self.receiver.audio_data
        print(f"Rotating chunk {current_chunk} in '{self.guild.name}'...")

        self.receiver.stop()
        self.chunk_num  += 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

        if self.vc.is_connected():
            self.receiver = VoiceReceiver(self.vc)
            self.receiver.start()
            print(f"Chunk {self.chunk_num} started in '{self.vc.channel.name}'")
        else:
            set_state(self.guild.id, State.IDLE)
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_receiver, current_audio, current_chunk)
        print(f"Chunk {current_chunk} saved.")

    async def stop(self, reason: str = "manual"):
        print(f"Stopping session ({reason}) in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        if self.receiver and self.receiver.is_recording:
            self.receiver.stop()

        audio_data = self.receiver.audio_data if self.receiver else {}
        chunk_num  = self.chunk_num
        if audio_data:
            print(f"Processing chunk {chunk_num} for '{self.guild.name}'...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._save_receiver, audio_data, chunk_num)
            print(f"Chunk {chunk_num} done.")

        set_state(self.guild.id, State.IDLE)
        await asyncio.sleep(1)

        if self.vc.is_connected():
            try:
                await self.vc.disconnect(force=True)
            except Exception as e:
                print(f"disconnect() error: {e}")

        guild_cooldown[self.guild.id] = time.time() + LEAVE_COOLDOWN
        print(f"Session ended in '{self.guild.name}'. Cooldown {LEAVE_COOLDOWN}s.")


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

        if gid in guild_cooldown and now >= guild_cooldown[gid]:
            del guild_cooldown[gid]

        if state == State.SAVING:
            continue

        if state == State.RECORDING:
            session = active_sessions.get(gid)

            if not vc or not vc.is_connected():
                print(f"Silent disconnect in '{guild.name}' - going IDLE.")
                if session and session.receiver and session.receiver.is_recording:
                    session.receiver.stop()
                active_sessions.pop(gid, None)
                set_state(gid, State.IDLE)
                guild_cooldown[gid] = now + LEAVE_COOLDOWN
                continue

            # Re-hook SPEAKING events if discord.py replaced WS after reconnect
            if session and session.receiver:
                session.receiver.rehook_if_needed()

            if not channel_has_target(vc.channel):
                print(f"No targets left in '{vc.channel.name}' - leaving.")
                active_sessions.pop(gid, None)
                if session:
                    await session.stop(reason="no targets")
                continue

            if session and (now - session.chunk_start) >= CHUNK_SECONDS:
                await session.rotate()

        elif state == State.IDLE:
            if gid in guild_cooldown:
                continue

            target = find_target_channel(guild)
            if not target:
                continue

            if vc and vc.is_connected():
                try:
                    await vc.disconnect(force=True)
                except Exception:
                    pass
                await asyncio.sleep(1)

            print(f"Joining '{target.name}' in '{guild.name}'...")
            try:
                new_vc  = await target.connect(timeout=60.0, self_deaf=False)
                session = RecordingSession(new_vc)
                session.start()
                active_sessions[gid] = session
                set_state(gid, State.RECORDING)
            except Exception as e:
                import traceback
                print(f"Failed to join '{target.name}': {e}")
                traceback.print_exc()
                guild_cooldown[gid] = now + LEAVE_COOLDOWN

# ==============================================================================
# DISCORD EVENTS
# ==============================================================================

@bot.event
async def on_ready():
    print(f"\nLogged in as {bot.user} (id={bot.user.id})")
    print(f"Monitoring {len(bot.guilds)} server(s) every {CHECK_INTERVAL}s\n")
    startup_cleanup()
    monitor.start()

@bot.event
async def on_voice_state_update(member, before, after):
    if member.id != bot.user.id:
        return
    if not (before.channel and not after.channel):
        return

    guild = before.channel.guild
    gid   = guild.id
    state = get_state(gid)

    if state == State.IDLE:
        return

    print(f"Bot disconnected from '{before.channel.name}' in '{guild.name}'")
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

@bot.command()
async def stop(ctx):
    """Save current chunk and pause. Bot rejoins automatically."""
    if not _whitelisted(ctx):
        await ctx.send("You are not allowed to use this command.")
        return
    gid   = ctx.guild.id
    state = get_state(gid)
    if state != State.RECORDING:
        await ctx.send("I'm not currently recording.")
        return
    session = active_sessions.pop(gid, None)
    await ctx.send("Saving chunk... I'll rejoin automatically when you're back.")
    if session:
        await session.stop(reason="!stop command")
    else:
        set_state(gid, State.IDLE)

@bot.command()
async def pause(ctx, minutes: int = None):
    """Pause your own recording for N minutes. Usage: !pause 30"""
    if not _whitelisted(ctx):
        await ctx.send("You are not allowed to use this command.")
        return
    if not minutes or minutes <= 0:
        await ctx.send("Usage: `!pause <minutes>`")
        return
    user_id   = ctx.author.id
    user_name = "".join(c for c in ctx.author.name if c.isalnum())
    await _flush_user_audio(ctx.guild, user_id, user_name)
    pause_user(user_id, minutes)
    resume_at = datetime.fromtimestamp(user_paused_until[user_id]).strftime("%H:%M:%S")
    await ctx.send(
        f"**{ctx.author.display_name}** paused for **{minutes}min**. "
        f"Resumes at **{resume_at}**. Use `!unpause` to resume early."
    )

@bot.command()
async def unpause(ctx):
    """Resume recording before the pause expires."""
    if not _whitelisted(ctx):
        await ctx.send("You are not allowed to use this command.")
        return
    if not is_user_paused(ctx.author.id):
        await ctx.send(f"**{ctx.author.display_name}** — you are not paused.")
        return
    unpause_user(ctx.author.id)
    await ctx.send(f"**{ctx.author.display_name}** — recording resumed.")

@bot.command(name="continue")
async def cmd_continue(ctx):
    """Alias for !unpause."""
    await unpause(ctx)

@bot.command()
async def whitelist(ctx):
    """Show the recording whitelist."""
    if not ALLOWED_USERS:
        await ctx.send("Whitelist is empty.")
    else:
        lines = "\n".join(f"- `{uid}`" for uid in ALLOWED_USERS)
        await ctx.send(f"**Recording {len(ALLOWED_USERS)} user(s):**\n{lines}")

@bot.command()
async def allow(ctx, user_id: int):
    """Add a user to the recording whitelist. Usage: !allow <user_id>"""
    ALLOWED_USERS.add(user_id)
    _save_whitelist()
    await ctx.send(f"`{user_id}` added. ({len(ALLOWED_USERS)} tracked)")

@bot.command()
async def unallow(ctx, user_id: int):
    """Remove a user from the recording whitelist. Usage: !unallow <user_id>"""
    ALLOWED_USERS.discard(user_id)
    _save_whitelist()
    msg = f"({len(ALLOWED_USERS)} remaining)" if ALLOWED_USERS else "Whitelist is now empty."
    await ctx.send(f"`{user_id}` removed. {msg}")

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

@bot.command()
async def status(ctx):
    """Show current recording status and DAVE session info for debugging."""
    gid     = ctx.guild.id
    state   = get_state(gid)
    session = active_sessions.get(gid)

    lines = [f"**State:** `{state.name}`"]

    if session and session.receiver:
        r = session.receiver
        try:
            conn         = session.vc._connection
            dave_session = getattr(conn, 'dave_session', None)
            dave_info    = type(dave_session).__name__ if dave_session else "None"
            mode         = getattr(conn, 'mode', 'unknown')
        except Exception:
            dave_info = "error"
            mode = "unknown"

        with r._lock:
            ssrc_map = dict(r._ssrc_to_user)

        lines += [
            f"**Transport mode:** `{mode}`",
            f"**dave_session:** `{dave_info}`",
            f"**SSRC map:** `{ssrc_map}`",
            f"**Chunk:** `{session.chunk_num}` ({int(time.time()-session.chunk_start)}s elapsed)",
            f"**Users with audio:** `{list(r.audio_data.keys())}`",
            f"**Packets ok/total:** `{r._dbg_ok}/{r._dbg_total}`",
            f"**Transport failures:** `{r._dbg_decrypt_fail}`",
            f"**DAVE fail/skip:** `{r._dbg_dave_fail}` / `{r._dbg_dave_skip}`",
            f"**Opus failures:** `{r._dbg_decode_fail}`",
        ]
    await ctx.send("\n".join(lines))

@bot.command()
async def restart(ctx):
    """Save recordings and restart the bot process."""
    if not _whitelisted(ctx):
        await ctx.send("You are not allowed to use this command.")
        return
    await ctx.send("Saving and restarting...")
    await _shutdown_all()
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ==============================================================================
# FLUSH HELPER
# ==============================================================================

async def _flush_user_audio(guild: discord.Guild, user_id: int, user_name: str):
    session = active_sessions.get(guild.id)
    if not session or not session.receiver or not session.receiver.is_recording:
        return
    if user_id not in session.receiver.audio_data:
        return
    buffer = session.receiver.audio_data[user_id]
    try:
        pcm_data = buffer.read_all()
        if not pcm_data:
            return
        folder   = user_folder(user_id, user_name)
        mp3_data = pcm_to_mp3(pcm_data)
        if mp3_data:
            fname = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.mp3")
        else:
            fname    = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.pcm")
            mp3_data = pcm_data
        with open(fname, 'wb') as f:
            f.write(mp3_data)
        print(f"   Pre-pause flush for {user_name}")
        merge_chunks(folder, today_str())
        buffer.reset()
        session.chunk_num += 1
    except Exception as e:
        print(f"   Pre-pause flush failed for {user_name}: {e}")

# ==============================================================================
# GRACEFUL SHUTDOWN
# ==============================================================================

async def _shutdown_all():
    print("Saving all recordings before shutdown...")
    for gid, session in list(active_sessions.items()):
        vc = session.vc
        if vc and vc.is_connected():
            print(f"   Saving '{session.guild.name}'...")
            if session.receiver and session.receiver.is_recording:
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
    print("Shutdown complete.")

# ==============================================================================
# ENTRY POINT
# ==============================================================================

if TOKEN:
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        asyncio.run(_shutdown_all())
else:
    print("ERROR: DISCORD_TOKEN not set.")