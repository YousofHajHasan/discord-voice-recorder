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

# DAVE / davey import — graceful fallback so code loads even without the package
try:
    import davey
    DAVE_AVAILABLE = True
    print("✅ davey package loaded successfully")
except ImportError:
    davey = None
    DAVE_AVAILABLE = False
    print("⚠️  davey package NOT available — DAVE decryption will be skipped (audio may be garbage on E2EE channels)")

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
# AUDIO BUFFER
# ==============================================================================

class AudioBuffer:
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
            return self._buf.tell() == 0


def pcm_to_mp3(pcm_data: bytes) -> bytes:
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

# ==============================================================================
# DAVE SESSION MANAGER
# ==============================================================================

class DaveSessionManager:
    """
    Manages the davey.Session lifecycle for one VoiceClient.

    The voice websocket carries DAVE protocol opcodes as binary frames.
    We monkey-patch the VoiceWebSocket.received_message to intercept them.

    Relevant opcodes (from Discord docs / discord.py gateway.py source):
      21 = DAVE_PROTOCOL_PREPARE_TRANSITION   (downgrade to version 0)
      22 = DAVE_PROTOCOL_EXECUTE_TRANSITION
      23 = DAVE_PROTOCOL_TRANSITION_READY     (we SEND this)
      24 = DAVE_PROTOCOL_PREPARE_EPOCH
      25 = DAVE_MLS_EXTERNAL_SENDER
      26 = DAVE_MLS_KEY_PACKAGE               (we SEND this)
      27 = DAVE_MLS_PROPOSALS
      28 = DAVE_MLS_COMMIT_WELCOME            (we SEND this)
      29 = DAVE_MLS_ANNOUNCE_COMMIT_TRANSITION
      30 = DAVE_MLS_WELCOME

    davey Python API (mirrors the JS npm package @snazzah/davey):
      davey.Session(version, user_id_str, channel_id_str)
      session.set_external_sender(bytes)
      session.get_serialized_key_package() -> bytes
      session.process_proposals(op_type, proposals_bytes) -> CommitWelcome | None
      session.process_commit(commit_bytes)
      session.process_welcome(welcome_bytes)
      session.create_key_ratchet(user_id_str) -> KeyRatchet
      ratchet.decrypt(frame_bytes) -> bytes    # DAVE frame decrypt
      davey.ProposalsOperationType.APPEND / REVOKE
    """

    # Voice WS opcodes
    OP_DAVE_PREPARE_TRANSITION          = 21
    OP_DAVE_EXECUTE_TRANSITION          = 22
    OP_DAVE_TRANSITION_READY            = 23
    OP_DAVE_PREPARE_EPOCH               = 24
    OP_DAVE_MLS_EXTERNAL_SENDER         = 25
    OP_DAVE_MLS_KEY_PACKAGE             = 26
    OP_DAVE_MLS_PROPOSALS               = 27
    OP_DAVE_MLS_COMMIT_WELCOME          = 28
    OP_DAVE_MLS_ANNOUNCE_COMMIT         = 29
    OP_DAVE_MLS_WELCOME                 = 30

    def __init__(self, vc: discord.VoiceClient, user_id: int, channel_id: int):
        self.vc         = vc
        self.user_id    = user_id
        self.channel_id = channel_id
        self._lock      = threading.Lock()
        self._session   = None          # davey.Session instance
        self._version   = 0             # current DAVE protocol version
        self._ratchets: dict = {}       # user_id_str -> KeyRatchet
        self._pending_transitions: dict = {}  # transition_id -> version
        self._ready     = False
        self._hooked    = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if not DAVE_AVAILABLE:
            print("   [DAVE] davey not available, skipping DAVE setup")
            return
        self._hook_ws()
        print(f"   [DAVE] DaveSessionManager started (user={self.user_id}, channel={self.channel_id})")

    def stop(self):
        self._unhook_ws()
        with self._lock:
            self._session = None
            self._ratchets.clear()
            self._ready = False
        print("   [DAVE] DaveSessionManager stopped")

    @property
    def is_ready(self) -> bool:
        return self._ready and self._session is not None

    # ------------------------------------------------------------------
    # Frame decryption (called from VoiceReceiver._on_packet per-user)
    # ------------------------------------------------------------------

    def decrypt_frame(self, opus_data: bytes, ssrc_user_id_str: str) -> bytes:
        """
        Attempt DAVE frame decryption.
        Returns decrypted bytes on success, or original bytes as fallback.
        """
        if not DAVE_AVAILABLE or not self._ready:
            return opus_data

        with self._lock:
            ratchet = self._ratchets.get(ssrc_user_id_str)
            if ratchet is None:
                # Lazily create the ratchet for this user
                if self._session is None:
                    return opus_data
                try:
                    ratchet = self._session.create_key_ratchet(ssrc_user_id_str)
                    self._ratchets[ssrc_user_id_str] = ratchet
                    print(f"   [DAVE] Created key ratchet for user {ssrc_user_id_str}")
                except Exception as e:
                    print(f"   [DAVE] ⚠️ create_key_ratchet({ssrc_user_id_str}) failed: {e}")
                    return opus_data

        try:
            decrypted = ratchet.decrypt(opus_data)
            return decrypted
        except Exception as e:
            # Throttle log spam
            if not hasattr(self, '_decrypt_err_count'):
                self._decrypt_err_count = 0
            self._decrypt_err_count += 1
            if self._decrypt_err_count <= 10 or self._decrypt_err_count % 500 == 0:
                print(f"   [DAVE] ⚠️ decrypt_frame failed for {ssrc_user_id_str} (#{self._decrypt_err_count}): {e}")
            return opus_data  # fallback: pass undecrypted to Opus decoder (will fail, but at least won't crash)

    # ------------------------------------------------------------------
    # WS hook / unhook
    # ------------------------------------------------------------------

    def _hook_ws(self):
        try:
            ws = self.vc._connection.ws
            if ws is None:
                print("   [DAVE] ⚠️ Voice WS is None, cannot hook")
                return
        except Exception as e:
            print(f"   [DAVE] ⚠️ Cannot access voice WS: {e}")
            return

        if getattr(ws, '__dave_hooked__', False):
            print("   [DAVE] WS already hooked")
            return

        original = ws.received_message
        manager  = self
        print(f"   [DAVE] Hooking voice WS ({type(ws).__name__})")

        async def patched(msg):
            # Binary frames carry DAVE opcodes
            if isinstance(msg, (bytes, bytearray)):
                await manager._handle_binary_op(ws, bytes(msg))
            return await original(msg)

        ws.__dave_hooked__           = True
        ws.__dave_original_handler__ = original
        ws.received_message          = patched
        self._hooked = True

    def _unhook_ws(self):
        if not self._hooked:
            return
        try:
            ws = self.vc._connection.ws
            if ws and hasattr(ws, '__dave_original_handler__'):
                ws.received_message = ws.__dave_original_handler__
                del ws.__dave_original_handler__
                del ws.__dave_hooked__
        except Exception:
            pass
        self._hooked = False

    # ------------------------------------------------------------------
    # Binary opcode handler
    # ------------------------------------------------------------------

    async def _handle_binary_op(self, ws, data: bytes):
        """
        Binary voice WS frame format (from Discord docs):
          [1 byte seq_ack_flag?] [2 bytes seq (big-endian, optional)] [1 byte opcode] [payload...]
        discord.py gateway.py shows:  seq = struct.unpack_from('>H', msg, 1)[0], op = msg[3], payload = msg[5:]
        i.e.  byte 0 = ? (ignored), bytes 1-2 = seq, byte 3 = op, bytes 4+ = payload
        Actually from discord.py source: seq is at offset 1 (2 bytes), op at offset 3, payload at offset 4.
        But some frames omit the seq entirely. We try both layouts.
        """
        if len(data) < 2:
            return

        # Layout: [flags(1)] [seq(2)] [op(1)] [payload...]
        if len(data) >= 4:
            op      = data[3]
            payload = data[4:]
        else:
            op      = data[1]
            payload = data[2:]

        print(f"   [DAVE] Binary op={op} payload_len={len(payload)}")

        if op == self.OP_DAVE_PREPARE_EPOCH:
            await self._on_prepare_epoch(ws, payload)

        elif op == self.OP_DAVE_MLS_EXTERNAL_SENDER:
            await self._on_external_sender(ws, payload)

        elif op == self.OP_DAVE_MLS_PROPOSALS:
            await self._on_proposals(ws, payload)

        elif op == self.OP_DAVE_MLS_ANNOUNCE_COMMIT:
            await self._on_commit_transition(ws, payload)

        elif op == self.OP_DAVE_MLS_WELCOME:
            await self._on_welcome(ws, payload)

        elif op == self.OP_DAVE_EXECUTE_TRANSITION:
            await self._on_execute_transition(ws, payload)

        elif op == self.OP_DAVE_PREPARE_TRANSITION:
            await self._on_prepare_transition(ws, payload)

    # ------------------------------------------------------------------
    # DAVE opcode handlers
    # ------------------------------------------------------------------

    async def _on_prepare_epoch(self, ws, payload: bytes):
        """
        Opcode 24 — DAVE Protocol Prepare Epoch.
        Payload: [1 byte epoch_id? / version?] possibly just signals that a new MLS group needs to be created.
        discord.py gateway.py: epoch == 1 → create a new MLS group.
        """
        if not DAVE_AVAILABLE:
            return
        if len(payload) < 3:
            print(f"   [DAVE] OP_PREPARE_EPOCH: payload too short ({len(payload)} bytes), raw={payload.hex()}")
            return

        # Layout from discord.py gateway.py: epoch_id at offset 0 (1 byte), version at offset 1 (2 bytes big-endian)
        epoch_id = payload[0]
        version  = struct.unpack_from('>H', payload, 1)[0] if len(payload) >= 3 else 1

        print(f"   [DAVE] Prepare epoch: epoch_id={epoch_id} version={version}")

        with self._lock:
            self._version = version
            self._ratchets.clear()  # Keys are invalid for the new epoch

        try:
            with self._lock:
                self._session = davey.Session(
                    version,
                    str(self.user_id),
                    str(self.channel_id),
                )
            key_package = self._session.get_serialized_key_package()
            print(f"   [DAVE] Created new Session v{version}, key_package={len(key_package)} bytes")

            # Send OP 26 MLS Key Package
            await self._send_binary(ws, self.OP_DAVE_MLS_KEY_PACKAGE, key_package)

        except Exception as e:
            print(f"   [DAVE] ❌ Failed to create davey.Session: {e}")
            import traceback; traceback.print_exc()

    async def _on_external_sender(self, ws, payload: bytes):
        """Opcode 25 — set the external sender blob."""
        if not DAVE_AVAILABLE or self._session is None:
            return
        try:
            self._session.set_external_sender(payload)
            print(f"   [DAVE] External sender set ({len(payload)} bytes)")
        except Exception as e:
            print(f"   [DAVE] ⚠️ set_external_sender failed: {e}")

    async def _on_proposals(self, ws, payload: bytes):
        """
        Opcode 27 — MLS Proposals.
        Payload: [1 byte op_type (0=append,1=revoke)] [proposals_bytes...]
        discord.py gateway.py shows optype = payload[0], proposals = payload[1:] (but discord.py uses msg[4:] with different offsets — we need to check)
        Actually from discord.py: optype = struct.unpack_from('B', msg, 4)[0], proposals = msg[5:]
        Since we already stripped the 4-byte header in _handle_binary_op, payload here = msg[4:], so optype = payload[0], data = payload[1:]
        """
        if not DAVE_AVAILABLE or self._session is None:
            return

        if len(payload) < 1:
            print(f"   [DAVE] OP_PROPOSALS: empty payload")
            return

        optype_byte = payload[0]
        proposals_data = payload[1:]

        op_type = (davey.ProposalsOperationType.APPEND
                   if optype_byte == 0
                   else davey.ProposalsOperationType.REVOKE)

        print(f"   [DAVE] Processing proposals: op={optype_byte} data={len(proposals_data)} bytes")

        try:
            result = self._session.process_proposals(op_type, proposals_data)
            if result is not None:
                # result is a CommitWelcome object
                commit_bytes  = result.commit
                welcome_bytes = result.welcome  # may be None
                payload_out   = commit_bytes + welcome_bytes if welcome_bytes else commit_bytes
                await self._send_binary(ws, self.OP_DAVE_MLS_COMMIT_WELCOME, payload_out)
                print(f"   [DAVE] Sent CommitWelcome: commit={len(commit_bytes)}b welcome={len(welcome_bytes) if welcome_bytes else 0}b")
        except Exception as e:
            print(f"   [DAVE] ⚠️ process_proposals failed: {e}")
            import traceback; traceback.print_exc()

    async def _on_commit_transition(self, ws, payload: bytes):
        """
        Opcode 29 — MLS Announce Commit Transition (existing members).
        Payload: [2 bytes transition_id big-endian] [commit_bytes...]
        discord.py: transition_id = struct.unpack_from('>H', msg, 3)[0], commit = msg[5:]
        With our offset adjustment: transition_id at payload[0:2], commit at payload[2:]
        """
        if not DAVE_AVAILABLE or self._session is None:
            return

        if len(payload) < 2:
            print(f"   [DAVE] OP_ANNOUNCE_COMMIT: payload too short")
            return

        transition_id = struct.unpack_from('>H', payload, 0)[0]
        commit_data   = payload[2:]

        print(f"   [DAVE] Processing commit: transition_id={transition_id} commit={len(commit_data)} bytes")

        try:
            self._session.process_commit(commit_data)
            with self._lock:
                if transition_id != 0:
                    self._pending_transitions[transition_id] = self._version
                self._ratchets.clear()  # All ratchets invalidated on commit
            # Signal ready
            await self._send_transition_ready(ws, transition_id)
            print(f"   [DAVE] Commit processed, sent transition ready (id={transition_id})")
        except Exception as e:
            print(f"   [DAVE] ⚠️ process_commit failed: {e}")
            import traceback; traceback.print_exc()

    async def _on_welcome(self, ws, payload: bytes):
        """
        Opcode 30 — MLS Welcome (new members being added to group).
        Payload: [2 bytes transition_id] [welcome_bytes...]
        """
        if not DAVE_AVAILABLE or self._session is None:
            return

        if len(payload) < 2:
            print(f"   [DAVE] OP_WELCOME: payload too short")
            return

        transition_id = struct.unpack_from('>H', payload, 0)[0]
        welcome_data  = payload[2:]

        print(f"   [DAVE] Processing welcome: transition_id={transition_id} welcome={len(welcome_data)} bytes")

        try:
            self._session.process_welcome(welcome_data)
            with self._lock:
                if transition_id != 0:
                    self._pending_transitions[transition_id] = self._version
                self._ratchets.clear()
            await self._send_transition_ready(ws, transition_id)
            print(f"   [DAVE] Welcome processed, sent transition ready (id={transition_id})")
        except Exception as e:
            print(f"   [DAVE] ⚠️ process_welcome failed: {e}")
            import traceback; traceback.print_exc()

    async def _on_execute_transition(self, ws, payload: bytes):
        """
        Opcode 22 — Execute Transition.
        After this, senders use the new protocol version / key ratchet.
        """
        if len(payload) >= 2:
            transition_id = struct.unpack_from('>H', payload, 0)[0]
        else:
            transition_id = 0

        print(f"   [DAVE] Execute transition id={transition_id}")

        with self._lock:
            if transition_id in self._pending_transitions:
                del self._pending_transitions[transition_id]
            self._ready = (self._session is not None)

        print(f"   [DAVE] Session ready={self._ready}")

    async def _on_prepare_transition(self, ws, payload: bytes):
        """
        Opcode 21 — Prepare Transition (downgrade to protocol version 0).
        We clear the session and signal ready.
        """
        if len(payload) >= 2:
            transition_id = struct.unpack_from('>H', payload, 0)[0]
        else:
            transition_id = 0

        print(f"   [DAVE] Prepare downgrade transition id={transition_id}")

        with self._lock:
            self._session = None
            self._ratchets.clear()
            self._ready   = False
            self._version = 0

        await self._send_transition_ready(ws, transition_id)

    # ------------------------------------------------------------------
    # Send helpers
    # ------------------------------------------------------------------

    async def _send_binary(self, ws, op: int, payload: bytes):
        """Send a binary voice WS frame: [0x00] [seq=0x0000] [op] [payload]"""
        frame = bytes([0x00, 0x00, 0x00, op]) + payload
        try:
            await ws.send_as_binary(frame)
        except Exception as e:
            print(f"   [DAVE] ⚠️ send_binary op={op} failed: {e}")

    async def _send_transition_ready(self, ws, transition_id: int):
        payload = struct.pack('>H', transition_id)
        await self._send_binary(ws, self.OP_DAVE_TRANSITION_READY, payload)


# ==============================================================================
# VOICE RECEIVER
# ==============================================================================

class VoiceReceiver:
    """
    Raw UDP socket listener that decrypts and decodes Discord voice packets.

    Pipeline:
      UDP packet → RTP parse → Transport decrypt (nacl) → DAVE decrypt (davey) → Opus decode → PCM buffer
    """

    SILENCE_FRAME = b'\xf8\xff\xfe'

    def __init__(self, vc: discord.VoiceClient, dave_manager: 'DaveSessionManager'):
        self.vc           = vc
        self._conn        = vc._connection
        self._dave        = dave_manager
        self.audio_data: dict = {}       # user_id -> AudioBuffer
        self._ssrc_to_user: dict = {}    # ssrc -> user_id
        self._decoders: dict = {}        # ssrc -> OpusDecoder
        self._lock        = threading.Lock()
        self._recording   = False
        self._dbg_total   = 0
        self._dbg_ok      = 0
        self._dbg_decrypt_fail = 0
        self._dbg_no_ssrc = 0
        self._dbg_not_whitelisted = 0
        self._dbg_paused  = 0
        self._dbg_silence = 0
        self._dbg_decode_fail = 0
        self._dbg_dave_fail = 0
        self._dbg_last_log = 0

    def start(self):
        self._recording = True
        self._conn.add_socket_listener(self._on_packet)
        self._hook_ws_speaking()
        try:
            mode = self._conn.mode
            print(f"   🎙️ VoiceReceiver started (mode={mode})")
        except Exception:
            print(f"   🎙️ VoiceReceiver started (mode=unknown)")

    def stop(self):
        self._recording = False
        try:
            self._conn.remove_socket_listener(self._on_packet)
        except Exception:
            pass
        self._unhook_ws_speaking()
        print(f"   🎙️ VoiceReceiver stopped")

    @property
    def is_recording(self) -> bool:
        return self._recording

    # ------------------------------------------------------------------
    # WS hook for SPEAKING events (SSRC → user_id mapping)
    # ------------------------------------------------------------------

    def _hook_ws_speaking(self):
        try:
            ws = self._conn.ws
            if ws is None:
                print("   ⚠️ Voice WS not available for SPEAKING hook")
                return
        except Exception as e:
            print(f"   ⚠️ Cannot hook SPEAKING: {e}")
            return

        if getattr(ws, '__speaking_hooked__', False):
            print("   ℹ️ SPEAKING hook already in place")
            return

        original  = ws.received_message
        receiver  = self
        print(f"   🔗 Hooking SPEAKING events on {type(ws).__name__}")

        async def patched(msg: dict):
            if isinstance(msg, dict):
                op   = msg.get('op')
                data = msg.get('d', {})
                if op == 5:   # SPEAKING
                    ssrc    = data.get('ssrc')
                    user_id = data.get('user_id')
                    if ssrc is not None and user_id is not None:
                        user_id = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = user_id
                        print(f"   🔊 SPEAKING map: ssrc={ssrc} → user_id={user_id}")
                elif op == 12:  # CLIENT_CONNECT
                    ssrc    = data.get('audio_ssrc')
                    user_id = data.get('user_id')
                    if ssrc and user_id:
                        user_id = int(user_id)
                        with receiver._lock:
                            receiver._ssrc_to_user[ssrc] = user_id
                        print(f"   🔗 CLIENT_CONNECT: ssrc={ssrc} → user_id={user_id}")
                elif op == 13:  # CLIENT_DISCONNECT
                    user_id = data.get('user_id')
                    if user_id:
                        user_id = int(user_id)
                        with receiver._lock:
                            to_rm = [s for s, u in receiver._ssrc_to_user.items() if u == user_id]
                            for s in to_rm:
                                del receiver._ssrc_to_user[s]
                                receiver._decoders.pop(s, None)
                        print(f"   🔌 CLIENT_DISCONNECT: user_id={user_id}")
            return await original(msg)

        ws.__speaking_hooked__           = True
        ws.__speaking_original_handler__ = original
        ws.received_message              = patched

    def _unhook_ws_speaking(self):
        try:
            ws = self._conn.ws
            if ws and hasattr(ws, '__speaking_original_handler__'):
                ws.received_message = ws.__speaking_original_handler__
                del ws.__speaking_original_handler__
                del ws.__speaking_hooked__
        except Exception:
            pass

    # ------------------------------------------------------------------
    # RTP / decrypt / decode pipeline
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

        # DAVE frame decryption (E2EE layer)
        if DAVE_AVAILABLE and self._dave.is_ready:
            try:
                opus_data = self._dave.decrypt_frame(opus_data, str(user_id))
            except Exception as e:
                self._dbg_dave_fail += 1
                if self._dbg_dave_fail <= 5:
                    print(f"   [DAVE] ⚠️ decrypt_frame exception: {e}")

        # Opus → PCM
        try:
            decoder = self._get_decoder(ssrc)
            pcm     = decoder.decode(opus_data, fec=False)
        except Exception as e:
            self._dbg_decode_fail += 1
            if self._dbg_decode_fail <= 5:
                print(f"   ⚠️ Opus decode error for ssrc={ssrc} user={user_id}: {e}")
            self._maybe_log()
            return

        self._dbg_ok += 1

        if user_id not in self.audio_data:
            self.audio_data[user_id] = AudioBuffer()
            print(f"   📝 First audio stored for user {user_id}")
        self.audio_data[user_id].write(pcm)
        self._maybe_log()

    def _get_decoder(self, ssrc: int):
        if ssrc not in self._decoders:
            self._decoders[ssrc] = discord_opus.Decoder()
        return self._decoders[ssrc]

    def _decrypt_transport(self, data: bytes):
        """Parse RTP header and decrypt transport encryption layer."""
        if len(data) < 12:
            return None

        pt = data[1] & 0x7F
        if pt >= 200:  # RTCP
            return None

        ssrc = struct.unpack_from('>I', data, 8)[0]

        try:
            if ssrc == self._conn.ssrc:
                return None
        except Exception:
            pass

        byte0      = data[0]
        cc         = byte0 & 0x0F
        has_ext    = byte0 & 0x10
        header_len = 12 + 4 * cc
        if has_ext:
            header_len += 4

        if len(data) <= header_len:
            return None

        header         = bytes(data[:header_len])
        encrypted_data = data[header_len:]

        # Log first packet header info
        if not hasattr(self, '_dbg_hdr_logged'):
            self._dbg_hdr_logged = True
            print(f"   🔎 First RTP pkt: byte0=0x{byte0:02x} CC={cc} ext={bool(has_ext)} hdr_len={header_len} total={len(data)}")

        try:
            mode       = self._conn.mode
            secret_key = bytes(self._conn.secret_key)

            if mode == 'aead_xchacha20_poly1305_rtpsize':
                opus = self._decrypt_aead_xchacha20(header, encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305_lite':
                opus = self._decrypt_xsalsa20_lite(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305_suffix':
                opus = self._decrypt_xsalsa20_suffix(encrypted_data, secret_key)
            elif mode == 'xsalsa20_poly1305':
                opus = self._decrypt_xsalsa20(header, encrypted_data, secret_key)
            else:
                if not hasattr(self, '_unknown_mode_warned'):
                    print(f"   ⚠️ Unknown transport mode: {mode}")
                    self._unknown_mode_warned = True
                return None
        except Exception as e:
            if not hasattr(self, '_transport_err_count'):
                self._transport_err_count = 0
            self._transport_err_count += 1
            if self._transport_err_count <= 5:
                print(f"   ⚠️ Transport decrypt error: {type(e).__name__}: {e} (mode={getattr(self._conn,'mode','?')})")
            return None

        if not opus:
            return None

        return (ssrc, opus)

    def _decrypt_aead_xchacha20(self, header: bytes, after_header: bytes, key: bytes) -> bytes:
        nonce_bytes = after_header[-4:]
        encrypted   = after_header[:-4]
        nonce       = bytearray(24)
        nonce[:4]   = nonce_bytes
        box = nacl.secret.Aead(key)
        return box.decrypt(bytes(encrypted), bytes(header), bytes(nonce))

    def _decrypt_xsalsa20(self, header: bytes, encrypted: bytes, key: bytes) -> bytes:
        box   = nacl.secret.SecretBox(key)
        nonce = bytearray(24)
        nonce[:12] = header[:12]
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

    def _maybe_log(self):
        now = time.time()
        if now - self._dbg_last_log < 30:
            return
        self._dbg_last_log = now
        with self._lock:
            ssrc_map = dict(self._ssrc_to_user)
        print(
            f"   📊 Packets total={self._dbg_total} ok={self._dbg_ok} "
            f"transport_fail={self._dbg_decrypt_fail} no_ssrc={self._dbg_no_ssrc} "
            f"!whitelist={self._dbg_not_whitelisted} paused={self._dbg_paused} "
            f"silence={self._dbg_silence} opus_fail={self._dbg_decode_fail} "
            f"dave_fail={self._dbg_dave_fail} "
            f"dave_ready={self._dave.is_ready} "
            f"ssrc_map={ssrc_map}"
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
        # Create DAVE manager first so it can hook WS before receiver starts
        self.dave        = DaveSessionManager(
            vc,
            user_id    = bot.user.id,
            channel_id = vc.channel.id,
        )
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
                    print(f"   ⚠️ Empty audio for {user_name}, skipping.")
                    continue
                mp3_data = pcm_to_mp3(pcm_data)
                if not mp3_data:
                    print(f"   ⚠️ MP3 conversion failed for {user_name}, saving raw PCM.")
                    chunk_file = os.path.join(folder, f"{user_name}_part{chunk_num}.pcm")
                    with open(chunk_file, 'wb') as f:
                        f.write(pcm_data)
                    continue
                chunk_file = os.path.join(folder, f"{user_name}_part{chunk_num}.mp3")
                with open(chunk_file, 'wb') as f:
                    f.write(mp3_data)
                print(f"   ✅ Saved chunk {chunk_num} for {user_name} ({len(mp3_data)//1024}KB)")
                saved_folders.add(folder)
            except Exception as e:
                print(f"   ❌ Failed to save audio for {user_name}: {e}")

        for folder in saved_folders:
            merge_chunks(folder, self.date_str)

    def start(self):
        # Start DAVE manager first (hooks WS for MLS opcodes)
        self.dave.start()
        # Then start receiver
        self.receiver    = VoiceReceiver(self.vc, self.dave)
        self.chunk_start = time.time()
        self.date_str    = today_str()
        self.receiver.start()
        print(f"▶️  Recording started — '{self.vc.channel.name}' chunk {self.chunk_num}")

    async def rotate(self):
        if not self.receiver or not self.receiver.is_recording:
            return

        current_chunk = self.chunk_num
        current_audio = self.receiver.audio_data
        print(f"🔄 Rotating chunk {current_chunk} in '{self.guild.name}'...")

        self.receiver.stop()
        self.chunk_num  += 1
        self.chunk_start = time.time()
        self.date_str    = today_str()

        if self.vc.is_connected():
            self.receiver = VoiceReceiver(self.vc, self.dave)
            self.receiver.start()
            print(f"▶️  Chunk {self.chunk_num} started in '{self.vc.channel.name}'")
        else:
            set_state(self.guild.id, State.IDLE)
            return

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_receiver, current_audio, current_chunk)
        print(f"✅ Chunk {current_chunk} saved.")

    async def stop(self, reason: str = "manual"):
        print(f"⏹️  Stopping session ({reason}) in '{self.guild.name}'...")
        set_state(self.guild.id, State.SAVING)

        if self.receiver and self.receiver.is_recording:
            self.receiver.stop()

        self.dave.stop()

        audio_data = self.receiver.audio_data if self.receiver else {}
        chunk_num  = self.chunk_num
        if audio_data:
            print(f"💾 Processing chunk {chunk_num} for '{self.guild.name}'...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._save_receiver, audio_data, chunk_num)
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
                print(f"⚠️  Silent disconnect in '{guild.name}' — going IDLE.")
                if session and session.receiver and session.receiver.is_recording:
                    session.receiver.stop()
                if session:
                    session.dave.stop()
                active_sessions.pop(gid, None)
                set_state(gid, State.IDLE)
                guild_cooldown[gid] = now + LEAVE_COOLDOWN
                continue

            if not channel_has_target(vc.channel):
                print(f"No targets left in '{vc.channel.name}' — leaving.")
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

            print(f"🎯 Joining '{target.name}' in '{guild.name}'...")
            try:
                new_vc  = await target.connect(timeout=60.0, self_deaf=False)
                session = RecordingSession(new_vc)
                session.start()
                active_sessions[gid] = session
                set_state(gid, State.RECORDING)
            except Exception as e:
                import traceback
                print(f"❌ Failed to join '{target.name}': {e}")
                traceback.print_exc()
                guild_cooldown[gid] = now + LEAVE_COOLDOWN

# ==============================================================================
# DISCORD EVENTS
# ==============================================================================

@bot.event
async def on_ready():
    print(f"\n🤖 Logged in as {bot.user} (id={bot.user.id})")
    print(f"   Monitoring {len(bot.guilds)} server(s) every {CHECK_INTERVAL}s")
    print(f"   DAVE support: {'✅ enabled' if DAVE_AVAILABLE else '❌ disabled (install davey)'}\n")
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

@bot.command()
async def stop(ctx):
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

@bot.command()
async def pause(ctx, minutes: int = None):
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    if not minutes or minutes <= 0:
        await ctx.send("⚠️ Usage: `!pause <minutes>`")
        return
    user_id   = ctx.author.id
    user_name = "".join(c for c in ctx.author.name if c.isalnum())
    await _flush_user_audio(ctx.guild, user_id, user_name)
    pause_user(user_id, minutes)
    resume_at = datetime.fromtimestamp(user_paused_until[user_id]).strftime("%H:%M:%S")
    await ctx.send(
        f"⏸️ **{ctx.author.display_name}** paused for **{minutes}min**. "
        f"Resumes at **{resume_at}**. Use `!unpause` to resume early."
    )

@bot.command()
async def unpause(ctx):
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    if not is_user_paused(ctx.author.id):
        await ctx.send(f"▶️ **{ctx.author.display_name}** — you are not paused.")
        return
    unpause_user(ctx.author.id)
    await ctx.send(f"▶️ **{ctx.author.display_name}** — recording resumed.")

@bot.command(name="continue")
async def cmd_continue(ctx):
    await unpause(ctx)

@bot.command()
async def whitelist(ctx):
    if not ALLOWED_USERS:
        await ctx.send("📋 Whitelist is empty.")
    else:
        lines = "\n".join(f"• `{uid}`" for uid in ALLOWED_USERS)
        await ctx.send(f"📋 **Recording {len(ALLOWED_USERS)} user(s):**\n{lines}")

@bot.command()
async def allow(ctx, user_id: int):
    ALLOWED_USERS.add(user_id)
    _save_whitelist()
    await ctx.send(f"✅ `{user_id}` added. ({len(ALLOWED_USERS)} tracked)")

@bot.command()
async def unallow(ctx, user_id: int):
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

@bot.command()
async def dave_status(ctx):
    """Show DAVE session status for debugging."""
    gid     = ctx.guild.id
    session = active_sessions.get(gid)
    if not session:
        await ctx.send("⚠️ No active recording session.")
        return
    dm = session.dave
    await ctx.send(
        f"🔐 **DAVE Status**\n"
        f"• davey available: `{DAVE_AVAILABLE}`\n"
        f"• Session ready: `{dm.is_ready}`\n"
        f"• Protocol version: `{dm._version}`\n"
        f"• Ratchets: `{list(dm._ratchets.keys())}`\n"
        f"• Pending transitions: `{list(dm._pending_transitions.keys())}`\n"
        f"• WS hooked: `{dm._hooked}`"
    )

@bot.command()
async def restart(ctx):
    if not _whitelisted(ctx):
        await ctx.send("⛔ You are not allowed to use this command.")
        return
    await ctx.send("🔄 Saving and restarting...")
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
            chunk_file = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.mp3")
            with open(chunk_file, 'wb') as f:
                f.write(mp3_data)
        else:
            chunk_file = os.path.join(folder, f"{user_name}_part{session.chunk_num}_pre_pause.pcm")
            with open(chunk_file, 'wb') as f:
                f.write(pcm_data)
        print(f"   💾 Pre-pause flush for {user_name}")
        merge_chunks(folder, today_str())
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
            if session.receiver and session.receiver.is_recording:
                session.receiver.stop()
                audio_data = session.receiver.audio_data
                if audio_data:
                    session._save_receiver(audio_data, session.chunk_num)
            session.dave.stop()
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