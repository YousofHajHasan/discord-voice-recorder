import discord
from discord.ext import commands, voice_recv
import time
import subprocess
import os
import glob # Helps find files to merge
from dotenv import load_dotenv

# CONFIGURATION
load_dotenv()
CHUNK_TIME = int(os.getenv('CHUNK_TIME', 300))
BASE_DIR = "Recordings" # Where to save everything
TOKEN = os.getenv('DISCORD_TOKEN')

print(f"Bot configured with CHUNK_TIME={CHUNK_TIME} seconds.")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Stores state: { user_id: {'start_time': 123, 'chunk_num': 1, 'current_file': 'path/to/file.pcm'} }
user_sessions = {}

def ensure_folder(user):
    """Creates a folder for the user if it doesn't exist."""
    safe_name = "".join(x for x in user.name if x.isalnum())
    folder_path = os.path.join(BASE_DIR, f"{safe_name}_{user.id}")
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

def convert_and_delete_pcm(pcm_filename):
    """
    1. Converts PCM -> MP3
    2. Deletes the PCM file immediately after success
    Running as a shell chain ensures we don't delete before conversion finishes.
    """
    if not os.path.exists(pcm_filename):
        return

    mp3_filename = pcm_filename.replace('.pcm', '.mp3')
    print(f"Bg Task: Converting & Deleting {pcm_filename}...")
    
    # The command: ffmpeg ... && rm ...
    # This ensures 'rm' only runs if ffmpeg succeeds.
    cmd = (
        f"ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"{pcm_filename}\" \"{mp3_filename}\" "
        f"-loglevel error && rm \"{pcm_filename}\""
    )
    
    # Run in background (shell=True is required for && to work)
    subprocess.Popen(cmd, shell=True)

def merge_user_audio(folder_path, output_name="Full_Recording.mp3"):
    """
    Finds all mp3 chunks in the user's folder and merges them into one file.
    """
    # 1. Find all MP3 chunks (exclude previous full recordings)
    chunks = sorted(glob.glob(os.path.join(folder_path, "*_part*.mp3")))
    
    if not chunks:
        return

    # 2. Create a text file list for ffmpeg
    list_file_path = os.path.join(folder_path, "files_to_merge.txt")
    with open(list_file_path, 'w') as f:
        for chunk in chunks:
            # ffmpeg requires format: file '/path/to/file.mp3'
            f.write(f"file '{os.path.abspath(chunk)}'\n")

    # 3. Run the merge command
    output_path = os.path.join(folder_path, output_name)
    print(f"Merging {len(chunks)} files into {output_path}...")
    
    subprocess.run([
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0', 
        '-i', list_file_path, '-c', 'copy', output_path
    ])
    
    # 4. Cleanup the list file (optional: delete chunks too if you want ONLY the full file)
    os.remove(list_file_path)

@bot.command()
async def join(ctx):
    if not ctx.author.voice:
        await ctx.send("You are not in a voice channel!")
        return

    voice_channel = ctx.author.voice.channel
    vc = await voice_channel.connect(cls=voice_recv.VoiceRecvClient)
    vc.listen(voice_recv.BasicSink(callback_function))
    await ctx.send(f"Joined! Recording to folder: `{BASE_DIR}/`")

def callback_function(user, data: voice_recv.VoiceData):
    if not data.pcm:
        return

    current_time = time.time()
    user_id = user.id
    
    # Initialize User Session
    if user_id not in user_sessions:
        folder_path = ensure_folder(user)
        filename = os.path.join(folder_path, f"{user.name}_part1.pcm")
        
        user_sessions[user_id] = {
            'start_time': current_time, 
            'chunk_num': 1,
            'current_file': filename,
            'folder': folder_path,
            'user_name': user.name
        }

    session = user_sessions[user_id]

    # Check Timer
    if current_time - session['start_time'] > CHUNK_TIME:
        old_file = session['current_file']
        
        # Switch to new file
        session['chunk_num'] += 1
        session['start_time'] = current_time
        new_filename = os.path.join(session['folder'], f"{session['user_name']}_part{session['chunk_num']}.pcm")
        session['current_file'] = new_filename
        
        # Convert & Delete OLD file
        convert_and_delete_pcm(old_file)

    # Write Data
    with open(session['current_file'], 'ab') as f:
        f.write(data.pcm)

@bot.command()
async def stop(ctx):
    if ctx.voice_client:
        ctx.voice_client.stop()
        await ctx.voice_client.disconnect()
        await ctx.send("Processing final files... (This might take a second)")

        # 1. Finish converting all active chunks
        pending_conversions = []
        for user_id, session in user_sessions.items():
            last_file = session['current_file']
            # We use Popen, so these happen in parallel
            convert_and_delete_pcm(last_file)
            pending_conversions.append(session['folder'])

        # 2. Clear session memory
        user_sessions.clear()

        # 3. Wait a moment for ffmpeg to finish the last chunks (simple sleep)
        # In a complex app, you'd check process PIDs, but this is usually enough.
        await discord.utils.sleep_until(discord.utils.utcnow() + discord.utils.timedelta(seconds=2))

        # 4. Merge Everything
        for folder in set(pending_conversions):
            merge_user_audio(folder)

        await ctx.send("✅ All recordings processed and merged!")
    else:
        await ctx.send("Not recording.")

if TOKEN:
    bot.run(TOKEN)
else:
    print("ERROR: No DISCORD_TOKEN found in environment variables!")