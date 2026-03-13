#include <dpp/dpp.h>
#include <fstream>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <ctime>
#include <unordered_set>
#include <sys/stat.h>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
std::string DISCORD_TOKEN;
std::string BASE_PATH = "recordings/";
int CHUNK_TIME = 60;
std::unordered_set<dpp::snowflake> ALLOWED_CHANNELS;
std::unordered_set<dpp::snowflake> ALLOWED_USERS;

// ---------------------------------------------------------------------------
// Global State
// ---------------------------------------------------------------------------
std::map<dpp::snowflake, std::vector<uint8_t>> audio_buffers;
std::map<dpp::snowflake, bool>                 udp_hole_punched;
std::map<dpp::snowflake, bool>                 guild_transitioning;

// FIX: atomic flag per guild to signal the UDP punch thread to stop safely
std::map<dpp::snowflake, std::shared_ptr<std::atomic<bool>>> punch_active;

std::mutex                                     bot_mutex;

// ---------------------------------------------------------------------------
// Dedicated FFmpeg worker thread
// ---------------------------------------------------------------------------
std::queue<std::function<void()>> ffmpeg_queue;
std::mutex                        ffmpeg_mutex;
std::condition_variable           ffmpeg_cv;
std::atomic<bool>                 ffmpeg_running{true};

void ffmpeg_worker() {
    std::cout << "[FFMPEG_WORKER] Thread started\n";
    while (ffmpeg_running || !ffmpeg_queue.empty()) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lock(ffmpeg_mutex);
            std::cout << "[FFMPEG_WORKER] Waiting for job...\n";
            ffmpeg_cv.wait(lock, [] { return !ffmpeg_queue.empty() || !ffmpeg_running; });
            if (ffmpeg_queue.empty()) {
                std::cout << "[FFMPEG_WORKER] Queue empty, looping\n";
                continue;
            }
            job = std::move(ffmpeg_queue.front());
            ffmpeg_queue.pop();
            std::cout << "[FFMPEG_WORKER] Job dequeued, queue size now: " << ffmpeg_queue.size() << "\n";
        }
        std::cout << "[FFMPEG_WORKER] Executing job...\n";
        job();
        std::cout << "[FFMPEG_WORKER] Job done\n";
    }
    std::cout << "[FFMPEG_WORKER] Thread exiting\n";
}

void enqueue_ffmpeg(std::function<void()> job) {
    std::cout << "[ENQUEUE_FFMPEG] Enqueueing job\n";
    {
        std::lock_guard<std::mutex> lock(ffmpeg_mutex);
        ffmpeg_queue.push(std::move(job));
        std::cout << "[ENQUEUE_FFMPEG] Job pushed, queue size now: " << ffmpeg_queue.size() << "\n";
    }
    ffmpeg_cv.notify_one();
    std::cout << "[ENQUEUE_FFMPEG] Worker notified\n";
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
void load_env_file() {
    std::cout << "[LOAD_ENV] Reading .env file...\n";
    std::ifstream file(".env");
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string key   = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        if      (key == "DISCORD_TOKEN") { DISCORD_TOKEN = value; std::cout << "[LOAD_ENV] DISCORD_TOKEN loaded\n"; }
        else if (key == "BASE_PATH") {
            BASE_PATH = value;
            if (!BASE_PATH.empty() && BASE_PATH.back() != '/') BASE_PATH += '/';
            std::cout << "[LOAD_ENV] BASE_PATH = " << BASE_PATH << "\n";
        }
        else if (key == "CHUNK_TIME") {
            CHUNK_TIME = std::stoi(value);
            std::cout << "[LOAD_ENV] CHUNK_TIME = " << CHUNK_TIME << "\n";
        }
        else if (key == "ALLOWED_CHANNELS" || key == "ALLOWED_USERS") {
            std::stringstream ss(value);
            std::string item;
            while (std::getline(ss, item, ',')) {
                if (key == "ALLOWED_CHANNELS") {
                    ALLOWED_CHANNELS.insert(std::stoull(item));
                    std::cout << "[LOAD_ENV] ALLOWED_CHANNEL: " << item << "\n";
                } else {
                    ALLOWED_USERS.insert(std::stoull(item));
                    std::cout << "[LOAD_ENV] ALLOWED_USER: " << item << "\n";
                }
            }
        }
    }
    std::cout << "[LOAD_ENV] Done\n";
}

void ensure_directory_exists(const std::string& path) {
    std::cout << "[DIR] Ensuring directory exists: " << path << "\n";
    std::system(("mkdir -p \"" + path + "\"").c_str());
}

// ---------------------------------------------------------------------------
// Reliable MP3 append
// FIX: filelist now uses absolute paths to avoid FFmpeg resolving them
// relative to the filelist's own directory, which was doubling the path.
// ---------------------------------------------------------------------------
void append_mp3(const std::string& daily_mp3, const std::string& chunk_mp3,
                const std::string& folder_path) {
    std::cout << "[APPEND_MP3] Entering — daily: " << daily_mp3 << " chunk: " << chunk_mp3 << "\n";

    struct stat st;
    if (stat(daily_mp3.c_str(), &st) != 0) {
        std::rename(chunk_mp3.c_str(), daily_mp3.c_str());
        std::cout << "[APPEND_MP3] No daily file yet — chunk promoted to daily: " << daily_mp3 << "\n";
        return;
    }

    // Resolve absolute paths so FFmpeg concat demuxer doesn't prepend the
    // filelist's directory to paths that are already relative.
    char abs_daily[PATH_MAX];
    char abs_chunk[PATH_MAX];
    if (!realpath(daily_mp3.c_str(), abs_daily) ||
        !realpath(chunk_mp3.c_str(), abs_chunk)) {
        std::cout << "[APPEND_MP3] ERROR — could not resolve absolute paths\n";
        return;
    }
    std::cout << "[APPEND_MP3] Absolute daily: " << abs_daily << "\n";
    std::cout << "[APPEND_MP3] Absolute chunk:  " << abs_chunk << "\n";

    std::string filelist = folder_path + "/filelist.txt";
    {
        std::ofstream fl(filelist);
        fl << "file '" << abs_daily << "'\n";
        fl << "file '" << abs_chunk  << "'\n";
    }
    std::cout << "[APPEND_MP3] filelist.txt written\n";

    std::string temp_out = folder_path + "/temp_combined.mp3";
    std::string cmd = "ffmpeg -y -f concat -safe 0 -i \"" + filelist
                      + "\" -c copy \"" + temp_out + "\" > /dev/null 2>&1";
    std::cout << "[APPEND_MP3] Running ffmpeg concat...\n";
    int rc = std::system(cmd.c_str());
    std::cout << "[APPEND_MP3] ffmpeg exited with code: " << rc << "\n";

    if (rc == 0 && stat(temp_out.c_str(), &st) == 0 && st.st_size > 0) {
        std::rename(temp_out.c_str(), daily_mp3.c_str());
        std::cout << "[APPEND_MP3] Success — daily file updated: " << daily_mp3 << "\n";
    } else {
        std::cout << "[APPEND_MP3] ERROR — ffmpeg concat failed for " << daily_mp3
                  << " — chunk preserved at " << chunk_mp3 << "\n";
        std::remove(filelist.c_str());
        return;
    }

    std::remove(filelist.c_str());
    std::remove(chunk_mp3.c_str());
    std::cout << "[APPEND_MP3] Temp files cleaned up\n";
    std::cout << "[APPEND_MP3] Leaving\n";
}

// ---------------------------------------------------------------------------
// Save buffers
// ---------------------------------------------------------------------------
void save_and_clear_buffers(dpp::cluster& bot) {
    std::cout << "[SAVE] Entering save_and_clear_buffers\n";

    std::map<dpp::snowflake, std::vector<uint8_t>> local_buffers;
    {
        std::lock_guard<std::mutex> lock(bot_mutex);
        if (audio_buffers.empty()) {
            std::cout << "[SAVE] No buffers to save, exiting early\n";
            return;
        }
        local_buffers = std::move(audio_buffers);
        std::cout << "[SAVE] Swapped " << local_buffers.size() << " user buffer(s)\n";
    }

    auto now_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char date_str[32];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", std::localtime(&now_c));
    std::string current_date(date_str);
    std::cout << "[SAVE] Current date: " << current_date << "\n";

    ensure_directory_exists(BASE_PATH);

    for (auto& [user_id, buffer] : local_buffers) {
        if (buffer.empty()) {
            std::cout << "[SAVE] Buffer for user " << user_id << " is empty, skipping\n";
            continue;
        }

        std::cout << "[SAVE] Processing user " << user_id
                  << " — buffer size: " << buffer.size() << " bytes\n";

        std::string folder_path = BASE_PATH + std::to_string(user_id);
        std::string daily_mp3   = folder_path + "/" + current_date + ".mp3";

        std::string username = "unknown";
        dpp::user* u = dpp::find_user(user_id);
        if (u) {
            username = u->username;
            std::cout << "[SAVE] Resolved username: " << username << "\n";
        } else {
            std::cout << "[SAVE] Could not resolve username for user " << user_id << "\n";
        }

        enqueue_ffmpeg([buf = std::move(buffer),
                        folder_path, daily_mp3, username, user_id]() mutable {

            std::cout << "[FFMPEG_JOB] Starting job for user " << user_id << "\n";

            ensure_directory_exists(folder_path);

            {
                std::ofstream meta(folder_path + "/username.txt", std::ios::trunc);
                meta << username << "\n";
                std::cout << "[FFMPEG_JOB] username.txt written: " << username << "\n";
            }

            std::string pcm_path  = folder_path + "/temp_chunk.pcm";
            std::string chunk_mp3 = folder_path + "/temp_chunk.mp3";

            {
                std::ofstream f(pcm_path, std::ios::binary);
                f.write(reinterpret_cast<const char*>(buf.data()), buf.size());
                std::cout << "[FFMPEG_JOB] PCM written: " << buf.size() << " bytes → " << pcm_path << "\n";
            }

            std::string enc = "ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"" + pcm_path
                              + "\" -b:a 192k \"" + chunk_mp3 + "\" > /dev/null 2>&1";
            std::cout << "[FFMPEG_JOB] Encoding PCM → MP3...\n";
            int rc = std::system(enc.c_str());
            std::cout << "[FFMPEG_JOB] ffmpeg encode exited with code: " << rc << "\n";
            std::remove(pcm_path.c_str());

            struct stat st;
            if (stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                std::cout << "[FFMPEG_JOB] ERROR — PCM->MP3 encode failed for " << folder_path << "\n";
                return;
            }
            std::cout << "[FFMPEG_JOB] chunk MP3 size: " << st.st_size << " bytes\n";

            append_mp3(daily_mp3, chunk_mp3, folder_path);
            std::cout << "[FFMPEG_JOB] Job complete for user " << user_id << "\n";
        });
    }

    std::cout << "[SAVE] Leaving save_and_clear_buffers\n";
}

// ---------------------------------------------------------------------------
// Verify bot placement
// ---------------------------------------------------------------------------
void verify_bot_placement(dpp::cluster& bot) {
    std::cout << "[VERIFY] Entering verify_bot_placement\n";

    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) { std::cout << "[VERIFY] Channel " << channel_id << " not found in cache\n"; continue; }
        dpp::guild* g = dpp::find_guild(c->guild_id);
        if (!g) { std::cout << "[VERIFY] Guild not found for channel " << channel_id << "\n"; continue; }
        if (guild_transitioning[g->id]) {
            std::cout << "[VERIFY] Guild " << g->id << " is transitioning, skipping\n";
            continue;
        }

        auto bot_state = g->voice_members.find(bot.me.id);
        if (bot_state == g->voice_members.end()) {
            std::cout << "[VERIFY] Bot not in any voice channel in guild " << g->id << "\n";
            continue;
        }

        dpp::snowflake bot_channel = bot_state->second.channel_id;
        if (bot_channel == 0) {
            std::cout << "[VERIFY] Bot channel_id is 0, skipping\n";
            continue;
        }

        bool in_allowed = ALLOWED_CHANNELS.count(bot_channel) > 0;
        std::cout << "[VERIFY] Bot is in channel " << bot_channel
                  << " — allowed: " << (in_allowed ? "yes" : "no") << "\n";

        int allowed_present = 0;
        for (const auto& [u_id, state] : g->voice_members) {
            if (state.channel_id == bot_channel && ALLOWED_USERS.count(u_id))
                allowed_present++;
        }
        std::cout << "[VERIFY] Allowed users present in bot's channel: " << allowed_present << "\n";

        dpp::discord_client* shard = bot.get_shard(g->shard_id);

        if (!in_allowed) {
            std::cout << "[VERIFY] Bot is in non-allowed channel " << bot_channel << " — disconnecting\n";
            guild_transitioning[g->id] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
        } else if (allowed_present == 0) {
            std::cout << "[VERIFY] Bot is alone in channel " << bot_channel << " — disconnecting\n";
            guild_transitioning[g->id] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
        }
    }

    std::cout << "[VERIFY] Leaving verify_bot_placement\n";
}

// ---------------------------------------------------------------------------
// Evaluate VCs
// ---------------------------------------------------------------------------
void evaluate_vcs(dpp::cluster& bot) {
    std::cout << "[EVAL] Entering evaluate_vcs\n";

    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) { std::cout << "[EVAL] Channel " << channel_id << " not found in cache\n"; continue; }
        dpp::guild* g = dpp::find_guild(c->guild_id);
        if (!g) { std::cout << "[EVAL] Guild not found for channel " << channel_id << "\n"; continue; }

        if (guild_transitioning[g->id]) {
            std::cout << "[EVAL] Guild " << g->id << " is transitioning, skipping\n";
            continue;
        }

        int allowed_present = 0;
        for (const auto& [u_id, state] : g->voice_members) {
            if (state.channel_id == channel_id && ALLOWED_USERS.count(u_id))
                allowed_present++;
        }
        std::cout << "[EVAL] Allowed users in channel " << channel_id << ": " << allowed_present << "\n";

        auto bot_state   = g->voice_members.find(bot.me.id);
        bool bot_in_this = (bot_state != g->voice_members.end()
                            && bot_state->second.channel_id == channel_id);
        bool bot_in_any  = (bot_state != g->voice_members.end()
                            && bot_state->second.channel_id != 0);

        std::cout << "[EVAL] bot_in_this=" << bot_in_this << " bot_in_any=" << bot_in_any << "\n";

        dpp::discord_client* shard = bot.get_shard(g->shard_id);

        if (allowed_present > 0 && !bot_in_this && !bot_in_any) {
            std::cout << "[EVAL] Allowed user detected — joining channel " << channel_id << "\n";
            guild_transitioning[g->id] = true;
            udp_hole_punched[g->id]    = false;

            // FIX: create a new active flag for this guild's punch thread
            punch_active[g->id] = std::make_shared<std::atomic<bool>>(true);

            if (shard) shard->connect_voice(g->id, channel_id);
        }
        else if (allowed_present == 0 && bot_in_this) {
            std::cout << "[EVAL] No allowed users left — disconnecting from channel " << channel_id << "\n";
            guild_transitioning[g->id] = true;

            // FIX: signal the punch thread to stop before disconnecting
            if (punch_active.count(g->id) && punch_active[g->id])
                punch_active[g->id]->store(false);

            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
        }
    }

    std::cout << "[EVAL] Leaving evaluate_vcs\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
int main() {
    std::cout.setf(std::ios::unitbuf);
    std::cout << "[MAIN] Bot starting...\n";

    load_env_file();
    if (DISCORD_TOKEN.empty()) {
        std::cout << "[MAIN] ERROR — Could not read DISCORD_TOKEN from .env!\n";
        return 1;
    }

    std::cout << "[MAIN] Starting FFmpeg worker thread\n";
    std::thread worker(ffmpeg_worker);

    dpp::cluster bot(DISCORD_TOKEN, dpp::i_all_intents);

    bot.on_log([](const dpp::log_t& event) {
        if (event.message.find("decrypt failed") != std::string::npos) return;
        if (event.message.find("SSL Error: 0")   != std::string::npos) return;
        if (event.severity >= dpp::ll_warning)
            std::cout << "[WARN/ERR] " << event.message << "\n";
    });

    bot.on_ready([&bot](const dpp::ready_t&) {
        std::cout << "[READY] Bot online! Chunk time: " << CHUNK_TIME
                  << "s | Save Path: " << BASE_PATH << "\n";

        bot.start_timer([&bot](dpp::timer) {
            std::cout << "[TIMER] Save timer fired\n";
            save_and_clear_buffers(bot);
        }, CHUNK_TIME);

        bot.start_timer([&bot](dpp::timer) {
            std::cout << "[TIMER] 10s watcher timer fired\n";
            verify_bot_placement(bot);
            evaluate_vcs(bot);
        }, 10);
    });

    bot.on_voice_state_update([&bot](const dpp::voice_state_update_t& event) {
        std::cout << "[VOICE_STATE] User " << event.state.user_id
                  << " state update in guild " << event.state.guild_id << "\n";

        if (event.state.user_id == bot.me.id) {
            dpp::snowflake gid = event.state.guild_id;
            if (guild_transitioning[gid]) {
                std::cout << "[VOICE_STATE] Bot transition confirmed — clearing lock for guild " << gid << "\n";
                guild_transitioning[gid] = false;
            }
            return;
        }
        evaluate_vcs(bot);
    });

    bot.on_voice_ready([](const dpp::voice_ready_t& event) {
        std::cout << "[VOICE_READY] Voice connection ready — starting UDP hole punch\n";
        dpp::discord_voice_client* vc       = event.voice_client;
        dpp::snowflake             guild_id = vc->server_id;

        // FIX: capture a shared_ptr to the active flag instead of a raw vc pointer.
        // If the bot disconnects before UDP is open, the flag is set to false and
        // the thread exits safely without touching the destroyed voice client.
        std::shared_ptr<std::atomic<bool>> active;
        if (punch_active.count(guild_id))
            active = punch_active[guild_id];
        else
            active = std::make_shared<std::atomic<bool>>(true);

        std::thread([vc, guild_id, active]() {
            std::cout << "[UDP_PUNCH] Thread started for guild " << guild_id << "\n";
            uint16_t silence[5760] = {0};
            int attempts = 0;
            while (active->load() && !udp_hole_punched[guild_id] && attempts < 20) {
                std::cout << "[UDP_PUNCH] Sending silence, attempt " << (attempts + 1) << "/20\n";
                if (vc && vc->is_ready()) vc->send_audio_raw(silence, sizeof(silence));
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                attempts++;
            }

            if (!active->load())
                std::cout << "[UDP_PUNCH] Stopped early — bot disconnected for guild " << guild_id << "\n";
            else if (udp_hole_punched[guild_id])
                std::cout << "[UDP_PUNCH] UDP route confirmed open for guild " << guild_id << "\n";
            else
                std::cout << "[UDP_PUNCH] Gave up after 20 attempts for guild " << guild_id << "\n";

            std::cout << "[UDP_PUNCH] Thread exiting for guild " << guild_id << "\n";
        }).detach();
    });

    bot.on_voice_receive([](const dpp::voice_receive_t& event) {
        if (event.user_id == 0) return;
        if (!ALLOWED_USERS.count(event.user_id)) return;

        dpp::snowflake guild_id = event.voice_client->server_id;

        if (!udp_hole_punched[guild_id]) {
            std::cout << "[VOICE_RECV] First real packet received — UDP open for guild " << guild_id << "\n";
            udp_hole_punched[guild_id] = true;

            // Signal punch thread to stop since UDP is now confirmed open
            if (punch_active.count(guild_id) && punch_active[guild_id])
                punch_active[guild_id]->store(false);
        }

        std::lock_guard<std::mutex> lock(bot_mutex);
        auto& buf = audio_buffers[event.user_id];
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(event.audio_data.data());
        buf.insert(buf.end(), ptr, ptr + event.audio_data.size());

        // Log buffer size every 1000 packets to avoid log spam
        static std::map<dpp::snowflake, int> packet_counts;
        if (++packet_counts[event.user_id] % 1000 == 0) {
            std::cout << "[VOICE_RECV] User " << event.user_id
                      << " — packets: " << packet_counts[event.user_id]
                      << " — buffer: " << buf.size() << " bytes\n";
        }
    });

    std::cout << "[MAIN] Starting bot event loop\n";
    bot.start(dpp::st_wait);

    std::cout << "[MAIN] Bot shutting down — stopping FFmpeg worker\n";
    ffmpeg_running = false;
    ffmpeg_cv.notify_all();
    worker.join();
    std::cout << "[MAIN] Clean shutdown complete\n";

    return 0;
}