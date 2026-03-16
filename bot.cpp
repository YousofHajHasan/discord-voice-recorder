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
#include <unordered_map>
#include <sys/stat.h>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
std::string DISCORD_TOKEN;
std::string BASE_PATH = "recordings/";
int CHUNK_TIME = 300; // Defaulting to 300 as per your logs
std::unordered_set<dpp::snowflake> ALLOWED_CHANNELS;
std::unordered_set<dpp::snowflake> ALLOWED_USERS;

// ---------------------------------------------------------------------------
// Global State
// ---------------------------------------------------------------------------
std::map<dpp::snowflake, std::vector<uint8_t>> audio_buffers;
std::map<dpp::snowflake, bool>                 udp_hole_punched;
std::map<dpp::snowflake, bool>                 guild_transitioning;
std::map<dpp::snowflake, std::shared_ptr<std::atomic<bool>>> punch_active;

// New global trackers for silence injection and logging
std::map<dpp::snowflake, std::chrono::time_point<std::chrono::steady_clock>> last_packet_time;
std::map<dpp::snowflake, int> packet_counts;
std::mutex bot_mutex;

// Thread-safe map for Voice States: guild_id -> (user_id -> channel_id)
std::map<dpp::snowflake, std::map<dpp::snowflake, dpp::snowflake>> safe_voice_states;
std::mutex state_mutex;

// ---------------------------------------------------------------------------
// Dedicated FFmpeg worker thread
// ---------------------------------------------------------------------------
std::queue<std::function<void()>> ffmpeg_queue;
std::mutex                        ffmpeg_mutex;
std::condition_variable           ffmpeg_cv;
std::atomic<bool>                 ffmpeg_running{true};

void ffmpeg_worker() {
    while (ffmpeg_running || !ffmpeg_queue.empty()) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lock(ffmpeg_mutex);
            ffmpeg_cv.wait(lock, [] { return !ffmpeg_queue.empty() || !ffmpeg_running; });
            if (ffmpeg_queue.empty()) continue;
            job = std::move(ffmpeg_queue.front());
            ffmpeg_queue.pop();
        }
        job();
    }
    std::cout << "[FFMPEG_WORKER] Thread exiting\n";
}

void enqueue_ffmpeg(std::function<void()> job) {
    {
        std::lock_guard<std::mutex> lock(ffmpeg_mutex);
        ffmpeg_queue.push(std::move(job));
    }
    ffmpeg_cv.notify_one();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
void load_env_file() {
    std::ifstream file(".env");
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string key   = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        if      (key == "DISCORD_TOKEN") DISCORD_TOKEN = value;
        else if (key == "BASE_PATH") {
            BASE_PATH = value;
            if (!BASE_PATH.empty() && BASE_PATH.back() != '/') BASE_PATH += '/';
        }
        else if (key == "CHUNK_TIME") CHUNK_TIME = std::stoi(value);
        else if (key == "ALLOWED_CHANNELS" || key == "ALLOWED_USERS") {
            std::stringstream ss(value);
            std::string item;
            while (std::getline(ss, item, ',')) {
                if (key == "ALLOWED_CHANNELS") ALLOWED_CHANNELS.insert(std::stoull(item));
                else                           ALLOWED_USERS.insert(std::stoull(item));
            }
        }
    }
}

void ensure_directory_exists(const std::string& path) {
    std::system(("mkdir -p \"" + path + "\"").c_str());
}

// ---------------------------------------------------------------------------
// Reliable MP3 append
// ---------------------------------------------------------------------------
void append_mp3(const std::string& daily_mp3, const std::string& chunk_mp3,
                const std::string& folder_path) {
    struct stat st;
    if (stat(daily_mp3.c_str(), &st) != 0) {
        std::rename(chunk_mp3.c_str(), daily_mp3.c_str());
        std::cout << "[APPEND_MP3] Created new daily file: " << daily_mp3 << "\n";
        return;
    }

    char abs_daily[PATH_MAX];
    char abs_chunk[PATH_MAX];
    if (!realpath(daily_mp3.c_str(), abs_daily) ||
        !realpath(chunk_mp3.c_str(), abs_chunk)) {
        std::cout << "[APPEND_MP3] ERROR — could not resolve absolute paths\n";
        return;
    }

    std::string filelist = folder_path + "/filelist.txt";
    {
        std::ofstream fl(filelist);
        fl << "file '" << abs_daily << "'\n";
        fl << "file '" << abs_chunk  << "'\n";
    }

    std::string temp_out = folder_path + "/temp_combined.mp3";
    std::string cmd = "ffmpeg -y -f concat -safe 0 -i \"" + filelist
                      + "\" -c copy \"" + temp_out + "\" > /dev/null 2>&1";
    int rc = std::system(cmd.c_str());

    if (rc == 0 && stat(temp_out.c_str(), &st) == 0 && st.st_size > 0) {
        std::rename(temp_out.c_str(), daily_mp3.c_str());
        std::cout << "[APPEND_MP3] Appended to daily file: " << daily_mp3 << "\n";
    } else {
        std::cout << "[APPEND_MP3] ERROR — concat failed (code " << rc << ") — chunk preserved: " << chunk_mp3 << "\n";
        std::remove(filelist.c_str());
        return;
    }

    std::remove(filelist.c_str());
    std::remove(chunk_mp3.c_str());
}

// ---------------------------------------------------------------------------
// Save buffers
// ---------------------------------------------------------------------------
void save_and_clear_buffers(dpp::cluster& bot) {
    std::map<dpp::snowflake, std::vector<uint8_t>> local_buffers;
    {
        std::lock_guard<std::mutex> lock(bot_mutex);
        if (audio_buffers.empty()) return;
        local_buffers = std::move(audio_buffers);
        audio_buffers.clear(); 
        
        // Reset tracking to prevent massive silence injections between chunks
        last_packet_time.clear();
        packet_counts.clear(); 
    }

    auto now_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char date_str[32];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", std::localtime(&now_c));
    std::string current_date(date_str);

    ensure_directory_exists(BASE_PATH);

    for (auto& [user_id, buffer] : local_buffers) {
        if (buffer.empty()) continue;

        std::cout << "[SAVE] User " << user_id << " — " << buffer.size() << " bytes\n";

        std::string folder_path = BASE_PATH + std::to_string(user_id);
        std::string daily_mp3   = folder_path + "/" + current_date + ".mp3";

        std::string username = "unknown";
        dpp::user* u = dpp::find_user(user_id);
        if (u) username = u->username;

        enqueue_ffmpeg([buf = std::move(buffer),
                        folder_path, daily_mp3, username, user_id]() mutable {

            ensure_directory_exists(folder_path);

            {
                std::ofstream meta(folder_path + "/username.txt", std::ios::trunc);
                meta << username << "\n";
            }

            std::string pcm_path  = folder_path + "/temp_chunk.pcm";
            std::string chunk_mp3 = folder_path + "/temp_chunk.mp3";

            {
                std::ofstream f(pcm_path, std::ios::binary);
                f.write(reinterpret_cast<const char*>(buf.data()), buf.size());
            }

            std::string enc = "ffmpeg -y -f s16le -ar 48000 -ac 2 -i \"" + pcm_path
                              + "\" -b:a 192k \"" + chunk_mp3 + "\" > /dev/null 2>&1";
            int rc = std::system(enc.c_str());
            std::remove(pcm_path.c_str());

            struct stat st;
            if (stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                std::cout << "[FFMPEG_JOB] ERROR — encode failed for user " << user_id << "\n";
                return;
            }

            std::cout << "[FFMPEG_JOB] Encoded " << st.st_size << " bytes for user " << user_id << "\n";
            append_mp3(daily_mp3, chunk_mp3, folder_path);
        });
    }
}

// ---------------------------------------------------------------------------
// Verify bot placement (Thread-Safe)
// ---------------------------------------------------------------------------
void verify_bot_placement(dpp::cluster& bot) {
    std::lock_guard<std::mutex> lock(state_mutex);
    
    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) continue;
        dpp::snowflake gid = c->guild_id;
        if (guild_transitioning[gid]) continue;

        dpp::snowflake bot_channel = safe_voice_states[gid][bot.me.id];
        if (bot_channel == 0) continue; 

        bool in_allowed = ALLOWED_CHANNELS.count(bot_channel) > 0;

        int allowed_present = 0;
        for (const auto& [u_id, c_id] : safe_voice_states[gid]) {
            if (c_id == bot_channel && ALLOWED_USERS.count(u_id)) {
                allowed_present++;
            }
        }

        dpp::discord_client* shard = bot.get_shard(dpp::find_guild(gid)->shard_id);

        if (!in_allowed) {
            std::cout << "[VERIFY] Bot in non-allowed channel — disconnecting\n";
            guild_transitioning[gid] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(gid);
        } else if (allowed_present == 0) {
            std::cout << "[VERIFY] Bot alone in channel — disconnecting\n";
            guild_transitioning[gid] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(gid);
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluate VCs (Thread-Safe)
// ---------------------------------------------------------------------------
void evaluate_vcs(dpp::cluster& bot) {
    std::lock_guard<std::mutex> lock(state_mutex);
    
    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) continue;
        dpp::snowflake gid = c->guild_id;
        if (guild_transitioning[gid]) continue;

        int allowed_present = 0;
        for (const auto& [u_id, c_id] : safe_voice_states[gid]) {
            if (c_id == channel_id && ALLOWED_USERS.count(u_id)) {
                allowed_present++;
            }
        }

        dpp::snowflake bot_c_id = safe_voice_states[gid][bot.me.id];
        bool bot_in_this = (bot_c_id == channel_id);
        bool bot_in_any  = (bot_c_id != 0);

        dpp::discord_client* shard = bot.get_shard(dpp::find_guild(gid)->shard_id);

        if (allowed_present > 0 && !bot_in_this && !bot_in_any) {
            std::cout << "[EVAL] Joining channel " << channel_id << "\n";
            guild_transitioning[gid] = true;
            udp_hole_punched[gid]    = false;
            punch_active[gid] = std::make_shared<std::atomic<bool>>(true);
            if (shard) shard->connect_voice(gid, channel_id);
        }
        else if (allowed_present == 0 && bot_in_this) {
            std::cout << "[EVAL] No allowed users left — disconnecting\n";
            guild_transitioning[gid] = true;
            if (punch_active.count(gid) && punch_active[gid])
                punch_active[gid]->store(false);
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(gid);
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
int main() {
    std::cout.setf(std::ios::unitbuf);

    load_env_file();
    if (DISCORD_TOKEN.empty()) {
        std::cout << "[MAIN] ERROR — Could not read DISCORD_TOKEN from .env!\n";
        return 1;
    }

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
                  << "s | Save path: " << BASE_PATH << "\n";

        bot.start_timer([&bot](dpp::timer) {
            save_and_clear_buffers(bot);
        }, CHUNK_TIME);

        bot.start_timer([&bot](dpp::timer) {
            verify_bot_placement(bot);
            evaluate_vcs(bot);
        }, 10);
    });

    bot.on_voice_state_update([&bot](const dpp::voice_state_update_t& event) {
        dpp::snowflake gid = event.state.guild_id;

        // Safely update our isolated state map
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            if (event.state.channel_id == 0) {
                safe_voice_states[gid].erase(event.state.user_id);
            } else {
                safe_voice_states[gid][event.state.user_id] = event.state.channel_id;
            }
        }

        if (event.state.user_id == bot.me.id) {
            if (guild_transitioning[gid]) {
                std::cout << "[VOICE_STATE] Bot transition confirmed for guild " << gid << "\n";
                guild_transitioning[gid] = false;
            }
            return;
        }
        evaluate_vcs(bot);
    });

    bot.on_voice_ready([](const dpp::voice_ready_t& event) {
        dpp::discord_voice_client* vc       = event.voice_client;
        dpp::snowflake             guild_id = vc->server_id;
        std::cout << "[VOICE_READY] Connected to guild " << guild_id << " — starting UDP punch\n";

        std::shared_ptr<std::atomic<bool>> active;
        if (punch_active.count(guild_id))
            active = punch_active[guild_id];
        else
            active = std::make_shared<std::atomic<bool>>(true);

        std::thread([vc, guild_id, active]() {
            uint16_t silence[5760] = {0};
            int attempts = 0;
            while (active->load() && !udp_hole_punched[guild_id] && attempts < 20) {
                if (vc && vc->is_ready()) vc->send_audio_raw(silence, sizeof(silence));
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                attempts++;
            }

            if (!active->load())
                std::cout << "[UDP_PUNCH] Stopped early for guild " << guild_id << "\n";
            else if (udp_hole_punched[guild_id])
                std::cout << "[UDP_PUNCH] UDP open for guild " << guild_id << " after " << attempts << " attempts\n";
            else
                std::cout << "[UDP_PUNCH] Gave up after 20 attempts for guild " << guild_id << "\n";
        }).detach();
    });

    bot.on_voice_receive([](const dpp::voice_receive_t& event) {
        if (event.user_id == 0) return;
        if (!ALLOWED_USERS.count(event.user_id)) return;

        dpp::snowflake guild_id = event.voice_client->server_id;

        if (!udp_hole_punched[guild_id]) {
            std::cout << "[VOICE_RECV] First packet from guild " << guild_id << " — UDP open\n";
            udp_hole_punched[guild_id] = true;
            if (punch_active.count(guild_id) && punch_active[guild_id])
                punch_active[guild_id]->store(false);
        }

        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(bot_mutex);
        auto& buf = audio_buffers[event.user_id];
        
        // --- PCM Silence Injection ---
        if (last_packet_time.count(event.user_id) > 0) {
            auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_packet_time[event.user_id]).count();
            
            // Discord sends 20ms frames. If gap is > 40ms, inject silence.
            int64_t silence_ms = diff_ms - 20; 
            if (silence_ms > 0) {
                // 48000 Hz * 2 channels * 2 bytes = 192 bytes per ms
                size_t silence_bytes = silence_ms * 192;
                
                // Align to 4-byte boundaries (Stereo 16-bit) to prevent audio tearing
                silence_bytes = (silence_bytes / 4) * 4; 
                buf.insert(buf.end(), silence_bytes, 0);
            }
        }
        last_packet_time[event.user_id] = now;

        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(event.audio_data.data());
        buf.insert(buf.end(), ptr, ptr + event.audio_data.size());

        if (++packet_counts[event.user_id] % 1000 == 0) {
            std::cout << "[VOICE_RECV] User " << event.user_id
                      << " — packets: " << packet_counts[event.user_id]
                      << " — buffer: " << buf.size() << " bytes\n";
        }
    });

    bot.on_voice_client_disconnect([&bot](const dpp::voice_client_disconnect_t& event) {
        dpp::snowflake guild_id = event.voice_client->server_id;
        std::cout << "[VOICE_DISCONNECT] Voice client lost for guild " << guild_id << " — resetting state\n";

        if (punch_active.count(guild_id) && punch_active[guild_id])
            punch_active[guild_id]->store(false);

        udp_hole_punched[guild_id]    = false;
        guild_transitioning[guild_id] = false;

        save_and_clear_buffers(bot);
        std::cout << "[VOICE_DISCONNECT] evaluate_vcs will rejoin if users still present\n";
    });

    std::cout << "[MAIN] Starting bot — chunk time: " << CHUNK_TIME << "s | path: " << BASE_PATH << "\n";
    bot.start(dpp::st_wait);

    ffmpeg_running = false;
    ffmpeg_cv.notify_all();
    worker.join();

    return 0;
}