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
std::mutex                                     bot_mutex;

// ---------------------------------------------------------------------------
// Dedicated FFmpeg worker thread
// All std::system() / file-I/O calls are dispatched here so they NEVER block
// the D++ shard threads that handle audio packet reception.
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
// Reliable MP3 append using ffmpeg -f concat filelist.
// The old "concat:" protocol is for MPEG-TS and silently fails on MP3.
// ---------------------------------------------------------------------------
void append_mp3(const std::string& daily_mp3, const std::string& chunk_mp3,
                const std::string& folder_path) {
    struct stat st;
    if (stat(daily_mp3.c_str(), &st) != 0) {
        std::rename(chunk_mp3.c_str(), daily_mp3.c_str());
        std::cout << "[CREATED] " << daily_mp3 << "\n";
        return;
    }

    std::string filelist = folder_path + "/filelist.txt";
    {
        std::ofstream fl(filelist);
        fl << "file '" << daily_mp3 << "'\n";
        fl << "file '" << chunk_mp3 << "'\n";
    }

    std::string temp_out = folder_path + "/temp_combined.mp3";
    std::string cmd = "ffmpeg -y -f concat -safe 0 -i \"" + filelist
                      + "\" -c copy \"" + temp_out + "\" > /dev/null 2>&1";
    int rc = std::system(cmd.c_str());

    if (rc == 0 && stat(temp_out.c_str(), &st) == 0 && st.st_size > 0) {
        std::rename(temp_out.c_str(), daily_mp3.c_str());
        std::cout << "[APPENDED] " << daily_mp3 << "\n";
    } else {
        std::cout << "[ERROR] ffmpeg concat failed for " << daily_mp3
                  << " — chunk preserved at " << chunk_mp3 << "\n";
        std::remove(filelist.c_str());
        return;
    }

    std::remove(filelist.c_str());
    std::remove(chunk_mp3.c_str());
}

// ---------------------------------------------------------------------------
// Save buffers — fast mutex swap, then dispatch each user to the FFmpeg thread.
//
// FOLDER STRUCTURE (username-change-proof):
//   BASE_PATH/<user_id>/               <- stable forever, never changes
//   BASE_PATH/<user_id>/username.txt   <- always updated to latest known name
//   BASE_PATH/<user_id>/2026-03-09.mp3
// ---------------------------------------------------------------------------
void save_and_clear_buffers(dpp::cluster& bot) {
    std::map<dpp::snowflake, std::vector<uint8_t>> local_buffers;
    {
        std::lock_guard<std::mutex> lock(bot_mutex);
        if (audio_buffers.empty()) return;
        local_buffers = std::move(audio_buffers);
    }

    auto now_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char date_str[32];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", std::localtime(&now_c));
    std::string current_date(date_str);

    ensure_directory_exists(BASE_PATH);

    for (auto& [user_id, buffer] : local_buffers) {
        if (buffer.empty()) continue;

        // Folder is keyed ONLY on the numeric user ID — immune to renames
        std::string folder_path = BASE_PATH + std::to_string(user_id);
        std::string daily_mp3   = folder_path + "/" + current_date + ".mp3";

        // Capture latest username for the metadata file (may have changed)
        std::string username = "unknown";
        dpp::user* u = dpp::find_user(user_id);
        if (u) username = u->username;

        enqueue_ffmpeg([buf      = std::move(buffer),
                        folder_path, daily_mp3, username]() mutable {

            ensure_directory_exists(folder_path);

            // Always overwrite username.txt with the latest known display name.
            // This is how you tell who's who without it affecting folder paths.
            {
                std::ofstream meta(folder_path + "/username.txt",
                                   std::ios::trunc);
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
            std::system(enc.c_str());
            std::remove(pcm_path.c_str());

            struct stat st;
            if (stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                std::cout << "[ERROR] PCM->MP3 encode failed for " << folder_path << "\n";
                return;
            }

            append_mp3(daily_mp3, chunk_mp3, folder_path);
        });
    }
}

// ---------------------------------------------------------------------------
// Self-awareness check — runs every 10s.
// "Am I in a valid channel? Is anyone whitelisted here? If not, leave."
// ---------------------------------------------------------------------------
void verify_bot_placement(dpp::cluster& bot) {
    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) continue;
        dpp::guild* g = dpp::find_guild(c->guild_id);
        if (!g) continue;
        if (guild_transitioning[g->id]) continue;

        auto bot_state = g->voice_members.find(bot.me.id);
        if (bot_state == g->voice_members.end()) continue;

        dpp::snowflake bot_channel = bot_state->second.channel_id;
        if (bot_channel == 0) continue;

        bool in_allowed = ALLOWED_CHANNELS.count(bot_channel) > 0;

        int allowed_present = 0;
        for (const auto& [u_id, state] : g->voice_members) {
            if (state.channel_id == bot_channel && ALLOWED_USERS.count(u_id))
                allowed_present++;
        }

        dpp::discord_client* shard = bot.get_shard(g->shard_id);

        if (!in_allowed) {
            std::cout << "[VERIFY] Bot is in non-allowed channel " << bot_channel
                      << " — disconnecting.\n";
            guild_transitioning[g->id] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
        } else if (allowed_present == 0) {
            std::cout << "[VERIFY] Bot is alone in channel " << bot_channel
                      << " — disconnecting.\n";
            guild_transitioning[g->id] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
        }
    }
}

// ---------------------------------------------------------------------------
// Watch allowed channels — join if a whitelisted user appears
// ---------------------------------------------------------------------------
void evaluate_vcs(dpp::cluster& bot) {
    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) continue;
        dpp::guild* g = dpp::find_guild(c->guild_id);
        if (!g) continue;

        if (guild_transitioning[g->id]) {
            std::cout << "[WATCHER] Guild " << g->id << " is transitioning, skipping.\n";
            continue;
        }

        int allowed_present = 0;
        for (const auto& [u_id, state] : g->voice_members) {
            if (state.channel_id == channel_id && ALLOWED_USERS.count(u_id))
                allowed_present++;
        }

        auto bot_state   = g->voice_members.find(bot.me.id);
        bool bot_in_this = (bot_state != g->voice_members.end()
                            && bot_state->second.channel_id == channel_id);
        bool bot_in_any  = (bot_state != g->voice_members.end()
                            && bot_state->second.channel_id != 0);

        dpp::discord_client* shard = bot.get_shard(g->shard_id);

        if (allowed_present > 0 && !bot_in_this && !bot_in_any) {
            std::cout << "[WATCHER] Allowed user detected. Joining channel "
                      << channel_id << "...\n";
            guild_transitioning[g->id] = true;
            udp_hole_punched[g->id]    = false;
            if (shard) shard->connect_voice(g->id, channel_id);
        }
        else if (allowed_present == 0 && bot_in_this) {
            std::cout << "[WATCHER] No allowed users left. Disconnecting...\n";
            guild_transitioning[g->id] = true;
            save_and_clear_buffers(bot);
            if (shard) shard->disconnect_voice(g->id);
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
        std::cout << "[ERROR] Could not read DISCORD_TOKEN from .env!\n";
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
        std::cout << "Bot online! Chunk time: " << CHUNK_TIME
                  << "s | Save Path: " << BASE_PATH << "\n";

        bot.start_timer([&bot](dpp::timer) {
            save_and_clear_buffers(bot);
        }, CHUNK_TIME);

        bot.start_timer([&bot](dpp::timer) {
            verify_bot_placement(bot);
            evaluate_vcs(bot);
        }, 10);
    });

    bot.on_voice_state_update([&bot](const dpp::voice_state_update_t& event) {
        if (event.state.user_id == bot.me.id) {
            dpp::snowflake gid = event.state.guild_id;
            if (guild_transitioning[gid]) {
                std::cout << "[WATCHER] Bot voice state confirmed. "
                             "Clearing transition lock for guild " << gid << ".\n";
                guild_transitioning[gid] = false;
            }
            return;
        }
        evaluate_vcs(bot);
    });

    bot.on_voice_ready([](const dpp::voice_ready_t& event) {
        std::cout << "Voice READY — keeping UDP route open...\n";
        dpp::discord_voice_client* vc       = event.voice_client;
        dpp::snowflake             guild_id = vc->server_id;

        std::thread([vc, guild_id]() {
            uint16_t silence[5760] = {0};
            int attempts = 0;
            while (!udp_hole_punched[guild_id] && attempts < 20) {
                if (vc && vc->is_ready()) vc->send_audio_raw(silence, sizeof(silence));
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                attempts++;
            }
        }).detach();
    });

    bot.on_voice_receive([](const dpp::voice_receive_t& event) {
        if (event.user_id == 0) return;
        if (!ALLOWED_USERS.count(event.user_id)) return;

        dpp::snowflake guild_id = event.voice_client->server_id;
        if (!udp_hole_punched[guild_id]) udp_hole_punched[guild_id] = true;

        std::lock_guard<std::mutex> lock(bot_mutex);
        auto& buf = audio_buffers[event.user_id];
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(event.audio_data.data());
        buf.insert(buf.end(), ptr, ptr + event.audio_data.size());
    });

    bot.start(dpp::st_wait);

    ffmpeg_running = false;
    ffmpeg_cv.notify_all();
    worker.join();

    return 0;
}