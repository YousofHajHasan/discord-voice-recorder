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
#include <algorithm>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
std::string DISCORD_TOKEN;
std::string BASE_PATH       = "recordings/";
int         CHUNK_TIME      = 300;
const size_t MAX_BUFFER_SIZE = 300 * 1024 * 1024; // 300 MB

// Audio settings (16kHz mono is the standard for AI/ASR pipelines)
const int    RECORD_SAMPLE_RATE = 16000;
const int    RECORD_CHANNELS    = 1;
// Discord always delivers 48kHz stereo s16le — each packet is 960 frames
const int    DISCORD_SAMPLE_RATE = 48000;
const int    DISCORD_CHANNELS    = 2;
const int    DISCORD_FRAME_SIZE  = 960; // samples per channel per packet
// Bytes per Discord packet: 960 frames * 2 ch * 2 bytes = 3840
const int    DISCORD_PACKET_BYTES = DISCORD_FRAME_SIZE * DISCORD_CHANNELS * 2;

// Silence injection: inject exactly this many seconds of silence when a gap
// of >= GAP_THRESHOLD seconds is detected between voice packets.
// Capped to avoid runaway buffer growth during long pauses.
const double GAP_THRESHOLD     = 1.50; // seconds — gap that triggers injection
const double SILENCE_INJECT_S  = 0.50; // seconds of silence to inject (fixed)

std::unordered_set<dpp::snowflake> ALLOWED_CHANNELS;
std::unordered_set<dpp::snowflake> ALLOWED_USERS;

// ---------------------------------------------------------------------------
// Global State
// ---------------------------------------------------------------------------
std::map<dpp::snowflake, std::vector<uint8_t>> audio_buffers;
std::map<dpp::snowflake, bool>                 udp_hole_punched;
std::map<dpp::snowflake, bool>                 guild_transitioning;
std::map<dpp::snowflake, std::shared_ptr<std::atomic<bool>>> punch_active;

std::map<dpp::snowflake, std::chrono::time_point<std::chrono::steady_clock>> last_packet_time;
std::map<dpp::snowflake, bool>  silence_injected; // track if silence already injected for this gap
std::map<dpp::snowflake, int>   packet_counts;
std::mutex bot_mutex;

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
// Generate silence bytes for the RECORD format (16kHz mono s16le)
// Fixed duration — never proportional to gap length to prevent buffer bloat.
// ---------------------------------------------------------------------------
std::vector<uint8_t> make_silence_bytes(double seconds = SILENCE_INJECT_S) {
    size_t num_samples = static_cast<size_t>(seconds * RECORD_SAMPLE_RATE * RECORD_CHANNELS);
    return std::vector<uint8_t>(num_samples * 2, 0); // 2 bytes per s16le sample
}

// ---------------------------------------------------------------------------
// Downsample Discord 48kHz stereo → 16kHz mono (s16le)
//
// Simple approach: average stereo channels first, then pick every 3rd sample
// (48000 / 16000 = 3). This is a basic decimation — good enough for voice.
// For production you'd use a proper polyphase FIR, but this avoids dependencies.
// ---------------------------------------------------------------------------
std::vector<uint8_t> downsample_discord_packet(const uint8_t* data, size_t size) {
    // Input: interleaved s16le stereo at 48kHz
    // Output: mono s16le at 16kHz
    const int16_t* src    = reinterpret_cast<const int16_t*>(data);
    size_t         frames = size / (DISCORD_CHANNELS * sizeof(int16_t)); // stereo frames
    const int      ratio  = DISCORD_SAMPLE_RATE / RECORD_SAMPLE_RATE;   // = 3

    std::vector<uint8_t> out;
    out.reserve((frames / ratio) * sizeof(int16_t));

    for (size_t i = 0; i < frames; i += ratio) {
        // Average left and right channels for this frame
        int32_t mono = (static_cast<int32_t>(src[i * DISCORD_CHANNELS])
                      + static_cast<int32_t>(src[i * DISCORD_CHANNELS + 1])) / 2;
        int16_t sample = static_cast<int16_t>(mono);
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&sample);
        out.push_back(bytes[0]);
        out.push_back(bytes[1]);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Reliable MP3 append — binary join of two MP3 files
// ---------------------------------------------------------------------------
void append_mp3(const std::string& daily_mp3, const std::string& chunk_mp3,
                const std::string& folder_path) {
    struct stat st;
    if (stat(daily_mp3.c_str(), &st) != 0) {
        std::rename(chunk_mp3.c_str(), daily_mp3.c_str());
        std::cout << "[APPEND_MP3] Created new daily file: " << daily_mp3 << "\n";
        return;
    }

    std::ifstream src(chunk_mp3, std::ios::binary);
    std::ofstream dst(daily_mp3, std::ios::binary | std::ios::app);

    if (src && dst) {
        dst << src.rdbuf();
        std::cout << "[APPEND_MP3] Appended to daily file: " << daily_mp3 << "\n";
    } else {
        std::cout << "[APPEND_MP3] ERROR opening files for binary append\n";
    }

    src.close();
    dst.close();
    std::remove(chunk_mp3.c_str());
}

// ---------------------------------------------------------------------------
// Build ffmpeg encode command
// Input:  raw s16le PCM at RECORD_SAMPLE_RATE / RECORD_CHANNELS
// Output: 128k MP3 (voice quality — 192k is overkill for 16kHz mono)
// -af loudnorm: normalizes loudness to -16 LUFS, true peak ceiling -1.5 dBFS
//              This prevents inter-chunk clipping when files are appended.
// ---------------------------------------------------------------------------
std::string build_ffmpeg_cmd(const std::string& pcm_path, const std::string& mp3_path) {
    return "ffmpeg -y"
           " -f s16le"
           " -ar " + std::to_string(RECORD_SAMPLE_RATE) +
           " -ac " + std::to_string(RECORD_CHANNELS) +
           " -i \"" + pcm_path + "\""
           " -af loudnorm=I=-16:TP=-1.5:LRA=11"
           " -b:a 128k"
           " \"" + mp3_path + "\""
           " > /dev/null 2>&1";
}

// ---------------------------------------------------------------------------
// Save and clear all buffers (called by timer or on overflow)
// ---------------------------------------------------------------------------
void save_and_clear_buffers(dpp::cluster& bot) {
    std::map<dpp::snowflake, std::vector<uint8_t>> local_buffers;
    {
        std::lock_guard<std::mutex> lock(bot_mutex);
        if (audio_buffers.empty()) return;
        local_buffers = std::move(audio_buffers);
        audio_buffers.clear();

        // Reset gap tracking so the next chunk doesn't inject silence
        // at the start based on the old last_packet_time
        last_packet_time.clear();
        silence_injected.clear();
        packet_counts.clear();
    }

    auto   now_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char   date_str[32];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", std::localtime(&now_c));
    std::string current_date(date_str);

    ensure_directory_exists(BASE_PATH);

    for (auto& [user_id, buffer] : local_buffers) {
        if (buffer.empty()) continue;

        std::cout << "[SAVE] User " << user_id
                  << " — " << buffer.size() << " bytes ("
                  << (buffer.size() / 1024 / 1024) << " MB)\n";

        std::string folder_path = BASE_PATH + std::to_string(user_id);
        std::string daily_mp3   = folder_path + "/" + current_date + ".mp3";

        std::string username = "unknown";
        dpp::user*  u        = dpp::find_user(user_id);
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

            std::string enc = build_ffmpeg_cmd(pcm_path, chunk_mp3);
            int rc = std::system(enc.c_str());

            struct stat st;
            if (rc != 0 || stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                std::cout << "[FFMPEG_JOB] Encode failed for user " << user_id << ", retrying...\n";
                rc = std::system(enc.c_str());
                if (rc != 0 || stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                    std::cout << "[FFMPEG_JOB] Retry failed. Saving raw PCM as recovery.\n";
                    std::string recovery = folder_path + "/recovery_"
                                         + std::to_string(std::time(nullptr)) + ".pcm";
                    std::rename(pcm_path.c_str(), recovery.c_str());
                    return;
                }
            }
            std::remove(pcm_path.c_str());

            std::cout << "[FFMPEG_JOB] Encoded " << st.st_size
                      << " bytes for user " << user_id << "\n";
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
            if (c_id == bot_channel && ALLOWED_USERS.count(u_id))
                allowed_present++;
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
            if (c_id == channel_id && ALLOWED_USERS.count(u_id))
                allowed_present++;
        }

        dpp::snowflake bot_c_id  = safe_voice_states[gid][bot.me.id];
        bool           bot_here  = (bot_c_id == channel_id);
        bool           bot_any   = (bot_c_id != 0);

        dpp::discord_client* shard = bot.get_shard(dpp::find_guild(gid)->shard_id);

        if (allowed_present > 0 && !bot_here && !bot_any) {
            std::cout << "[EVAL] Joining channel " << channel_id << "\n";
            guild_transitioning[gid] = true;
            udp_hole_punched[gid]    = false;
            punch_active[gid] = std::make_shared<std::atomic<bool>>(true);
            if (shard) shard->connect_voice(gid, channel_id);
        } else if (allowed_present == 0 && bot_here) {
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
// Shared FFmpeg encode + append logic (used by both timer and overflow paths)
// ---------------------------------------------------------------------------
void enqueue_encode_and_append(std::vector<uint8_t> buf,
                                const std::string& folder_path,
                                const std::string& daily_mp3,
                                const std::string& username,
                                uint64_t           user_id) {
    enqueue_ffmpeg([buf = std::move(buf), folder_path, daily_mp3, username, user_id]() mutable {
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

        std::string enc = build_ffmpeg_cmd(pcm_path, chunk_mp3);
        int rc = std::system(enc.c_str());

        struct stat st;
        if (rc != 0 || stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
            std::cout << "[FFMPEG_JOB] Encode failed for user " << user_id << ", retrying...\n";
            rc = std::system(enc.c_str());
            if (rc != 0 || stat(chunk_mp3.c_str(), &st) != 0 || st.st_size == 0) {
                std::cout << "[FFMPEG_JOB] Retry failed. Saving raw PCM as recovery.\n";
                std::string recovery = folder_path + "/recovery_"
                                      + std::to_string(std::time(nullptr)) + ".pcm";
                std::rename(pcm_path.c_str(), recovery.c_str());
                return;
            }
        }
        std::remove(pcm_path.c_str());

        std::cout << "[FFMPEG_JOB] Encoded " << st.st_size
                  << " bytes for user " << user_id << "\n";
        append_mp3(daily_mp3, chunk_mp3, folder_path);
    });
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

    std::cout << "[MAIN] Audio format: "
              << RECORD_SAMPLE_RATE << "Hz, "
              << RECORD_CHANNELS << "ch mono | "
              << "Silence injection: " << SILENCE_INJECT_S << "s on gaps >= "
              << GAP_THRESHOLD << "s\n";

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
        {
            std::lock_guard<std::mutex> lock(state_mutex);
            if (event.state.channel_id == 0)
                safe_voice_states[gid].erase(event.state.user_id);
            else
                safe_voice_states[gid][event.state.user_id] = event.state.channel_id;
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
                std::cout << "[UDP_PUNCH] UDP open for guild " << guild_id
                          << " after " << attempts << " attempts\n";
            else
                std::cout << "[UDP_PUNCH] Gave up after 20 attempts for guild " << guild_id << "\n";
        }).detach();
    });

    bot.on_voice_receive([&bot](const dpp::voice_receive_t& event) {
        if (event.user_id == 0) return;
        if (!ALLOWED_USERS.count(event.user_id)) return;

        // Drop pure-silence packets (Discord sends these as keep-alives)
        bool is_silence = std::all_of(
            event.audio_data.begin(), event.audio_data.end(),
            [](uint8_t b) { return b == 0; });
        if (is_silence) return;

        dpp::snowflake guild_id = event.voice_client->server_id;

        if (!udp_hole_punched[guild_id]) {
            std::cout << "[VOICE_RECV] First packet from guild "
                      << guild_id << " — UDP open\n";
            udp_hole_punched[guild_id] = true;
            if (punch_active.count(guild_id) && punch_active[guild_id])
                punch_active[guild_id]->store(false);
        }

        // Downsample from Discord format (48kHz stereo) → record format (16kHz mono)
        std::vector<uint8_t> pcm = downsample_discord_packet(
            reinterpret_cast<const uint8_t*>(event.audio_data.data()),
            event.audio_data.size());

        std::vector<uint8_t> overflow_buf;
        {
            std::lock_guard<std::mutex> lock(bot_mutex);
            auto& buf = audio_buffers[event.user_id];
            auto  now = std::chrono::steady_clock::now();

            // ── Silence injection ──────────────────────────────────────────
            // If we've seen this user before and there's been a gap of >= 0.5s
            // since their last packet, inject exactly SILENCE_INJECT_S of silence
            // once. This prevents hard amplitude splices between speech bursts
            // without causing runaway buffer growth during long pauses.
            if (last_packet_time.count(event.user_id)) {
                double gap = std::chrono::duration<double>(
                    now - last_packet_time[event.user_id]).count();

                if (gap >= GAP_THRESHOLD && !silence_injected[event.user_id]) {
                    auto sil = make_silence_bytes(SILENCE_INJECT_S);
                    buf.insert(buf.end(), sil.begin(), sil.end());
                    silence_injected[event.user_id] = true;
                }
            }

            // Reset silence flag — user is speaking again
            silence_injected[event.user_id] = false;
            last_packet_time[event.user_id] = now;

            // Append downsampled PCM
            buf.insert(buf.end(), pcm.begin(), pcm.end());

            if (++packet_counts[event.user_id] % 1000 == 0) {
                std::cout << "[VOICE_RECV] User " << event.user_id
                          << " — packets: " << packet_counts[event.user_id]
                          << " — buffer: " << buf.size() << " bytes"
                          << " (" << (buf.size() / 1024 / 1024) << " MB)\n";
            }

            // Overflow guard — force save if buffer grows too large
            if (buf.size() >= MAX_BUFFER_SIZE) {
                std::cout << "[VOICE_RECV] User " << event.user_id
                          << " exceeded max buffer (" << buf.size()
                          << " bytes). Forcing save.\n";
                overflow_buf = std::move(buf);
                audio_buffers[event.user_id].clear();
                packet_counts[event.user_id]  = 0;
                silence_injected[event.user_id] = false;
                last_packet_time.erase(event.user_id);
            }
        }

        // Process forced overflow save outside the lock
        if (!overflow_buf.empty()) {
            auto   now_c = std::chrono::system_clock::to_time_t(
                               std::chrono::system_clock::now());
            char   date_str[32];
            std::strftime(date_str, sizeof(date_str), "%Y-%m-%d",
                          std::localtime(&now_c));

            std::string folder_path = BASE_PATH + std::to_string(event.user_id);
            std::string daily_mp3   = folder_path + "/" + std::string(date_str) + ".mp3";
            dpp::user*  u           = dpp::find_user(event.user_id);
            std::string username    = u ? u->username : "unknown";

            enqueue_encode_and_append(std::move(overflow_buf), folder_path,
                                      daily_mp3, username, event.user_id);
        }
    });

    bot.on_voice_client_disconnect([&bot](const dpp::voice_client_disconnect_t& event) {
        dpp::snowflake guild_id = event.voice_client->server_id;
        std::cout << "[VOICE_DISCONNECT] Voice client lost for guild "
                  << guild_id << " — resetting state\n";

        if (punch_active.count(guild_id) && punch_active[guild_id])
            punch_active[guild_id]->store(false);

        udp_hole_punched[guild_id]    = false;
        guild_transitioning[guild_id] = false;

        save_and_clear_buffers(bot);
        std::cout << "[VOICE_DISCONNECT] evaluate_vcs will rejoin if users still present\n";
    });

    std::cout << "[MAIN] Starting bot — chunk time: " << CHUNK_TIME
              << "s | path: " << BASE_PATH << "\n";
    bot.start(dpp::st_wait);

    ffmpeg_running = false;
    ffmpeg_cv.notify_all();
    worker.join();

    return 0;
}