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
#include <ctime>
#include <unordered_set>
#include <sys/stat.h> // For mkdir

// --- Configuration Variables ---
std::string DISCORD_TOKEN;
int CHUNK_TIME = 60;
std::unordered_set<dpp::snowflake> ALLOWED_CHANNELS;
std::unordered_set<dpp::snowflake> ALLOWED_USERS;

// --- Global State ---
std::map<dpp::snowflake, std::vector<uint8_t>> audio_buffers;
std::map<dpp::snowflake, bool> udp_hole_punched; 
std::mutex bot_mutex; 

// --- Helper: Parse .env file ---
void load_env_file() {
    std::ifstream file(".env");
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto delimiter_pos = line.find('=');
        if (delimiter_pos == std::string::npos) continue;

        std::string key = line.substr(0, delimiter_pos);
        std::string value = line.substr(delimiter_pos + 1);

        if (key == "DISCORD_TOKEN") DISCORD_TOKEN = value;
        else if (key == "CHUNK_TIME") CHUNK_TIME = std::stoi(value);
        else if (key == "ALLOWED_CHANNELS" || key == "ALLOWED_USERS") {
            std::stringstream ss(value);
            std::string item;
            while (std::getline(ss, item, ',')) {
                if (key == "ALLOWED_CHANNELS") ALLOWED_CHANNELS.insert(std::stoull(item));
                else ALLOWED_USERS.insert(std::stoull(item));
            }
        }
    }
}

// --- Helper: Check Directory Exists ---
void ensure_directory_exists(const std::string& path) {
    std::string cmd = "mkdir -p " + path;
    std::system(cmd.c_str());
}

// --- Core Logic: Save and Append MP3s ---
void save_and_clear_buffers(dpp::cluster& bot) {
    std::lock_guard<std::mutex> lock(bot_mutex);
    if (audio_buffers.empty()) return;

    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    char date_str[32];
    std::strftime(date_str, sizeof(date_str), "%Y-%m-%d", std::localtime(&now_c));
    std::string current_date(date_str);

    for (auto& [user_id, buffer] : audio_buffers) {
        if (buffer.empty()) continue;

        std::string username = "unknown";
        dpp::user* u = dpp::find_user(user_id);
        if (u) username = u->username;

        std::string user_str = std::to_string(user_id);
        std::string folder_path = "recordings/" + username + "_" + user_str;
        ensure_directory_exists(folder_path);

        std::string pcm_chunk = folder_path + "/temp_chunk.pcm";
        std::string mp3_chunk = folder_path + "/temp_chunk.mp3";
        std::string daily_mp3 = folder_path + "/" + current_date + ".mp3";

        // 1. Write raw data to temp PCM
        std::ofstream file(pcm_chunk, std::ios::binary);
        file.write((char*)buffer.data(), buffer.size());
        file.close();

        // 2. Convert temp PCM to temp MP3
        std::string convert_cmd = "ffmpeg -y -f s16le -ar 48000 -ac 2 -i " + pcm_chunk + " -b:a 192k " + mp3_chunk + " > /dev/null 2>&1";
        std::system(convert_cmd.c_str());

        // 3. Check if Daily MP3 exists and append/create
        struct stat buffer_stat;
        if (stat(daily_mp3.c_str(), &buffer_stat) == 0) {
            std::string temp_combined = folder_path + "/temp_combined.mp3";
            std::string concat_cmd = "ffmpeg -y -i \"concat:" + daily_mp3 + "|" + mp3_chunk + "\" -c copy " + temp_combined + " > /dev/null 2>&1 && mv " + temp_combined + " " + daily_mp3;
            std::system(concat_cmd.c_str());
            std::cout << "[APPENDED] Added chunk to " << daily_mp3 << "\n";
        } else {
            std::string move_cmd = "mv " + mp3_chunk + " " + daily_mp3;
            std::system(move_cmd.c_str());
            std::cout << "[CREATED] Started new daily record: " << daily_mp3 << "\n";
        }

        // Cleanup
        std::remove(pcm_chunk.c_str());
        std::remove(mp3_chunk.c_str());
    }

    audio_buffers.clear();
}

// --- Core Logic: Watch Allowed Channels ---
void evaluate_vcs(dpp::cluster& bot) {
    for (const auto& channel_id : ALLOWED_CHANNELS) {
        dpp::channel* c = dpp::find_channel(channel_id);
        if (!c) continue;
        dpp::guild* g = dpp::find_guild(c->guild_id);
        if (!g) continue;

        int allowed_users_present = 0;
        for (const auto& [u_id, state] : g->voice_members) {
            if (state.channel_id == channel_id && ALLOWED_USERS.count(u_id)) {
                allowed_users_present++;
            }
        }

        auto bot_state = g->voice_members.find(bot.me.id);
        bool bot_in_this_vc = (bot_state != g->voice_members.end() && bot_state->second.channel_id == channel_id);

        dpp::discord_client* shard = bot.get_shard(g->shard_id);

        if (allowed_users_present > 0 && !bot_in_this_vc) {
            std::cout << "[WATCHER] Allowed user detected. Joining channel " << channel_id << "...\n";
            udp_hole_punched[g->id] = false;
            // Tell the shard directly to join the specific channel
            if (shard) shard->connect_voice(g->id, channel_id);
        } 
        else if (allowed_users_present == 0 && bot_in_this_vc) {
            std::cout << "[WATCHER] No allowed users left in channel. Disconnecting...\n";
            save_and_clear_buffers(bot);
            // Tell the shard directly to leave the channel
            if (shard) shard->disconnect_voice(g->id);
        }
    }
}

int main() {
    load_env_file();
    if (DISCORD_TOKEN.empty()) {
        std::cout << "[ERROR] Could not read DISCORD_TOKEN from .env!\n";
        return 1;
    }

    dpp::cluster bot(DISCORD_TOKEN, dpp::i_all_intents);

    bot.on_log([](const dpp::log_t& event) {
        if (event.message.find("decrypt failed, frame is not encrypted") != std::string::npos) return; 
        if (event.severity >= dpp::ll_warning) std::cout << "[WARN/ERR] " << event.message << "\n";
    });

    bot.on_ready([&bot](const dpp::ready_t& event) {
        std::cout << "Bot online! Chunk time: " << CHUNK_TIME << "s\n";
        
        bot.start_timer([&bot](dpp::timer timer_handle) {
            save_and_clear_buffers(bot);
        }, CHUNK_TIME);

        bot.start_timer([&bot](dpp::timer timer_handle) {
            evaluate_vcs(bot);
        }, 10);
    });

    bot.on_voice_state_update([&bot](const dpp::voice_state_update_t& event) {
        evaluate_vcs(bot);
    });

    bot.on_voice_ready([](const dpp::voice_ready_t& event) {
        std::cout << "Voice READY — keeping UDP route open...\n";
        dpp::discord_voice_client* vc = event.voice_client;
        dpp::snowflake guild_id = vc->server_id;
        
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
        
        // STRICT PRIVACY: Ignore completely if not an allowed user
        if (ALLOWED_USERS.find(event.user_id) == ALLOWED_USERS.end()) return;

        // Corrected the vc pointer to voice_client
        dpp::snowflake guild_id = event.voice_client->server_id;
        if (!udp_hole_punched[guild_id]) udp_hole_punched[guild_id] = true;
        
        std::lock_guard<std::mutex> lock(bot_mutex);
        
        auto& buffer = audio_buffers[event.user_id];
        const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(event.audio_data.data());
        buffer.insert(buffer.end(), data_ptr, data_ptr + event.audio_data.size());
    });

    bot.start(dpp::st_wait);
    return 0;
}