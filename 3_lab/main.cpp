#include <iostream>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <openssl/sha.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

std::string compute_sha1(const fs::path& path) {
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA_CTX sha;
    SHA1_Init(&sha);

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return "";

    char buffer[8192];
    while (file.read(buffer, sizeof(buffer)))
        SHA1_Update(&sha, buffer, file.gcount());
    // обработать остаток
    if (file.gcount())
        SHA1_Update(&sha, buffer, file.gcount());

    SHA1_Final(hash, &sha);

    char hex[41];
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
        sprintf(hex + i * 2, "%02x", hash[i]);

    return std::string(hex, 40);
}

bool replace_with_link(const fs::path& original, const fs::path& duplicate) {
    fs::remove(duplicate);
    return link(original.c_str(), duplicate.c_str()) == 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <directory>\n";
        return 1;
    }

    fs::path target_dir = argv[1];
    if (!fs::exists(target_dir)) {
        std::cerr << "Directory does not exist\n";
        return 1;
    }

    std::unordered_map<std::string, fs::path> hash_map;
    int duplicates = 0;

    for (auto& entry : fs::recursive_directory_iterator(target_dir)) {
        if (!entry.is_regular_file()) continue;

        fs::path path = entry.path();
        std::string hash = compute_sha1(path);
        if (hash.empty()) continue;

        if (hash_map.find(hash) != hash_map.end()) {
            std::cout << "[Duplicate] " << path << " => " << hash_map[hash] << "\n";
            if (replace_with_link(hash_map[hash], path)) {
                std::cout << "  ✔ Replaced with hard link\n";
                ++duplicates;
            } else {
                std::cerr << "  ✘ Failed to link\n";
            }
        } else {
            hash_map[hash] = path;
        }
    }

    std::cout << "Completed. Replaced " << duplicates << " duplicates with hard links.\n";
    return 0;
}