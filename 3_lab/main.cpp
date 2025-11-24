#include <iostream>
#include <filesystem> // Стандарт C++17 для работы с файлами
#include <fstream>
#include <unordered_map> // Хэш-таблица (словарь)
#include <vector>
#include <openssl/sha.h> // Библиотека для SHA1
#include <sys/stat.h>
#include <unistd.h>      // Для системного вызова link()

namespace fs = std::filesystem; // Сокращение для удобства

// Функция вычисления SHA1 хэша файла
std::string compute_sha1(const fs::path& path) {
    unsigned char hash[SHA_DIGEST_LENGTH]; // Буфер для сырого хэша (20 байт)
    SHA_CTX sha;
    SHA1_Init(&sha); // Инициализация контекста

    std::ifstream file(path, std::ios::binary); // Открываем файл в бинарном режиме
    if (!file.is_open()) return "";

    char buffer[8192]; // Читаем кусками по 8 КБ, чтобы не грузить весь файл в ОЗУ
    while (file.read(buffer, sizeof(buffer)))
        SHA1_Update(&sha, buffer, file.gcount()); // Обновляем хэш прочитанным куском
    
    // Если остался "хвостик" файла меньше 8 КБ
    if (file.gcount())
        SHA1_Update(&sha, buffer, file.gcount());

    SHA1_Final(hash, &sha); // Финализируем вычисление

    // Перевод сырых байт в читаемую hex-строку (например, "a94b...")
    char hex[41];
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
        sprintf(hex + i * 2, "%02x", hash[i]);

    return std::string(hex, 40);
}

// Функция замены файла на жесткую ссылку
bool replace_with_link(const fs::path& original, const fs::path& duplicate) {
    // Сначала нужно удалить дубликат, иначе link не сработает (имя занято)
    fs::remove(duplicate); 
    
    // link(existing_path, new_path) — это системный вызов POSIX (из unistd.h)
    // Он создает жесткую ссылку.
    // Возвращает 0 при успехе.
    return link(original.c_str(), duplicate.c_str()) == 0;
    
    // Примечание: в современном C++ можно использовать fs::create_hard_link(original, duplicate)
}

int main(int argc, char* argv[]) {
    // Проверка аргументов командной строки
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <directory>\n";
        return 1;
    }

    fs::path target_dir = argv[1];
    if (!fs::exists(target_dir)) {
        std::cerr << "Directory does not exist\n";
        return 1;
    }

    // Карта (Словарь): Ключ = Хэш файла, Значение = Путь к первому найденному файлу
    std::unordered_map<std::string, fs::path> hash_map;
    int duplicates = 0;

    // Рекурсивный обход директории (заходит во все папки внутри)
    for (auto& entry : fs::recursive_directory_iterator(target_dir)) {
        // Нас интересуют только файлы (не папки, не сокеты)
        if (!entry.is_regular_file()) continue;

        fs::path path = entry.path();
        std::string hash = compute_sha1(path); // Считаем хэш
        if (hash.empty()) continue;

        // Проверяем, видели ли мы уже такой хэш
        if (hash_map.find(hash) != hash_map.end()) {
            // ДА, видели. Значит 'path' — это дубликат того, что в hash_map[hash]
            std::cout << "[Duplicate] " << path << " => " << hash_map[hash] << "\n";
            
            // Заменяем текущий файл ссылкой на первый найденный
            if (replace_with_link(hash_map[hash], path)) {
                std::cout << "  ✔ Replaced with hard link\n";
                ++duplicates;
            } else {
                std::cerr << "  ✘ Failed to link\n"; 
                // Ошибка может быть, если файлы на разных разделах диска
            }
        } else {
            // НЕТ, не видели. Запоминаем этот файл как "оригинал"
            hash_map[hash] = path;
        }
    }

    std::cout << "Completed. Replaced " << duplicates << " duplicates with hard links.\n";
    return 0;
}