#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <algorithm> // Для std::sort

// Подключаем и C++ обертку (для простых операций), и базовую библиотеку C
#include "sqlite_modern_cpp.h"
#include "sqlite3.h" // Важно: используем базовый API для сложных запросов

namespace fs = std::filesystem;

// --- ГЛАВНЫЕ НАСТРОЙКИ ---
const std::string ELEMENT_ID_COLUMN = "elemId";
const std::string SET_N_COLUMN = "setN";
const std::string OUTPUT_CSV_FILENAME = "Enveloped_Reinforcement_Analysis.csv";
const std::string OUTPUT_DB_FILENAME = "Enveloped_Reinforcement_Analysis.db";
// --------------------------

struct ResultInfo {
    double value = 0.0;
    std::string source_db;
    std::string source_table;
    long long source_setN = 0;
};

using MaxResultsMap = std::unordered_map<long long, std::unordered_map<std::string, ResultInfo>>;

void processDatabase(const fs::path& db_path, MaxResultsMap& results) {
    std::cout << "\n🔄 Обрабатывается файл: " << db_path.filename().string() << std::endl;
    sqlite3* db_handle; // Используем базовый C-указатель на базу данных

    if (sqlite3_open(db_path.string().c_str(), &db_handle) != SQLITE_OK) {
        std::cerr << "  ❌ Ошибка открытия файла: " << sqlite3_errmsg(db_handle) << std::endl;
        sqlite3_close(db_handle);
        return;
    }

    // Получаем список таблиц, используя C++ обертку для простоты
    std::vector<std::string> table_names;
    try {
        sqlite::database db_wrapper(db_path.string());
        db_wrapper << "SELECT name FROM sqlite_master WHERE type='table';"
                   >> [&](std::string name) {
                       table_names.push_back(name);
                   };
    } catch (const std::exception& e) {
        std::cerr << "  ❌ Ошибка получения списка таблиц: " << e.what() << std::endl;
        sqlite3_close(db_handle);
        return;
    }

    for (const auto& table_name : table_names) {
        std::cout << "  - Чтение таблицы: '" << table_name << "'" << std::endl;
        
        std::string query = "SELECT * FROM \"" + table_name + "\";";
        sqlite3_stmt* stmt; // Указатель на подготовленный запрос

        if (sqlite3_prepare_v2(db_handle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "    ❌ Ошибка подготовки запроса: " << sqlite3_errmsg(db_handle) << std::endl;
            continue;
        }

        // Определяем индексы нужных столбцов один раз
        int col_count = sqlite3_column_count(stmt);
        int elemId_idx = -1, setN_idx = -1;
        std::vector<std::pair<std::string, int>> reinf_cols;

        for (int i = 0; i < col_count; ++i) {
            std::string col_name = sqlite3_column_name(stmt, i);
            if (col_name == ELEMENT_ID_COLUMN) elemId_idx = i;
            else if (col_name == SET_N_COLUMN) setN_idx = i;
            else if (col_name.rfind("As", 0) == 0) reinf_cols.push_back({col_name, i});
        }

        if (elemId_idx == -1 || setN_idx == -1) {
            std::cout << "    ⚠️ Пропуск: в таблице нет столбцов '" << ELEMENT_ID_COLUMN << "' или '" << SET_N_COLUMN << "'." << std::endl;
            sqlite3_finalize(stmt);
            continue;
        }

        // Проходим по всем строкам результата
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            long long element_id = sqlite3_column_int64(stmt, elemId_idx);
            long long set_n = sqlite3_column_int64(stmt, setN_idx);

            // Обновляем максимумы
            for (const auto& reinf_col : reinf_cols) {
                const std::string& col_name = reinf_col.first;
                int col_idx = reinf_col.second;
                double current_value = sqlite3_column_double(stmt, col_idx);

                if (results[element_id].find(col_name) == results[element_id].end() || current_value > results[element_id][col_name].value) {
                    results[element_id][col_name] = {current_value, db_path.filename().string(), table_name, set_n};
                }
            }
        }
        sqlite3_finalize(stmt); // Освобождаем ресурсы запроса
    }
    sqlite3_close(db_handle); // Закрываем базу данных
}

void saveResults(const MaxResultsMap& results) {
    std::cout << "\n✍️ Запись результатов..." << std::endl;
    
    std::ofstream csv_file(OUTPUT_CSV_FILENAME);
    csv_file << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    // Используем C++ обертку для удобной записи
    sqlite::database db(OUTPUT_DB_FILENAME);
    db << "CREATE TABLE IF NOT EXISTS EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);";
    
    // ИСПРАВЛЕНИЕ: Ручное управление транзакцией
    db << "BEGIN TRANSACTION;";

    std::vector<long long> sorted_keys;
    for(const auto& pair : results) sorted_keys.push_back(pair.first);
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (const auto& element_id : sorted_keys) {
        const auto& reinf_map = results.at(element_id);
        for (const auto& pair : reinf_map) {
            const std::string& reinf_type = pair.first;
            const ResultInfo& info = pair.second;

            csv_file << element_id << ";" << reinf_type << ";" << info.value << ";" << info.source_db << ";" << info.source_table << ";" << info.source_setN << "\n";
            
            db << "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);"
               << element_id << reinf_type << info.value << info.source_db << info.source_table << info.source_setN;
        }
    }

    db << "COMMIT;"; // Завершаем транзакцию
    csv_file.close();

    std::cout << "✅ Результаты успешно сохранены в " << OUTPUT_CSV_FILENAME << " и " << OUTPUT_DB_FILENAME << std::endl;
}

int main() {
    #ifdef _WIN32
        std::system("chcp 65001 > nul");
    #endif
    
    std::cout << "--- Запуск анализатора огибающей армирования (C++ версия) ---" << std::endl;
    
    MaxResultsMap all_max_results;
    const fs::path current_path(".");

    for (const auto& entry : fs::directory_iterator(current_path)) {
        if (entry.is_regular_file() && entry.path().extension() == ".db") {
            if (entry.path().filename().string() != OUTPUT_DB_FILENAME) {
                processDatabase(entry.path(), all_max_results);
            }
        }
    }

    if (all_max_results.empty()) {
        std::cout << "\n❌ Не удалось собрать данные. Проверьте .db файлы в папке." << std::endl;
    } else {
        saveResults(all_max_results);
    }

    std::cout << "\n🎉 Анализ завершен!" << std::endl;
    return 0;
}

