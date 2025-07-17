#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <algorithm>

// Подключаем ТОЛЬКО базовую библиотеку SQLite
#include "sqlite3.h"

namespace fs = std::filesystem;

// --- ГЛАВНЫЕ НАСТРОЙКИ ---
fs::path target_path = ".";
const std::string ELEMENT_ID_COLUMN = "elemId";
const std::string SET_N_COLUMN = "setN";
const std::string OUTPUT_CSV_FILENAME = "Enveloped_Reinforcement_Analysis.csv";
const std::string OUTPUT_DB_FILENAME = "Enveloped_Reinforcement_Analysis.db";
// --------------------------

// Структура для хранения информации об источнике максимального значения
struct ResultInfo
{
    double value = 0.0;
    std::string source_db;
    std::string source_table;
    long long source_setN = 0;
};

// Основной тип для хранения данных
using MaxResultsMap = std::unordered_map<long long, std::unordered_map<std::string, ResultInfo>>;

// Вспомогательная функция для вывода ошибок SQLite
void log_sqlite_error(const std::string& message, sqlite3* db_handle)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(db_handle) << std::endl;
}

void processDatabase(const fs::path& db_path, MaxResultsMap& results)
{
    std::cout << "\nProcessing file: " << db_path.filename().string() << std::endl;
    sqlite3* db_handle;

    // Используем конструктор пути, который корректно работает с UTF-8 на Windows
    if (sqlite3_open_v2(db_path.string().c_str(), &db_handle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        log_sqlite_error("Could not open file", db_handle);
        sqlite3_close(db_handle);
        return;
    }

    // 1. Получаем список таблиц через C API
    sqlite3_stmt* table_stmt;
    const char* table_query = "SELECT name FROM sqlite_master WHERE type='table';";
    if (sqlite3_prepare_v2(db_handle, table_query, -1, &table_stmt, nullptr) != SQLITE_OK)
    {
        log_sqlite_error("Failed to fetch table list", db_handle);
        sqlite3_close(db_handle);
        return;
    }

    std::vector<std::string> table_names;
    while (sqlite3_step(table_stmt) == SQLITE_ROW)
    {
        table_names.push_back(reinterpret_cast<const char*>(sqlite3_column_text(table_stmt, 0)));
    }
    sqlite3_finalize(table_stmt);

    // 2. Обрабатываем каждую таблицу
    for (const auto& table_name : table_names)
    {
        std::cout << "  - Reading table: '" << table_name << "'" << std::endl;

        std::string query = "SELECT * FROM \"" + table_name + "\";";
        sqlite3_stmt* stmt;

        if (sqlite3_prepare_v2(db_handle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
        {
            log_sqlite_error("Failed to prepare query", db_handle);
            continue;
        }

        int col_count = sqlite3_column_count(stmt);
        int elemId_idx = -1, setN_idx = -1;
        std::vector<std::pair<std::string, int>> reinf_cols;

        for (int i = 0; i < col_count; ++i)
        {
            std::string col_name = sqlite3_column_name(stmt, i);
            if (col_name == ELEMENT_ID_COLUMN) elemId_idx = i;
            else if (col_name == SET_N_COLUMN) setN_idx = i;
            else if (col_name.rfind("As", 0) == 0) reinf_cols.push_back({ col_name, i });
        }

        if (elemId_idx == -1 || setN_idx == -1)
        {
            std::cout << "    WARNING: Skipping table. Missing '" << ELEMENT_ID_COLUMN << "' or '" << SET_N_COLUMN << "' columns." << std::endl;
            sqlite3_finalize(stmt);
            continue;
        }

        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            long long element_id = sqlite3_column_int64(stmt, elemId_idx);
            long long set_n = sqlite3_column_int64(stmt, setN_idx);

            for (const auto& reinf_col : reinf_cols)
            {
                const std::string& col_name = reinf_col.first;
                int col_idx = reinf_col.second;
                double current_value = sqlite3_column_double(stmt, col_idx);

                if (results[element_id].find(col_name) == results[element_id].end() || current_value > results[element_id][col_name].value)
                {
                    results[element_id][col_name] = { current_value, db_path.filename().string(), table_name, set_n };
                }
            }
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db_handle);
}

void saveResults(const MaxResultsMap& results)
{
    std::cout << "\nWriting results..." << std::endl;

    // --- Сохранение в CSV ---
    std::ofstream csv_file(target_path.string() + "\\" + OUTPUT_CSV_FILENAME);
    csv_file << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    // --- Сохранение в DB через C API ---
    sqlite3* db_handle;
    if (sqlite3_open((target_path.string() + "\\" + OUTPUT_DB_FILENAME).c_str(), &db_handle) != SQLITE_OK)
    {
        log_sqlite_error("Could not create output database", db_handle);
        sqlite3_close(db_handle);
        return;
    }

    char* err_msg = nullptr;
    const char* create_table_sql = "CREATE TABLE IF NOT EXISTS EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);";
    sqlite3_exec(db_handle, create_table_sql, 0, 0, &err_msg);
    sqlite3_exec(db_handle, "BEGIN TRANSACTION;", 0, 0, &err_msg);

    const char* insert_sql = "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);";
    sqlite3_stmt* insert_stmt;
    sqlite3_prepare_v2(db_handle, insert_sql, -1, &insert_stmt, nullptr);

    std::vector<long long> sorted_keys;
    for (const auto& pair : results) sorted_keys.push_back(pair.first);
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (const auto& element_id : sorted_keys)
    {
        const auto& reinf_map = results.at(element_id);
        for (const auto& pair : reinf_map)
        {
            const std::string& reinf_type = pair.first;
            const ResultInfo& info = pair.second;

            csv_file << element_id << ";" << reinf_type << ";" << info.value << ";" << info.source_db << ";" << info.source_table << ";" << info.source_setN << "\n";

            sqlite3_bind_int64(insert_stmt, 1, element_id);
            sqlite3_bind_text(insert_stmt, 2, reinf_type.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(insert_stmt, 3, info.value);
            sqlite3_bind_text(insert_stmt, 4, info.source_db.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(insert_stmt, 5, info.source_table.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_int64(insert_stmt, 6, info.source_setN);

            sqlite3_step(insert_stmt);
            sqlite3_clear_bindings(insert_stmt);
            sqlite3_reset(insert_stmt);
        }
    }

    sqlite3_finalize(insert_stmt);
    sqlite3_exec(db_handle, "COMMIT;", 0, 0, &err_msg);
    if (err_msg) sqlite3_free(err_msg);
    sqlite3_close(db_handle);

    std::cout << "OK: Results successfully saved to " << OUTPUT_CSV_FILENAME << " and " << OUTPUT_DB_FILENAME << std::endl;
}

int main()
{
#ifdef _WIN32
    // Эта команда настраивает консоль на работу с UTF-8,
    // что необходимо для корректного отображения и ввода русских символов в пути.
    std::system("chcp 65001 > nul");
#endif

    std::cout << "--- Reinforcement Envelope Analyzer (Pure C API) ---" << std::endl;

    std::string input_path_str;
    std::cout << "Enter path to directory with .db files (or '.' for current directory): ";
    std::getline(std::cin, input_path_str);

    if (input_path_str.empty() || input_path_str == ".")
    {
        input_path_str = ".";
    }

    
    try
    {
        // std::filesystem::path хорошо работает с UTF-8 строками,
        // которые мы получаем из консоли после chcp 65001.
        target_path = input_path_str;
        if (!fs::exists(target_path) || !fs::is_directory(target_path))
        {
            std::cerr << "ERROR: Path does not exist or is not a directory: " << input_path_str << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "ERROR: Invalid path. " << e.what() << std::endl;
        return 1;
    }

    MaxResultsMap all_max_results;

    for (const auto& entry : fs::directory_iterator(target_path))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".db")
        {
            if (entry.path().filename().string() != OUTPUT_DB_FILENAME)
            {
                processDatabase(entry.path(), all_max_results);
            }
        }
    }

    if (all_max_results.empty())
    {
        std::cout << "\nERROR: No data was collected. Check .db files in the specified directory." << std::endl;
    }
    else
    {
        saveResults(all_max_results);
    }

    std::cout << "\nAnalysis complete!" << std::endl;
    return 0;
}
