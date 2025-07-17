#include <iostream>
#include <string>
#include <vector>
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
const std::string TEMP_DB_FILENAME = "__temp_envelope.db";
// --------------------------

// Вспомогательная функция для вывода ошибок SQLite
void log_sqlite_error(const std::string& message, sqlite3* db_handle, char* err_msg = nullptr)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(db_handle) << std::endl;
    if (err_msg)
    {
        sqlite3_free(err_msg);
    }
}

// Функция для обработки одной базы данных и записи результатов во временную БД
void processDatabase(const fs::path& db_path, sqlite3* temp_db_handle)
{
    std::cout << "\nProcessing file: " << db_path.filename().string() << std::endl;
    sqlite3* source_db_handle;

    if (sqlite3_open_v2(db_path.string().c_str(), &source_db_handle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        log_sqlite_error("Could not open source file", source_db_handle);
        sqlite3_close(source_db_handle);
        return;
    }

    // Подготавливаем запрос для вставки/обновления данных во временной БД (UPSERT)
    const char* upsert_sql = R"(
        INSERT INTO IntermediateResults (Element_ID, Reinforcement_Type, Max_Value, Source_DB, Source_Table, Source_SetN)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(Element_ID, Reinforcement_Type) DO UPDATE SET
            Max_Value = excluded.Max_Value,
            Source_DB = excluded.Source_DB,
            Source_Table = excluded.Source_Table,
            Source_SetN = excluded.Source_SetN
        WHERE excluded.Max_Value > Max_Value;
    )";
    sqlite3_stmt* upsert_stmt;
    if (sqlite3_prepare_v2(temp_db_handle, upsert_sql, -1, &upsert_stmt, nullptr) != SQLITE_OK)
    {
        log_sqlite_error("Failed to prepare UPSERT statement", temp_db_handle);
        sqlite3_close(source_db_handle);
        return;
    }

    // Получаем список таблиц из исходной БД
    sqlite3_stmt* table_stmt;
    sqlite3_prepare_v2(source_db_handle, "SELECT name FROM sqlite_master WHERE type='table';", -1, &table_stmt, nullptr);
    std::vector<std::string> table_names;
    while (sqlite3_step(table_stmt) == SQLITE_ROW)
    {
        table_names.push_back(reinterpret_cast<const char*>(sqlite3_column_text(table_stmt, 0)));
    }
    sqlite3_finalize(table_stmt);

    // Обрабатываем каждую таблицу
    for (const auto& table_name : table_names)
    {
        std::cout << "  - Reading table: '" << table_name << "'" << std::endl;

        std::string query = "SELECT * FROM \"" + table_name + "\";";
        sqlite3_stmt* select_stmt;
        if (sqlite3_prepare_v2(source_db_handle, query.c_str(), -1, &select_stmt, nullptr) != SQLITE_OK) continue;

        int col_count = sqlite3_column_count(select_stmt);
        int elemId_idx = -1, setN_idx = -1;
        std::vector<std::pair<std::string, int>> reinf_cols;
        for (int i = 0; i < col_count; ++i)
        {
            std::string col_name = sqlite3_column_name(select_stmt, i);
            if (col_name == ELEMENT_ID_COLUMN) elemId_idx = i;
            else if (col_name == SET_N_COLUMN) setN_idx = i;
            else if (col_name.rfind("As", 0) == 0) reinf_cols.push_back({ col_name, i });
        }

        if (elemId_idx == -1 || setN_idx == -1)
        {
            sqlite3_finalize(select_stmt);
            continue;
        }

        // Читаем строки и сразу пишем во временную БД
        while (sqlite3_step(select_stmt) == SQLITE_ROW)
        {
            long long element_id = sqlite3_column_int64(select_stmt, elemId_idx);
            long long set_n = sqlite3_column_int64(select_stmt, setN_idx);

            for (const auto& reinf_col : reinf_cols)
            {
                sqlite3_bind_int64(upsert_stmt, 1, element_id);
                sqlite3_bind_text(upsert_stmt, 2, reinf_col.first.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(upsert_stmt, 3, sqlite3_column_double(select_stmt, reinf_col.second));
                sqlite3_bind_text(upsert_stmt, 4, db_path.filename().string().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(upsert_stmt, 5, table_name.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(upsert_stmt, 6, set_n);

                sqlite3_step(upsert_stmt);
                sqlite3_reset(upsert_stmt);
            }
        }
        sqlite3_finalize(select_stmt);
    }
    sqlite3_finalize(upsert_stmt);
    sqlite3_close(source_db_handle);
}

// Функция для сохранения итоговых результатов из временной БД
void saveFinalResults(sqlite3* temp_db_handle)
{
    std::cout << "\nWriting final results..." << std::endl;

    std::ofstream csv_file(target_path.string() + "\\" + OUTPUT_CSV_FILENAME);
    csv_file << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    sqlite3* final_db_handle;
    sqlite3_open((target_path.string() + "\\" + OUTPUT_DB_FILENAME).c_str(), &final_db_handle);
    char* err_msg = nullptr;
    sqlite3_exec(final_db_handle, "CREATE TABLE IF NOT EXISTS EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);", 0, 0, &err_msg);
    sqlite3_exec(final_db_handle, "BEGIN TRANSACTION;", 0, 0, &err_msg);
    sqlite3_stmt* insert_stmt;
    sqlite3_prepare_v2(final_db_handle, "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);", -1, &insert_stmt, nullptr);

    sqlite3_stmt* select_stmt;
    sqlite3_prepare_v2(temp_db_handle, "SELECT Element_ID, Reinforcement_Type, Max_Value, Source_DB, Source_Table, Source_SetN FROM IntermediateResults ORDER BY Element_ID, Reinforcement_Type;", -1, &select_stmt, nullptr);

    while (sqlite3_step(select_stmt) == SQLITE_ROW)
    {
        long long element_id = sqlite3_column_int64(select_stmt, 0);
        const char* reinf_type = reinterpret_cast<const char*>(sqlite3_column_text(select_stmt, 1));
        double max_value = sqlite3_column_double(select_stmt, 2);
        const char* source_db = reinterpret_cast<const char*>(sqlite3_column_text(select_stmt, 3));
        const char* source_table = reinterpret_cast<const char*>(sqlite3_column_text(select_stmt, 4));
        long long source_setN = sqlite3_column_int64(select_stmt, 5);

        // Запись в CSV
        csv_file << element_id << ";" << reinf_type << ";" << max_value << ";" << source_db << ";" << source_table << ";" << source_setN << "\n";

        // Запись в итоговую БД
        sqlite3_bind_int64(insert_stmt, 1, element_id);
        sqlite3_bind_text(insert_stmt, 2, reinf_type, -1, SQLITE_STATIC);
        sqlite3_bind_double(insert_stmt, 3, max_value);
        sqlite3_bind_text(insert_stmt, 4, source_db, -1, SQLITE_STATIC);
        sqlite3_bind_text(insert_stmt, 5, source_table, -1, SQLITE_STATIC);
        sqlite3_bind_int64(insert_stmt, 6, source_setN);
        sqlite3_step(insert_stmt);
        sqlite3_reset(insert_stmt);
    }

    sqlite3_finalize(select_stmt);
    sqlite3_finalize(insert_stmt);
    sqlite3_exec(final_db_handle, "COMMIT;", 0, 0, &err_msg);
    if (err_msg) sqlite3_free(err_msg);
    sqlite3_close(final_db_handle);

    std::cout << "OK: Results successfully saved to " << OUTPUT_CSV_FILENAME << " and " << OUTPUT_DB_FILENAME << std::endl;
}

int main()
{
    std::cout << "--- Reinforcement Envelope Analyzer (Memory Optimized) ---" << std::endl;

    std::string input_path_str;
    std::cout << "Enter path to directory with .db files (or '.' for current directory): ";
    std::getline(std::cin, input_path_str);

    if (input_path_str.empty() || input_path_str == ".")
    {
        input_path_str = ".";
    }

    try
    {
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

    // 1. Создаем и настраиваем временную базу данных
    if (fs::exists(target_path.string() + "\\" + TEMP_DB_FILENAME))
    {
        fs::remove(target_path.string() + "\\" + TEMP_DB_FILENAME);
    }
    sqlite3* temp_db_handle;
    if (sqlite3_open((target_path.string() + "\\" + TEMP_DB_FILENAME).c_str(), &temp_db_handle) != SQLITE_OK)
    {
        log_sqlite_error("Could not create temporary database", temp_db_handle);
        return 1;
    }

    const char* create_temp_table_sql = R"(
        CREATE TABLE IntermediateResults (
            Element_ID          INTEGER,
            Reinforcement_Type  TEXT,
            Max_Value           REAL,
            Source_DB           TEXT,
            Source_Table        TEXT,
            Source_SetN         INTEGER,
            PRIMARY KEY (Element_ID, Reinforcement_Type)
        );
    )";
    char* err_msg = nullptr;
    sqlite3_exec(temp_db_handle, create_temp_table_sql, 0, 0, &err_msg);
    sqlite3_exec(temp_db_handle, "BEGIN TRANSACTION;", 0, 0, &err_msg);

    // 2. Обрабатываем все файлы, записывая данные во временную БД
    int file_count = 0;
    for (const auto& entry : fs::directory_iterator(target_path))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".db")
        {
            if (entry.path().filename().string() != OUTPUT_DB_FILENAME)
            {
                processDatabase(entry.path(), temp_db_handle);
                file_count++;
            }
        }
    }

    sqlite3_exec(temp_db_handle, "COMMIT;", 0, 0, &err_msg);

    // 3. Сохраняем итоговые результаты из временной БД
    if (file_count > 0)
    {
        saveFinalResults(temp_db_handle);
    }
    else
    {
        std::cout << "\nNo .db files found to process in the specified directory." << std::endl;
    }

    // 4. Закрываем и удаляем временную БД
    sqlite3_close(temp_db_handle);
    if (fs::exists(target_path.string() + "\\" + TEMP_DB_FILENAME))
    {
        fs::remove(target_path.string() + "\\" + TEMP_DB_FILENAME);
    }

    std::cout << "\nAnalysis complete!" << std::endl;
    return 0;
}
