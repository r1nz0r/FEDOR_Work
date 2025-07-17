#pragma once // Стандартная защита от двойного включения файла

#include <string>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include "sqlite3.h"

// =================================================================
//              ВЫБОР РЕЖИМА РАБОТЫ (ГЛАВНАЯ НАСТРОЙКА)
// =================================================================
// - Раскомментируй строку ниже для режима с низкой памятью (на диске).
// - Закомментируй для режима в оперативной памяти (быстрый).
#define MEMORY_OPTIMIZED
// =================================================================

namespace fs = std::filesystem;

/// <summary>
/// Инкапсулирует всю логику анализа огибающей армирования.
/// </summary>
class EnvelopeAnalyzer
{
public:
    // Конструктор класса
    EnvelopeAnalyzer();

    // Запускает полный цикл работы анализатора.
    void Run();

private:
    // --- Конфигурация ---
    struct Config
    {
        const std::string ELEMENT_ID_COLUMN = "elemId";
        const std::string SET_N_COLUMN = "setN";
        const std::string OUTPUT_CSV_FILENAME = "Enveloped_Reinforcement_Analysis.csv";
        const std::string OUTPUT_DB_FILENAME = "Enveloped_Reinforcement_Analysis.db";
        const std::string TEMP_DB_FILENAME = "__temp_envelope.db";
    };

    // --- Структуры данных ---
    struct ResultInfo
    {
        double value = 0.0;
        std::string source_db;
        std::string source_table;
        long long source_setN = 0;
    };
    using MaxResultsMap = std::unordered_map<long long, std::unordered_map<std::string, ResultInfo>>;

    // --- Приватные поля класса ---
    Config config_;
    MaxResultsMap allMaxResults_; // Используется только в режиме In-Memory

    // --- Основные методы ---
    void RunInMemory(const fs::path& targetPath);
    void RunOnDisk(const fs::path& targetPath);
    fs::path GetTargetPathFromUser();

    // --- Общие вспомогательные методы ---
    void LogSqliteError(const std::string& message, sqlite3* dbHandle);
    std::vector<std::string> GetTableNames(sqlite3* dbHandle);
    bool IsProcessableDbFile(const fs::directory_entry& entry);

    // --- Методы для режима In-Memory ---
    void ProcessDatabaseInMemory(const fs::path& dbPath);
    void ProcessTableInMemory(const std::string& tableName, sqlite3* dbHandle, const std::string& sourceDbName);
    void SaveResultsInMemory(const fs::path& targetPath);

    // --- Методы для режима On-Disk ---
    sqlite3* InitializeTempDatabase(const fs::path& tempDbPath);
    void ProcessDatabaseOnDisk(const fs::path& dbPath, sqlite3* tempDbHandle);
    void ProcessTableOnDisk(const std::string& tableName, sqlite3* sourceDbHandle, sqlite3_stmt* upsertStmt, const std::string& sourceDbName);
    void SaveFinalResultsOnDisk(sqlite3* tempDbHandle, const fs::path& targetPath);
};

