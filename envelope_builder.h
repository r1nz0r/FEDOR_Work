#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <set>
#include "sqlite3.h"

// =================================================================
//              ВЫБОР РЕЖИМА РАБОТЫ (ГЛАВНАЯ НАСТРОЙКА)
// =================================================================
// - Раскомментируй строку ниже для режима с низкой памятью (на диске).
// - Закомментируй для режима в оперативной памяти (быстрый).
// #define MEMORY_OPTIMIZED
// =================================================================

namespace fs = std::filesystem;

/// <summary>
/// Инкапсулирует логику сборки итоговой огибающей базы данных.
/// </summary>
class EnvelopeBuilder
{
public:
    EnvelopeBuilder();
    void Run();

private:
    // --- Конфигурация ---
    struct Config
    {
        const std::string ELEMENTS_TABLE_NAME = "Elements";
        const std::string ELEMENT_ID_COLUMN = "elemId";
        const std::string OUTPUT_DB_FILENAME = "Envelope.db";
        const std::string TEMP_DB_FILENAME = "__temp_envelope.db";
        const std::string ENVELOPED_TABLE_NAME = "Enveloped Reinforcement"; // Имя с пробелом
    };

    // --- Структуры данных ---
    using ElementProperties = std::unordered_map<std::string, std::string>;
    using VerifiedElementsMap = std::unordered_map<long long, ElementProperties>;
    
    // Универсальная структура для хранения огибающих значений
    // { elemId -> { ColumnName -> MaxValue } }
    using EnvelopedDataMap = std::unordered_map<long long, std::unordered_map<std::string, double>>;

    // --- Приватные поля класса ---
    Config config_;
    VerifiedElementsMap verifiedElements_;
    EnvelopedDataMap envelopedData_; // Используется только в режиме In-Memory

    // --- Основные этапы работы ---
    bool CollectAndVerifyElements(const fs::path& targetPath);
    void AssembleFinalDatabase(const fs::path& targetPath);

    // --- Режимы огибания ---
    void EnvelopeDataInMemory(const fs::path& targetPath);
    void EnvelopeDataOnDisk(const fs::path& targetPath);
    
    // --- Вспомогательные методы ---
    fs::path GetTargetPathFromUser();
    void LogSqliteError(const std::string& message, sqlite3* dbHandle);
    std::vector<std::string> GetTableNames(sqlite3* dbHandle);
    std::set<std::string> CollectAllEnvelopedColumns();
    std::set<std::string> CollectAllEnvelopedColumnsFromTempDb(sqlite3* tempDbHandle);
};

