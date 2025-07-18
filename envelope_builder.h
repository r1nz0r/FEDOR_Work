#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <set>
#include <map>

#include "sqlite3.h"

// =================================================================
//              ВЫБОР РЕЖИМА РАБОТЫ (ГЛАВНАЯ НАСТРОЙКА)
// =================================================================
// - Раскомментируй строку ниже для режима с низкой памятью (на диске).
// - Закомментируй для режима в оперативной памяти (быстрый).
// #define MEMORY_OPTIMIZED
// =================================================================

namespace fs = std::filesystem;

class EnvelopeBuilder
{
public:
    EnvelopeBuilder();
    void Run();

private:
    struct Config
    {
        const std::string ELEMENTS_TABLE_NAME = "Elements";
        const std::string ELEMENT_ID_COLUMN = "elemId";
        const std::string SET_N_COLUMN = "setN";
        const std::string ELEM_TYPE_COLUMN = "elemType";
        const std::string OUTPUT_DB_FILENAME = "Envelope.db";
        const std::string TEMP_DB_FILENAME = "__temp_envelope.db";
        const std::string ENVELOPED_TABLE_NAME = "Enveloped Reinforcement";
    };

    using ElementProperties = std::unordered_map<std::string, std::string>;
    using VerifiedElementsMap = std::unordered_map<long long, ElementProperties>;
    
    // { elemId -> { ColumnName -> MaxValue } }
    using EnvelopedDataMap = std::unordered_map<long long, std::unordered_map<std::string, double>>;

    Config config_;
    VerifiedElementsMap verifiedElements_;
    EnvelopedDataMap envelopedData_;

    // --- Основные этапы ---
    bool CollectAndVerifyElements(const fs::path& targetPath);
    void AssembleFinalDatabase(const fs::path& targetPath);
    void EnvelopeDataInMemory(const fs::path& targetPath);
    void EnvelopeDataOnDisk(const fs::path& targetPath);
    
    // --- Вспомогательные методы ---
    fs::path GetTargetPathFromUser();
    void LogSqliteError(const std::string& message, sqlite3* dbHandle);
    std::vector<std::string> GetTableNames(sqlite3* dbHandle);
    std::set<std::string> CollectAllEnvelopedColumns();
    std::set<std::string> CollectAllEnvelopedColumnsFromTempDb(sqlite3* tempDbHandle);
};

