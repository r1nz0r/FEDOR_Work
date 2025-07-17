#pragma once

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
    };

    // --- Структуры данных ---
    using ElementProperties = std::unordered_map<std::string, std::string>;
    using VerifiedElementsMap = std::unordered_map<long long, ElementProperties>;
    
    struct ReinforcementResult
    {
        double value = 0.0;
    };
    using ReinforcementMap = std::unordered_map<long long, std::unordered_map<std::string, ReinforcementResult>>;

    // --- Приватные поля класса ---
    Config config_;
    VerifiedElementsMap verifiedElements_; // Данные об элементах всегда хранятся в памяти для верификации
    ReinforcementMap envelopedReinforcement_; // Используется только в режиме In-Memory

    // --- Основные этапы работы ---
    bool CollectAndVerifyElements(const fs::path& targetPath);
    void AssembleFinalDatabase(const fs::path& targetPath);

    // --- Режимы огибания ---
    void EnvelopeReinforcementInMemory(const fs::path& targetPath);
    void EnvelopeReinforcementOnDisk(const fs::path& targetPath);

    // --- Вспомогательные методы ---
    fs::path GetTargetPathFromUser();
    void LogSqliteError(const std::string& message, sqlite3* dbHandle);
    std::vector<std::string> GetTableNames(sqlite3* dbHandle);
};

