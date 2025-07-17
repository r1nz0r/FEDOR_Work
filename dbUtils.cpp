#include "DbUtils.h" // Подключаем наш заголовочный файл
#include <iostream>
#include <fstream>

/// <summary>
/// Внутренняя функция для вывода ошибок SQLite в консоль.
/// </summary>
static void LogSqliteError(const std::string& message, sqlite3* dbHandle)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(dbHandle) << std::endl;
}

std::vector<std::string> GetTableNames(sqlite3* dbHandle)
{
    std::vector<std::string> tableNames;
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(dbHandle, "SELECT name FROM sqlite_master WHERE type='table';", -1, &stmt, nullptr) == SQLITE_OK)
    {
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            tableNames.push_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
        }
    }
    sqlite3_finalize(stmt);
    return tableNames;
}

void ConvertTableToCsv(sqlite3* dbHandle, const std::string& tableName, const fs::path& outputFilePath)
{
    std::cout << "  - Converting table '" << tableName << "' to " << outputFilePath.filename().string() << std::endl;

    std::ofstream csvFile(outputFilePath);
    if (!csvFile.is_open())
    {
        std::cerr << "  ERROR: Could not create file " << outputFilePath.string() << std::endl;
        return;
    }

    std::string query = "SELECT * FROM \"" + tableName + "\";";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(dbHandle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Failed to prepare select query", dbHandle);
        return;
    }

    int colCount = sqlite3_column_count(stmt);

    // Записываем заголовки
    for (int i = 0; i < colCount; ++i)
    {
        csvFile << sqlite3_column_name(stmt, i) << (i == colCount - 1 ? "" : ";");
    }
    csvFile << "\n";

    // Записываем строки
    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        for (int i = 0; i < colCount; ++i)
        {
            const char* data = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
            csvFile << (data ? data : "") << (i == colCount - 1 ? "" : ";");
        }
        csvFile << "\n";
    }

    sqlite3_finalize(stmt);
}

void ProcessDatabaseFile(const fs::path& dbPath, const fs::path& outputDir)
{
    std::cout << "\nProcessing file: " << dbPath.filename().string() << std::endl;
    sqlite3* dbHandle;
    if (sqlite3_open_v2(dbPath.string().c_str(), &dbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Could not open file", dbHandle);
        sqlite3_close(dbHandle);
        return;
    }

    std::string dbNameWithoutExt = dbPath.stem().string();
    for (const auto& tableName : GetTableNames(dbHandle))
    {
        fs::path outputFilePath = outputDir / (dbNameWithoutExt + "_" + tableName + ".csv");
        ConvertTableToCsv(dbHandle, tableName, outputFilePath);
    }

    sqlite3_close(dbHandle);
}

