#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>

// Подключаем ТОЛЬКО базовую библиотеку SQLite
#include "sqlite3.h"

namespace fs = std::filesystem;

/// <summary>
/// Выводит сообщение об ошибке SQLite в консоль.
/// </summary>
void LogSqliteError(const std::string& message, sqlite3* dbHandle)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(dbHandle) << std::endl;
}

/// <summary>
/// Получает список имен всех таблиц из открытой базы данных.
/// </summary>
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

/// <summary>
/// Конвертирует одну таблицу из базы данных в CSV файл.
/// </summary>
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

/// <summary>
/// Обрабатывает один DB файл: находит все таблицы и запускает их конвертацию.
/// </summary>
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

int main()
{
    std::cout << "--- DB to CSV Converter ---" << std::endl;

    std::cout << "Enter path to directory with .db files (or '.' for current directory): ";
    std::string inputPathStr;
    std::getline(std::cin, inputPathStr);

    fs::path targetPath = inputPathStr.empty() || inputPathStr == "." ? "." : inputPathStr;

    if (!fs::exists(targetPath) || !fs::is_directory(targetPath))
    {
        std::cerr << "ERROR: Path does not exist or is not a directory: " << inputPathStr << std::endl;
        return 1;
    }

    fs::path outputDir = targetPath / "csv_extracted";
    try
    {
        fs::create_directory(outputDir);
        std::cout << "Created output directory: " << outputDir.string() << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cerr << "ERROR: Could not create output directory. " << e.what() << std::endl;
        return 1;
    }

    for (const auto& entry : fs::directory_iterator(targetPath))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".db")
        {
            ProcessDatabaseFile(entry.path(), outputDir);
        }
    }

    std::cout << "\nConversion complete!" << std::endl;
    return 0;
}

