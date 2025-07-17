#include "CsvToDbUtils.h"
#include "sqlite3.h"
#include <iostream>
#include <fstream>
#include <sstream>

/// <summary>
/// Внутренняя функция для вывода ошибок SQLite в консоль.
/// </summary>
static void LogSqliteError(const std::string& message, sqlite3* dbHandle)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(dbHandle) << std::endl;
}

/// <summary>
/// Разделяет строку на части по заданному разделителю.
/// </summary>
static std::vector<std::string> SplitString(const std::string& s, char delimiter)
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter))
    {
        tokens.push_back(token);
    }
    return tokens;
}

std::map<std::string, std::vector<fs::path>> GroupCsvFilesByPrefix(const fs::path& directory)
{
    std::map<std::string, std::vector<fs::path>> fileGroups;
    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".csv")
        {
            std::string filename = entry.path().stem().string();
            size_t lastUnderscorePos = filename.find_last_of('_');

            if (lastUnderscorePos != std::string::npos)
            {
                std::string prefix = filename.substr(0, lastUnderscorePos);
                fileGroups[prefix].push_back(entry.path());
            }
        }
    }
    return fileGroups;
}

void CreateDatabaseFromGroup(const fs::path& targetDir, const std::string& dbName, const std::vector<fs::path>& csvFiles)
{
    fs::path dbPath = targetDir / (dbName + ".db");
    std::cout << "\nCreating database: " << dbPath.filename().string() << std::endl;

    sqlite3* dbHandle;
    if (sqlite3_open(dbPath.string().c_str(), &dbHandle) != SQLITE_OK)
    {
        LogSqliteError("Could not create database file", dbHandle);
        sqlite3_close(dbHandle);
        return;
    }

    char* errMsg = nullptr;
    sqlite3_exec(dbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

    for (const auto& csvPath : csvFiles)
    {
        std::string filename = csvPath.stem().string();
        size_t lastUnderscorePos = filename.find_last_of('_');
        std::string tableName = filename.substr(lastUnderscorePos + 1);

        std::cout << "  - Processing file: " << csvPath.filename().string() << " -> table: '" << tableName << "'" << std::endl;

        std::ifstream csvFile(csvPath);
        if (!csvFile.is_open())
        {
            std::cerr << "    ERROR: Could not open CSV file." << std::endl;
            continue;
        }

        // Читаем заголовок
        std::string headerLine;
        if (!std::getline(csvFile, headerLine))
        {
            std::cerr << "    WARNING: CSV file is empty or unreadable." << std::endl;
            continue;
        }
        std::vector<std::string> headers = SplitString(headerLine, ';');

        // Создаем таблицу
        std::stringstream createTableSql;
        createTableSql << "CREATE TABLE IF NOT EXISTS \"" << tableName << "\" (";
        for (size_t i = 0; i < headers.size(); ++i)
        {
            // Для простоты и надежности все столбцы создаем как TEXT
            createTableSql << "\"" << headers[i] << "\" TEXT" << (i == headers.size() - 1 ? "" : ", ");
        }
        createTableSql << ");";
        
        sqlite3_exec(dbHandle, createTableSql.str().c_str(), 0, 0, &errMsg);
        if (errMsg)
        {
            LogSqliteError("Failed to create table", dbHandle);
            sqlite3_free(errMsg);
            errMsg = nullptr;
        }

        // Готовим запрос на вставку
        std::stringstream insertSql;
        insertSql << "INSERT INTO \"" << tableName << "\" VALUES (";
        for (size_t i = 0; i < headers.size(); ++i)
        {
            insertSql << "?" << (i == headers.size() - 1 ? "" : ",");
        }
        insertSql << ");";

        sqlite3_stmt* insertStmt;
        if (sqlite3_prepare_v2(dbHandle, insertSql.str().c_str(), -1, &insertStmt, nullptr) != SQLITE_OK)
        {
            LogSqliteError("Failed to prepare insert statement", dbHandle);
            continue;
        }

        // Читаем и вставляем данные
        std::string dataLine;
        while (std::getline(csvFile, dataLine))
        {
            if (dataLine.empty()) continue;
            std::vector<std::string> values = SplitString(dataLine, ';');

            if (values.size() != headers.size())
            {
                std::cerr << "    WARNING: Row has incorrect number of columns. Skipping." << std::endl;
                continue;
            }

            for (size_t i = 0; i < values.size(); ++i)
            {
                sqlite3_bind_text(insertStmt, i + 1, values[i].c_str(), -1, SQLITE_TRANSIENT);
            }

            if (sqlite3_step(insertStmt) != SQLITE_DONE)
            {
                LogSqliteError("Failed to execute insert step", dbHandle);
            }
            sqlite3_reset(insertStmt);
        }
        sqlite3_finalize(insertStmt);
    }

    sqlite3_exec(dbHandle, "COMMIT;", 0, 0, &errMsg);
    if (errMsg)
    {
        LogSqliteError("Failed to commit transaction", dbHandle);
        sqlite3_free(errMsg);
    }
    sqlite3_close(dbHandle);
}
