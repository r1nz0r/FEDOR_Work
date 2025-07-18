#include "DbUtils.h"
#include "sqlite3.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>

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
        if (!token.empty() && token.back() == '\r') {
            token.pop_back();
        }
        tokens.push_back(token);
    }
    return tokens;
}

/// <summary>
/// "Угадывает" тип данных SQLite по строковому значению.
/// </summary>
static std::string InferDataType(const std::string& value)
{
    if (value.empty()) return "TEXT";

    bool hasDecimal = false;
    bool isNumeric = true;
    for (size_t i = 0; i < value.length(); ++i) {
        if (i == 0 && value[i] == '-') continue; // Allow negative sign
        if (value[i] == '.' || value[i] == ',') { // Allow decimal separators
            if (hasDecimal) { // More than one decimal point
                isNumeric = false;
                break;
            }
            hasDecimal = true;
        } else if (!isdigit(value[i])) {
            isNumeric = false;
            break;
        }
    }

    if (isNumeric) {
        return hasDecimal ? "REAL" : "INTEGER";
    }
    return "TEXT";
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
        if (!csvFile.is_open()) continue;

        std::string headerLine;
        if (!std::getline(csvFile, headerLine)) continue;
        
        std::vector<std::string> csvHeaders = SplitString(headerLine, ';');
        
        std::string createTableSql;
        std::string insertSql;
        
        if (tableName == "Elements")
        {
            createTableSql = R"(CREATE TABLE "Elements" ("elemId" INT, "elemType" INT, "CGrade" TEXT, "SLGrade" TEXT, "STGrade" TEXT, "CSType" INT, "b1" REAL, "h1" REAL, "a1" REAL, "a2" REAL, "t1" REAL, "t2" REAL, "reinfStep1" REAL, "reinfStep2" REAL, "a3" REAL, "a4" REAL, PRIMARY KEY("elemId"));)";
            insertSql = R"(INSERT INTO "Elements" ("elemId", "elemType", "CGrade", "SLGrade", "STGrade", "CSType", "b1", "h1", "a1", "a2", "t1", "t2", "reinfStep1", "reinfStep2", "a3", "a4") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);)";
        }
        else if (tableName == "Enveloped Reinforcement")
        {
            createTableSql = R"(CREATE TABLE "Enveloped Reinforcement" ("setN" INT, "elemId" INT, "elemType" INT, "As1Ti" REAL, "As1Tj" REAL, "As1Bi" REAL, "As1Bj" REAL, "As2Ti" REAL, "As2Tj" REAL, "As2Bi" REAL, "As2Bj" REAL, "Asw1i" REAL, "Asw1j" REAL, "Asw2i" REAL, "Asw2j" REAL, "Reinf1" REAL, "Reinf2" REAL, "Crack1i" REAL, "Crack1j" REAL, "Crack2i" REAL, "Crack2j" REAL, "Sw1i" REAL, "Sw1j" REAL, "Sw2i" REAL, "Sw2j" REAL, "ls1i" REAL, "ls1j" REAL, "ls2i" REAL, "ls2j" REAL, CONSTRAINT "fk_elements" FOREIGN KEY("elemId") REFERENCES "Elements"("elemId"), PRIMARY KEY("elemId"));)";
            insertSql = R"(INSERT INTO "Enveloped Reinforcement" ("setN", "elemId", "elemType", "As1Ti", "As1Tj", "As1Bi", "As1Bj", "As2Ti", "As2Tj", "As2Bi", "As2Bj", "Asw1i", "Asw1j", "Asw2i", "Asw2j", "Reinf1", "Reinf2", "Crack1i", "Crack1j", "Crack2i", "Crack2j", "Sw1i", "Sw1j", "Sw2i", "Sw2j", "ls1i", "ls1j", "ls2i", "ls2j") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);)";
        }
        else // Блок для всех остальных, обычных таблиц
        {
            // --- НОВАЯ ЛОГИКА: "Угадываем" типы данных ---
            std::string firstDataLine;
            std::vector<std::string> firstDataValues;
            if (std::getline(csvFile, firstDataLine)) {
                firstDataValues = SplitString(firstDataLine, ';');
            }

            std::stringstream createSqlStream, insertSqlStream;
            createSqlStream << "CREATE TABLE IF NOT EXISTS \"" << tableName << "\" (";
            insertSqlStream << "INSERT INTO \"" << tableName << "\" VALUES (";
            for (size_t i = 0; i < csvHeaders.size(); ++i)
            {
                std::string colType = "TEXT";
                if (i < firstDataValues.size()) {
                    colType = InferDataType(firstDataValues[i]);
                }
                createSqlStream << "\"" << csvHeaders[i] << "\" " << colType << (i == csvHeaders.size() - 1 ? "" : ", ");
                insertSqlStream << "?" << (i == csvHeaders.size() - 1 ? "" : ",");
            }
            createSqlStream << ");";
            insertSqlStream << ");";
            createTableSql = createSqlStream.str();
            insertSql = insertSqlStream.str();
            
            // Возвращаем файловый указатель в начало данных
            csvFile.clear();
            csvFile.seekg(headerLine.length() + 1); // +1 for newline
        }
        
        sqlite3_exec(dbHandle, createTableSql.c_str(), 0, 0, &errMsg);
        if (errMsg) { LogSqliteError("Failed to create table", dbHandle); sqlite3_free(errMsg); errMsg = nullptr; }

        sqlite3_stmt* insertStmt;
        if (sqlite3_prepare_v2(dbHandle, insertSql.c_str(), -1, &insertStmt, nullptr) != SQLITE_OK)
        {
            LogSqliteError("Failed to prepare insert statement", dbHandle);
            continue;
        }

        std::string dataLine;
        while (std::getline(csvFile, dataLine))
        {
            if (dataLine.empty()) continue;
            std::vector<std::string> values = SplitString(dataLine, ';');
            if (values.size() != csvHeaders.size()) continue;

            for (size_t i = 0; i < values.size(); ++i)
            {
                sqlite3_bind_text(insertStmt, i + 1, values[i].c_str(), -1, SQLITE_TRANSIENT);
            }
            if (sqlite3_step(insertStmt) != SQLITE_DONE) LogSqliteError("Failed to execute insert step", dbHandle);
            sqlite3_reset(insertStmt);
        }
        sqlite3_finalize(insertStmt);
    }

    sqlite3_exec(dbHandle, "COMMIT;", 0, 0, &errMsg);
    if (errMsg) { LogSqliteError("Failed to commit transaction", dbHandle); sqlite3_free(errMsg); }
    sqlite3_close(dbHandle);
}

