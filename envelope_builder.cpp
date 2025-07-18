/*
--------------------------------------------------------------------------
|                                 EnvelopeBuilder.h                                |
--------------------------------------------------------------------------
*/
#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <set>
#include <map>

#include "sqlite3.h"

namespace fs = std::filesystem;

namespace Builder
{
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
            // --- НОВОЕ: Имя для второй, "суммирующей" базы данных ---
            const std::string OUTPUT_DB_SUMMED_FILENAME = "Envelope_Summed.db";
            const std::string ENVELOPED_TABLE_NAME = "Enveloped Reinforcement";
        };

        using ElementProperties = std::unordered_map<std::string, std::string>;
        using VerifiedElementsMap = std::unordered_map<long long, ElementProperties>;

        // Ключ - ID элемента, значение - карта {Имя столбца: максимальное значение}
        // Сюда же будут сохраняться и "виртуальные" поля для сумм.
        using EnvelopedDataMap = std::unordered_map<long long, std::unordered_map<std::string, double>>;

        Config config_;
        VerifiedElementsMap verifiedElements_;
        EnvelopedDataMap envelopedData_;

        // --- Основные этапы ---
        bool CollectAndVerifyElements(const fs::path& targetPath);
        void EnvelopeDataInMemory(const fs::path& targetPath);
        // --- ИЗМЕНЕНО: Метод теперь принимает флаг, указывающий, какую версию БД создавать ---
        void AssembleFinalDatabase(const fs::path& targetPath, bool createSummedVersion);

        // --- Вспомогательные методы ---
        fs::path GetTargetPathFromUser();
        void LogSqliteError(const std::string& message, sqlite3* dbHandle);
        std::vector<std::string> GetTableNames(sqlite3* dbHandle);
        std::set<std::string> CollectAllEnvelopedColumns();
    };
}


/*
--------------------------------------------------------------------------
|                               EnvelopeBuilder.cpp                              |
--------------------------------------------------------------------------
*/
#include <iostream>
#include <fstream>
#include <algorithm>
#include <stdexcept>
#include <sstream>

// #include "EnvelopeBuilder.h" // Предполагается, что заголовок в том же файле

namespace Builder
{
    EnvelopeBuilder::EnvelopeBuilder() {}

    void EnvelopeBuilder::Run()
    {
        std::cout << "--- Universal Envelope Builder ---" << std::endl;
        fs::path targetPath = GetTargetPathFromUser();
        if (targetPath.empty()) return;

        try
        {
            // Проходы 1 и 2 являются общими для обеих баз данных
            if (!CollectAndVerifyElements(targetPath)) return;
            EnvelopeDataInMemory(targetPath);

            // --- ИЗМЕНЕНО: Сначала создаем оригинальную БД ---
            std::cout << "\n--- Assembling ORIGINAL database ---" << std::endl;
            AssembleFinalDatabase(targetPath, false);

            // --- ИЗМЕНЕНО: Затем создаем новую, "суммирующую" БД ---
            std::cout << "\n--- Assembling SUMMED database (for shells) ---" << std::endl;
            AssembleFinalDatabase(targetPath, true);

            std::cout << "\nBuild successful for both databases!" << std::endl;
        }
        catch (const std::exception& e)
        {
            std::cerr << "\nCRITICAL ERROR: " << e.what() << std::endl;
        }
    }

    bool EnvelopeBuilder::CollectAndVerifyElements(const fs::path& targetPath)
    {
        std::cout << "\nPASS 1: Verifying '" << config_.ELEMENTS_TABLE_NAME << "' tables..." << std::endl;
        for (const auto& entry : fs::directory_iterator(targetPath))
        {
            if (!entry.is_regular_file() || entry.path().extension() != ".db") continue;

            sqlite3* dbHandle;
            if (sqlite3_open_v2(entry.path().string().c_str(), &dbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) continue;

            std::string query = "SELECT * FROM \"" + config_.ELEMENTS_TABLE_NAME + "\";";
            sqlite3_stmt* stmt;
            if (sqlite3_prepare_v2(dbHandle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            {
                sqlite3_close(dbHandle);
                continue;
            }

            std::cout << "  - Checking file: " << entry.path().filename().string() << std::endl;

            int colCount = sqlite3_column_count(stmt);
            int elemIdIdx = -1;
            std::vector<std::string> colNames;
            for (int i = 0; i < colCount; ++i)
            {
                std::string colName = sqlite3_column_name(stmt, i);
                if (colName == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;
                colNames.push_back(colName);
            }

            if (elemIdIdx == -1)
            {
                sqlite3_close(dbHandle);
                continue;
            }

            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                long long currentElemId = sqlite3_column_int64(stmt, elemIdIdx);
                ElementProperties currentProps;
                for (int i = 0; i < colCount; ++i)
                {
                    if (i == elemIdIdx) continue;
                    const char* value = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                    currentProps[colNames[i]] = value ? value : "";
                }

                if (verifiedElements_.count(currentElemId))
                {
                    if (currentProps != verifiedElements_.at(currentElemId))
                    {
                        throw std::runtime_error("Data mismatch for elemId " + std::to_string(currentElemId) + " in file '" + entry.path().filename().string() + "'.");
                    }
                }
                else
                {
                    verifiedElements_[currentElemId] = currentProps;
                }
            }
            sqlite3_finalize(stmt);
            sqlite3_close(dbHandle);
        }
        std::cout << "Verification successful. Found " << verifiedElements_.size() << " unique elements." << std::endl;
        return true;
    }

    void EnvelopeBuilder::EnvelopeDataInMemory(const fs::path& targetPath)
    {
        std::cout << "\nPASS 2: Enveloping data (In-Memory mode)..." << std::endl;
        for (const auto& entry : fs::directory_iterator(targetPath))
        {
            if (!entry.is_regular_file() || entry.path().extension() != ".db") continue;
            std::cout << "  - Processing file: " << entry.path().filename().string() << std::endl;
            sqlite3* dbHandle;
            if (sqlite3_open_v2(entry.path().string().c_str(), &dbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) continue;

            for (const auto& tableName : GetTableNames(dbHandle))
            {
                if (tableName == config_.ELEMENTS_TABLE_NAME) continue;

                std::string query = "SELECT * FROM \"" + tableName + "\";";
                sqlite3_stmt* stmt;
                if (sqlite3_prepare_v2(dbHandle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) continue;

                int colCount = sqlite3_column_count(stmt);
                int elemIdIdx = -1;
                std::vector<std::string> colNames;
                for (int i = 0; i < colCount; ++i) 
                {
                    std::string colName = sqlite3_column_name(stmt, i);
                    colNames.push_back(colName);
                    if (colName == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;
                }

                if (elemIdIdx == -1) 
                {
                    sqlite3_finalize(stmt);
                    continue;
                }
                
                // --- ИЗМЕНЕНО: Логика перестроена для обработки всей строки целиком ---
                while (sqlite3_step(stmt) == SQLITE_ROW)
                {
                    long long elementId = sqlite3_column_int64(stmt, elemIdIdx);
                    if (verifiedElements_.find(elementId) == verifiedElements_.end()) continue;

                    std::unordered_map<std::string, double> currentRowNumerics;

                    // 1. Сначала применяем стандартное огибание для ВСЕХ числовых столбцов
                    // и одновременно собираем значения из текущей строки для логики суммирования.
                    for (int i = 0; i < colCount; ++i)
                    {
                        const std::string& colName = colNames[i];
                        if (colName == config_.ELEMENT_ID_COLUMN || colName == config_.SET_N_COLUMN || colName == config_.ELEM_TYPE_COLUMN) continue;

                        int colType = sqlite3_column_type(stmt, i);
                        if (colType == SQLITE_INTEGER || colType == SQLITE_FLOAT)
                        {
                            double currentValue = sqlite3_column_double(stmt, i);
                            currentRowNumerics[colName] = currentValue; // Сохраняем для шага 2

                            // Стандартное огибание (нахождение максимума)
                            if (envelopedData_[elementId].find(colName) == envelopedData_[elementId].end() || currentValue > envelopedData_[elementId][colName])
                            {
                                envelopedData_[elementId][colName] = currentValue;
                            }
                        }
                    }

                    // 2. Затем, если это оболочка, дополнительно вычисляем и огибаем СУММЫ
                    bool isShell = verifiedElements_.at(elementId).count(config_.ELEM_TYPE_COLUMN) &&
                                   verifiedElements_.at(elementId).at(config_.ELEM_TYPE_COLUMN) == "2";
                    
                    if (isShell)
                    {
                        double asw1i = currentRowNumerics.count("Asw1i") ? currentRowNumerics.at("Asw1i") : 0.0;
                        double asw2i = currentRowNumerics.count("Asw2i") ? currentRowNumerics.at("Asw2i") : 0.0;
                        double asw1j = currentRowNumerics.count("Asw1j") ? currentRowNumerics.at("Asw1j") : 0.0;
                        double asw2j = currentRowNumerics.count("Asw2j") ? currentRowNumerics.at("Asw2j") : 0.0;

                        double sum_i = asw1i + asw2i;
                        double sum_j = asw1j + asw2j;

                        // Огибаем сумму для узла i, сохраняя в "виртуальное" поле
                        const std::string sum_i_key = "__Asw_sum_i";
                        if (envelopedData_[elementId].find(sum_i_key) == envelopedData_[elementId].end() || sum_i > envelopedData_[elementId][sum_i_key])
                        {
                            envelopedData_[elementId][sum_i_key] = sum_i;
                        }
                        
                        // Огибаем сумму для узла j, сохраняя в "виртуальное" поле
                        const std::string sum_j_key = "__Asw_sum_j";
                        if (envelopedData_[elementId].find(sum_j_key) == envelopedData_[elementId].end() || sum_j > envelopedData_[elementId][sum_j_key])
                        {
                            envelopedData_[elementId][sum_j_key] = sum_j;
                        }
                    }
                }
                sqlite3_finalize(stmt);
            }
            sqlite3_close(dbHandle);
        }
    }

    void EnvelopeBuilder::AssembleFinalDatabase(const fs::path& targetPath, bool createSummedVersion)
    {
        // --- ИЗМЕНЕНО: Выбираем имя файла и выводим сообщение в зависимости от типа создаваемой БД ---
        const std::string dbFilename = createSummedVersion ? config_.OUTPUT_DB_SUMMED_FILENAME : config_.OUTPUT_DB_FILENAME;
        std::cout << "\nPASS 3: Assembling final database '" << dbFilename << "'..." << std::endl;
        
        fs::path finalDbPath = targetPath / dbFilename;
        if (fs::exists(finalDbPath)) fs::remove(finalDbPath);

        sqlite3* finalDbHandle;
        if (sqlite3_open(finalDbPath.string().c_str(), &finalDbHandle) != SQLITE_OK) throw std::runtime_error("Could not create final database.");

        char* errMsg = nullptr;
        sqlite3_exec(finalDbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

        // 1. Создание и заполнение таблицы Elements (без изменений)
        const char* createElementsTableSql = R"(
        CREATE TABLE "Elements" (
            "elemId"	INT, "elemType"	INT, "CGrade"	TEXT, "SLGrade"	TEXT, "STGrade"	TEXT,
            "CSType"	INT, "b1"	REAL, "h1"	REAL, "a1"	REAL, "a2"	REAL, "t1"	REAL,
            "t2"	REAL, "reinfStep1"	REAL, "reinfStep2"	REAL, "a3"	REAL, "a4"	REAL,
            PRIMARY KEY("elemId")
        );)";
        sqlite3_exec(finalDbHandle, createElementsTableSql, 0, 0, &errMsg);

        const char* insertElementSql = R"(
        INSERT INTO "Elements" ("elemId", "elemType", "CGrade", "SLGrade", "STGrade", "CSType", 
        "b1", "h1", "a1", "a2", "t1", "t2", "reinfStep1", "reinfStep2", "a3", "a4") 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);)";
        sqlite3_stmt* insertElementStmt;
        sqlite3_prepare_v2(finalDbHandle, insertElementSql, -1, &insertElementStmt, nullptr);
        std::vector<std::string> propOrder = {
            "elemType", "CGrade", "SLGrade", "STGrade", "CSType", "b1", "h1",
            "a1", "a2", "t1", "t2", "reinfStep1", "reinfStep2", "a3", "a4"
        };
        for (const auto& pair : verifiedElements_)
        {
            sqlite3_bind_int64(insertElementStmt, 1, pair.first);
            for (size_t i = 0; i < propOrder.size(); ++i)
            {
                const std::string& propName = propOrder[i];
                if (pair.second.count(propName))
                    sqlite3_bind_text(insertElementStmt, i + 2, pair.second.at(propName).c_str(), -1, SQLITE_STATIC);
                else
                    sqlite3_bind_null(insertElementStmt, i + 2);
            }
            sqlite3_step(insertElementStmt);
            sqlite3_reset(insertElementStmt);
        }
        sqlite3_finalize(insertElementStmt);

        // 2. Сборка таблицы "Enveloped Reinforcement"
        std::set<std::string> allHeadersSet = CollectAllEnvelopedColumns();
        if (allHeadersSet.empty())
        {
            std::cout << "No enveloped data found to assemble." << std::endl;
        }
        else
        {
            std::vector<std::string> orderedHeaders = {
                "As1Ti", "As1Tj", "As1Bi", "As1Bj", "As2Ti", "As2Tj", "As2Bi", "As2Bj",
                "Asw1i", "Asw1j", "Asw2i", "Asw2j", "Reinf1", "Reinf2", "Crack1i", "Crack1j",
                "Crack2i", "Crack2j", "Sw1i", "Sw1j", "Sw2i", "Sw2j", "ls1i", "ls1j", "ls2i", "ls2j"
            };
            std::vector<std::string> finalHeaders;
            for (const auto& header : orderedHeaders)
            {
                if (allHeadersSet.count(header)) finalHeaders.push_back(header);
            }

            std::stringstream createReinfTableSql;
            createReinfTableSql << "CREATE TABLE \"" << config_.ENVELOPED_TABLE_NAME << "\" ("
                << "\"" << config_.SET_N_COLUMN << "\" INT, "
                << "\"" << config_.ELEMENT_ID_COLUMN << "\" INT, "
                << "\"" << config_.ELEM_TYPE_COLUMN << "\" INT";
            for (const auto& header : finalHeaders) createReinfTableSql << ", \"" << header << "\" REAL";
            createReinfTableSql << ", PRIMARY KEY(\"" << config_.ELEMENT_ID_COLUMN << "\")"
                << ", CONSTRAINT \"fk_elements\" FOREIGN KEY(\"" << config_.ELEMENT_ID_COLUMN << "\") REFERENCES \"" << config_.ELEMENTS_TABLE_NAME << "\"(elemId));";
            sqlite3_exec(finalDbHandle, createReinfTableSql.str().c_str(), 0, 0, &errMsg);

            std::stringstream insertReinfSql;
            insertReinfSql << "INSERT INTO \"" << config_.ENVELOPED_TABLE_NAME << "\" (\"" << config_.SET_N_COLUMN << "\", \"" << config_.ELEMENT_ID_COLUMN << "\", \"" << config_.ELEM_TYPE_COLUMN << "\"";
            for (const auto& header : finalHeaders) insertReinfSql << ", \"" << header << "\"";
            insertReinfSql << ") VALUES (?,?,?";
            for (size_t i = 0; i < finalHeaders.size(); ++i) insertReinfSql << ",?";
            insertReinfSql << ");";

            sqlite3_stmt* insertStmt;
            sqlite3_prepare_v2(finalDbHandle, insertReinfSql.str().c_str(), -1, &insertStmt, nullptr);

            for (const auto& elemPair : envelopedData_)
            {
                long long elementId = elemPair.first;
                const auto& elementData = elemPair.second;

                sqlite3_bind_int64(insertStmt, 1, 1);
                sqlite3_bind_int64(insertStmt, 2, elementId);

                if (verifiedElements_.count(elementId) && verifiedElements_.at(elementId).count(config_.ELEM_TYPE_COLUMN))
                    sqlite3_bind_int(insertStmt, 3, std::stoi(verifiedElements_.at(elementId).at(config_.ELEM_TYPE_COLUMN)));
                else
                    sqlite3_bind_null(insertStmt, 3);
                
                // --- ИЗМЕНЕНО: Главная логика подмены значений для суммирующей версии ---
                bool isShellForSumming = createSummedVersion &&
                                         verifiedElements_.count(elementId) &&
                                         verifiedElements_.at(elementId).count(config_.ELEM_TYPE_COLUMN) &&
                                         verifiedElements_.at(elementId).at(config_.ELEM_TYPE_COLUMN) == "2";

                int colIdx = 4;
                for (const auto& header : finalHeaders)
                {
                    double valueToBind = 0.0;
                    bool valueWasSet = false;

                    if (isShellForSumming) {
                        if (header == "Asw1i") {
                            valueToBind = elementData.count("__Asw_sum_i") ? elementData.at("__Asw_sum_i") : 0.0;
                            valueWasSet = true;
                        } else if (header == "Asw2i") {
                            valueToBind = 0.0; // Зануляем
                            valueWasSet = true;
                        } else if (header == "Asw1j") {
                            valueToBind = elementData.count("__Asw_sum_j") ? elementData.at("__Asw_sum_j") : 0.0;
                            valueWasSet = true;
                        } else if (header == "Asw2j") {
                            valueToBind = 0.0; // Зануляем
                            valueWasSet = true;
                        }
                    }

                    if (valueWasSet) {
                        sqlite3_bind_double(insertStmt, colIdx, valueToBind);
                    } else {
                        // Стандартная логика для всех остальных случаев
                        if (elementData.count(header)) {
                            sqlite3_bind_double(insertStmt, colIdx, elementData.at(header));
                        } else {
                            sqlite3_bind_null(insertStmt, colIdx);
                        }
                    }
                    colIdx++;
                }
                sqlite3_step(insertStmt);
                sqlite3_reset(insertStmt);
            }
            sqlite3_finalize(insertStmt);
        }

        sqlite3_exec(finalDbHandle, "COMMIT;", 0, 0, &errMsg);
        if (errMsg)
        {
            LogSqliteError("Error during final assembly", finalDbHandle);
            sqlite3_free(errMsg);
        }
        sqlite3_close(finalDbHandle);
        std::cout << "OK: Database '" << dbFilename << "' created successfully." << std