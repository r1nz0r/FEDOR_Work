#include "EnvelopeBuilder.h"
#include <iostream>
#include <fstream>
#include <algorithm>
#include <stdexcept>
#include <sstream>

EnvelopeBuilder::EnvelopeBuilder() {}

void EnvelopeBuilder::Run()
{
    std::cout << "--- Universal Envelope Builder ---" << std::endl;
    fs::path targetPath = GetTargetPathFromUser();
    if (targetPath.empty()) return;

    try
    {
        if (!CollectAndVerifyElements(targetPath)) return;

#ifdef MEMORY_OPTIMIZED
        EnvelopeDataOnDisk(targetPath);
#else
        EnvelopeDataInMemory(targetPath);
#endif

        AssembleFinalDatabase(targetPath);
        std::cout << "\nBuild successful!" << std::endl;
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

#ifdef MEMORY_OPTIMIZED
void EnvelopeBuilder::EnvelopeDataOnDisk(const fs::path& targetPath)
{
    std::cout << "\nPASS 2: Enveloping data (On-Disk mode)..." << std::endl;
    fs::path tempDbPath = targetPath / config_.TEMP_DB_FILENAME;
    if (fs::exists(tempDbPath)) fs::remove(tempDbPath);
    
    sqlite3* tempDbHandle;
    if (sqlite3_open(tempDbPath.string().c_str(), &tempDbHandle) != SQLITE_OK) throw std::runtime_error("Could not create temporary database.");
    
    const char* createTempTableSql = "CREATE TABLE EnvelopedData (Element_ID INTEGER, ColumnName TEXT, Max_Value REAL, PRIMARY KEY (Element_ID, ColumnName));";
    char* errMsg = nullptr;
    sqlite3_exec(tempDbHandle, createTempTableSql, 0, 0, &errMsg);
    sqlite3_exec(tempDbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

    for (const auto& entry : fs::directory_iterator(targetPath))
    {
        if (!entry.is_regular_file() || entry.path().extension() != ".db") continue;
        
        std::cout << "  - Processing file: " << entry.path().filename().string() << std::endl;
        
        sqlite3* sourceDbHandle;
        if (sqlite3_open_v2(entry.path().string().c_str(), &sourceDbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) continue;

        const char* upsertSql = "INSERT INTO EnvelopedData VALUES (?, ?, ?) ON CONFLICT(Element_ID, ColumnName) DO UPDATE SET Max_Value = excluded.Max_Value WHERE excluded.Max_Value > Max_Value;";
        sqlite3_stmt* upsertStmt;
        sqlite3_prepare_v2(tempDbHandle, upsertSql, -1, &upsertStmt, nullptr);

        for (const auto& tableName : GetTableNames(sourceDbHandle))
        {
            if (tableName == config_.ELEMENTS_TABLE_NAME) continue;

            std::string query = "SELECT * FROM \"" + tableName + "\";";
            sqlite3_stmt* selectStmt;
            if (sqlite3_prepare_v2(sourceDbHandle, query.c_str(), -1, &selectStmt, nullptr) != SQLITE_OK) continue;

            int colCount = sqlite3_column_count(selectStmt);
            int elemIdIdx = -1;
            for (int i = 0; i < colCount; ++i) if (std::string(sqlite3_column_name(selectStmt, i)) == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;

            if (elemIdIdx != -1) {
                while (sqlite3_step(selectStmt) == SQLITE_ROW) {
                    long long elementId = sqlite3_column_int64(selectStmt, elemIdIdx);
                    for (int i = 0; i < colCount; ++i) {
                        std::string colName = sqlite3_column_name(selectStmt, i);
                        if (colName == config_.ELEMENT_ID_COLUMN || colName == config_.SET_N_COLUMN || colName == config_.ELEM_TYPE_COLUMN) continue;
                        
                        int colType = sqlite3_column_type(selectStmt, i);
                        if (colType == SQLITE_INTEGER || colType == SQLITE_FLOAT) {
                            sqlite3_bind_int64(upsertStmt, 1, elementId);
                            sqlite3_bind_text(upsertStmt, 2, colName.c_str(), -1, SQLITE_TRANSIENT);
                            sqlite3_bind_double(upsertStmt, 3, sqlite3_column_double(selectStmt, i));
                            sqlite3_step(upsertStmt);
                            sqlite3_reset(upsertStmt);
                        }
                    }
                }
            }
            sqlite3_finalize(selectStmt);
        }
        sqlite3_finalize(upsertStmt);
        sqlite3_close(sourceDbHandle);
    }
    
    sqlite3_exec(tempDbHandle, "COMMIT;", 0, 0, &errMsg);
    sqlite3_close(tempDbHandle);
}
#else
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
            for (int i = 0; i < colCount; ++i) if (std::string(sqlite3_column_name(stmt, i)) == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;

            if (elemIdIdx != -1) {
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    long long elementId = sqlite3_column_int64(stmt, elemIdIdx);
                    for (int i = 0; i < colCount; ++i) {
                        std::string colName = sqlite3_column_name(stmt, i);
                        if (colName == config_.ELEMENT_ID_COLUMN || colName == config_.SET_N_COLUMN || colName == config_.ELEM_TYPE_COLUMN) continue;
                        
                        int colType = sqlite3_column_type(stmt, i);
                        if (colType == SQLITE_INTEGER || colType == SQLITE_FLOAT) {
                            double currentValue = sqlite3_column_double(stmt, i);
                            if (envelopedData_[elementId].find(colName) == envelopedData_[elementId].end() || currentValue > envelopedData_[elementId][colName]) {
                                envelopedData_[elementId][colName] = currentValue;
                            }
                        }
                    }
                }
            }
            sqlite3_finalize(stmt);
        }
        sqlite3_close(dbHandle);
    }
}
#endif

void EnvelopeBuilder::AssembleFinalDatabase(const fs::path& targetPath)
{
    std::cout << "\nPASS 3: Assembling final database '" << config_.OUTPUT_DB_FILENAME << "'..." << std::endl;
    fs::path finalDbPath = targetPath / config_.OUTPUT_DB_FILENAME;
    if (fs::exists(finalDbPath)) fs::remove(finalDbPath);

    sqlite3* finalDbHandle;
    if (sqlite3_open(finalDbPath.string().c_str(), &finalDbHandle) != SQLITE_OK) throw std::runtime_error("Could not create final database.");

    char* errMsg = nullptr;
    sqlite3_exec(finalDbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

    // 1. Создаем и заполняем таблицу Elements с ФИКСИРОВАННОЙ СТРУКТУРОЙ
    const char* createElementsTableSql = R"(
        CREATE TABLE "Elements" (
            "elemId"	INT,
            "elemType"	INT,
            "CGrade"	TEXT,
            "SLGrade"	TEXT,
            "STGrade"	TEXT,
            "CSType"	INT,
            "b1"	REAL,
            "h1"	REAL,
            "a1"	REAL,
            "a2"	REAL,
            "t1"	REAL,
            "t2"	REAL,
            "reinfStep1"	REAL,
            "reinfStep2"	REAL,
            "a3"	REAL,
            "a4"	REAL,
            PRIMARY KEY("elemId")
        );
    )";
    sqlite3_exec(finalDbHandle, createElementsTableSql, 0, 0, &errMsg);

    const char* insertElementSql = R"(
        INSERT INTO "Elements" ("elemId", "elemType", "CGrade", "SLGrade", "STGrade", "CSType", 
        "b1", "h1", "a1", "a2", "t1", "t2", "reinfStep1", "reinfStep2", "a3", "a4") 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    )";
    
    sqlite3_stmt* insertElementStmt;
    sqlite3_prepare_v2(finalDbHandle, insertElementSql, -1, &insertElementStmt, nullptr);
    
    // Определяем порядок для вставки
    std::vector<std::string> propOrder = {
        "elemType", "CGrade", "SLGrade", "STGrade", "CSType", "b1", "h1",
        "a1", "a2", "t1", "t2", "reinfStep1", "reinfStep2", "a3", "a4"
    };

    for (const auto& pair : verifiedElements_) {
        sqlite3_bind_int64(insertElementStmt, 1, pair.first); // elemId
        for (size_t i = 0; i < propOrder.size(); ++i) {
            const std::string& propName = propOrder[i];
            // Если свойство есть, вставляем, если нет - NULL
            if (pair.second.count(propName)) {
                 sqlite3_bind_text(insertElementStmt, i + 2, pair.second.at(propName).c_str(), -1, SQLITE_STATIC);
            } else {
                 sqlite3_bind_null(insertElementStmt, i + 2);
            }
        }
        sqlite3_step(insertElementStmt);
        sqlite3_reset(insertElementStmt);
    }
    sqlite3_finalize(insertElementStmt);

    // 2. Собираем все уникальные заголовки для огибающей таблицы
    std::set<std::string> allHeadersSet;
#ifdef MEMORY_OPTIMIZED
    fs::path tempDbPath = targetPath / config_.TEMP_DB_FILENAME;
    sqlite3* tempDbHandle;
    sqlite3_open(tempDbPath.string().c_str(), &tempDbHandle);
    allHeadersSet = CollectAllEnvelopedColumnsFromTempDb(tempDbHandle);
    sqlite3_close(tempDbHandle);
#else
    allHeadersSet = CollectAllEnvelopedColumns();
#endif

    if (allHeadersSet.empty()) {
        std::cout << "No enveloped data found to assemble." << std::endl;
    } else {
        // 3. ИСПОЛЬЗУЕМ ФИКСИРОВАННЫЙ ПОРЯДОК КОЛОНОК, как в прошлый раз
        std::vector<std::string> orderedHeaders = {
            "As1Ti", "As1Tj", "As1Bi", "As1Bj", "As2Ti", "As2Tj", "As2Bi", "As2Bj",
            "Asw1i", "Asw1j", "Asw2i", "Asw2j", "Reinf1", "Reinf2", "Crack1i", "Crack1j",
            "Crack2i", "Crack2j", "Sw1i", "Sw1j", "Sw2i", "Sw2j", "ls1i", "ls1j", "ls2i", "ls2j"
        };
        std::vector<std::string> finalHeaders;
        for(const auto& header : orderedHeaders) {
            if(allHeadersSet.count(header)) finalHeaders.push_back(header);
        }

        // 4. Динамически создаем таблицу "Enveloped Reinforcement"
        std::stringstream createReinfTableSql;
        createReinfTableSql << "CREATE TABLE \"" << config_.ENVELOPED_TABLE_NAME << "\" ("
                            << "\"" << config_.SET_N_COLUMN << "\" INT, "
                            << "\"" << config_.ELEMENT_ID_COLUMN << "\" INT, "
                            << "\"" << config_.ELEM_TYPE_COLUMN << "\" INT";
        for (const auto& header : finalHeaders) createReinfTableSql << ", \"" << header << "\" REAL";
        createReinfTableSql << ", PRIMARY KEY(\"" << config_.ELEMENT_ID_COLUMN << "\")"
                            << ", CONSTRAINT \"fk_elements\" FOREIGN KEY(\"" << config_.ELEMENT_ID_COLUMN << "\") REFERENCES \"" << config_.ELEMENTS_TABLE_NAME << "\"(elemId));";
        sqlite3_exec(finalDbHandle, createReinfTableSql.str().c_str(), 0, 0, &errMsg);

        // 5. Заполняем ее
        std::stringstream insertReinfSql;
        insertReinfSql << "INSERT INTO \"" << config_.ENVELOPED_TABLE_NAME << "\" (\"" << config_.SET_N_COLUMN << "\", \"" << config_.ELEMENT_ID_COLUMN << "\", \"" << config_.ELEM_TYPE_COLUMN << "\"";
        for (const auto& header : finalHeaders) insertReinfSql << ", \"" << header << "\"";
        insertReinfSql << ") VALUES (?,?,?";
        for (size_t i = 0; i < finalHeaders.size(); ++i) insertReinfSql << ",?";
        insertReinfSql << ");";
        
        sqlite3_stmt* insertStmt;
        sqlite3_prepare_v2(finalDbHandle, insertReinfSql.str().c_str(), -1, &insertStmt, nullptr);

#ifdef MEMORY_OPTIMIZED
        fs::path tempDbPath = targetPath / config_.TEMP_DB_FILENAME;
        std::string attachSql = "ATTACH DATABASE '" + tempDbPath.string() + "' AS temp_db;";
        sqlite3_exec(finalDbHandle, attachSql.c_str(), 0, 0, &errMsg);
        
        for (const auto& elemPair : verifiedElements_) {
            long long elementId = elemPair.first;
            
            sqlite3_bind_int64(insertStmt, 1, 1);
            sqlite3_bind_int64(insertStmt, 2, elementId);
            
            if (elemPair.second.count(config_.ELEM_TYPE_COLUMN)) {
                 sqlite3_bind_int(insertStmt, 3, std::stoi(elemPair.second.at(config_.ELEM_TYPE_COLUMN)));
            } else {
                 sqlite3_bind_null(insertStmt, 3);
            }

            int colIdx = 4;
            for (const auto& header : finalHeaders) {
                sqlite3_stmt* selectValueStmt;
                sqlite3_prepare_v2(finalDbHandle, "SELECT Max_Value FROM temp_db.EnvelopedData WHERE Element_ID = ? AND ColumnName = ?;", -1, &selectValueStmt, nullptr);
                sqlite3_bind_int64(selectValueStmt, 1, elementId);
                sqlite3_bind_text(selectValueStmt, 2, header.c_str(), -1, SQLITE_STATIC);
                if (sqlite3_step(selectValueStmt) == SQLITE_ROW) {
                    sqlite3_bind_double(insertStmt, colIdx, sqlite3_column_double(selectValueStmt, 0));
                } else {
                    sqlite3_bind_null(insertStmt, colIdx);
                }
                sqlite3_finalize(selectValueStmt);
                colIdx++;
            }
            sqlite3_step(insertStmt);
            sqlite3_reset(insertStmt);
        }
        sqlite3_exec(finalDbHandle, "DETACH DATABASE temp_db;", 0, 0, &errMsg);
        if (fs::exists(tempDbPath)) fs::remove(tempDbPath);
#else
        for (const auto& elemPair : envelopedData_) {
            long long elementId = elemPair.first;
            
            sqlite3_bind_int64(insertStmt, 1, 1);
            sqlite3_bind_int64(insertStmt, 2, elementId);

            if (verifiedElements_.count(elementId) && verifiedElements_.at(elementId).count(config_.ELEM_TYPE_COLUMN)) {
                sqlite3_bind_int(insertStmt, 3, std::stoi(verifiedElements_.at(elementId).at(config_.ELEM_TYPE_COLUMN)));
            } else {
                sqlite3_bind_null(insertStmt, 3);
            }

            int colIdx = 4;
            for (const auto& header : finalHeaders) {
                if (elemPair.second.count(header)) {
                    sqlite3_bind_double(insertStmt, colIdx, elemPair.second.at(header));
                } else {
                    sqlite3_bind_null(insertStmt, colIdx);
                }
                colIdx++;
            }
            sqlite3_step(insertStmt);
            sqlite3_reset(insertStmt);
        }
#endif
        sqlite3_finalize(insertStmt);
    }
    
    sqlite3_exec(finalDbHandle, "COMMIT;", 0, 0, &errMsg);
    if(errMsg) {
        LogSqliteError("Error during final assembly", finalDbHandle);
        sqlite3_free(errMsg);
    }
    sqlite3_close(finalDbHandle);
    std::cout << "OK: Final database created successfully." << std::endl;
}

// --- Остальные вспомогательные функции ---
std::set<std::string> EnvelopeBuilder::CollectAllEnvelopedColumns() { /* ... */ return {}; }
std::set<std::string> EnvelopeBuilder::CollectAllEnvelopedColumnsFromTempDb(sqlite3*) { /* ... */ return {}; }
fs::path EnvelopeBuilder::GetTargetPathFromUser() { /* ... */ return {}; }
void EnvelopeBuilder::LogSqliteError(const std::string&, sqlite3*) { /* ... */ }
std::vector<std::string> EnvelopeBuilder::GetTableNames(sqlite3*) { /* ... */ return {}; }

