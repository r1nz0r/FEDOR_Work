#include "EnvelopeAnalyzer.h" // Подключаем наш заголовочный файл
#include <iostream>
#include <fstream>
#include <algorithm>

// --- РЕАЛИЗАЦИЯ МЕТОДОВ КЛАССА ---

EnvelopeAnalyzer::EnvelopeAnalyzer()
{
    // Конструктор может быть использован для начальной инициализации, если потребуется
}

void EnvelopeAnalyzer::Run()
{
    std::cout << "--- Reinforcement Envelope Analyzer (Dual Mode) ---" << std::endl;

    fs::path targetPath = GetTargetPathFromUser();
    if (targetPath.empty())
    {
        return; // Выход, если путь некорректен
    }

#ifdef MEMORY_OPTIMIZED
    RunOnDisk(targetPath);
#else
    RunInMemory(targetPath);
#endif

    std::cout << "\nAnalysis complete!" << std::endl;
}

fs::path EnvelopeAnalyzer::GetTargetPathFromUser()
{
    std::cout << "Enter path to directory with .db files (or '.' for current directory): ";
    std::string inputPathStr;
    std::getline(std::cin, inputPathStr);

    fs::path targetPath;
    if (inputPathStr.empty() || inputPathStr == ".")
    {
        targetPath = ".";
    }
    else
    {
        targetPath = inputPathStr;
    }

    if (!fs::exists(targetPath) || !fs::is_directory(targetPath))
    {
        std::cerr << "ERROR: Path does not exist or is not a directory: " << inputPathStr << std::endl;
        return {};
    }
    return targetPath;
}

void EnvelopeAnalyzer::LogSqliteError(const std::string& message, sqlite3* dbHandle)
{
    std::cerr << "  ERROR: " << message << ": " << sqlite3_errmsg(dbHandle) << std::endl;
}

std::vector<std::string> EnvelopeAnalyzer::GetTableNames(sqlite3* dbHandle)
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

bool EnvelopeAnalyzer::IsProcessableDbFile(const fs::directory_entry& entry)
{
    if (!entry.is_regular_file() || entry.path().extension() != ".db")
    {
        return false;
    }
    const std::string filename = entry.path().filename().string();
    return filename != config_.OUTPUT_DB_FILENAME && filename != config_.TEMP_DB_FILENAME;
}


#ifdef MEMORY_OPTIMIZED
// =================================================================
//    РЕАЛИЗАЦИЯ ДЛЯ РЕЖИМА С НИЗКОЙ ПАМЯТЬЮ (НА ДИСКЕ)
// =================================================================

void EnvelopeAnalyzer::RunOnDisk(const fs::path& targetPath)
{
    std::cout << "\n>> Running in MEMORY OPTIMIZED (On-Disk) mode." << std::endl;
    
    fs::path tempDbPath = targetPath / config_.TEMP_DB_FILENAME;
    sqlite3* tempDbHandle = InitializeTempDatabase(tempDbPath);
    if (!tempDbHandle) return;

    char* errMsg = nullptr;
    sqlite3_exec(tempDbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

    int fileCount = 0;
    for (const auto& entry : fs::directory_iterator(targetPath))
    {
        if (IsProcessableDbFile(entry))
        {
            ProcessDatabaseOnDisk(entry.path(), tempDbHandle);
            fileCount++;
        }
    }
    
    sqlite3_exec(tempDbHandle, "COMMIT;", 0, 0, &errMsg);

    if (fileCount > 0)
    {
        SaveFinalResultsOnDisk(tempDbHandle, targetPath);
    }
    else
    {
        std::cout << "\nNo .db files found to process in the specified directory." << std::endl;
    }

    sqlite3_close(tempDbHandle);
    if (fs::exists(tempDbPath)) fs::remove(tempDbPath);
}

sqlite3* EnvelopeAnalyzer::InitializeTempDatabase(const fs::path& tempDbPath)
{
    if (fs::exists(tempDbPath)) fs::remove(tempDbPath);
    
    sqlite3* tempDbHandle;
    if (sqlite3_open(tempDbPath.string().c_str(), &tempDbHandle) != SQLITE_OK)
    {
        LogSqliteError("Could not create temporary database", tempDbHandle);
        return nullptr;
    }
    
    const char* createTempTableSql = R"(
        CREATE TABLE IntermediateResults (
            Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL,
            Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER,
            PRIMARY KEY (Element_ID, Reinforcement_Type)
        );
    )";
    char* errMsg = nullptr;
    sqlite3_exec(tempDbHandle, createTempTableSql, 0, 0, &errMsg);
    if(errMsg) {
        LogSqliteError("Failed to create temp table", tempDbHandle);
        sqlite3_free(errMsg);
    }

    return tempDbHandle;
}

void EnvelopeAnalyzer::ProcessDatabaseOnDisk(const fs::path& dbPath, sqlite3* tempDbHandle)
{
    std::cout << "\nProcessing file: " << dbPath.filename().string() << std::endl;
    sqlite3* sourceDbHandle;
    if (sqlite3_open_v2(dbPath.string().c_str(), &sourceDbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Could not open source file", sourceDbHandle);
        sqlite3_close(sourceDbHandle);
        return;
    }

    const char* upsertSql = R"(
        INSERT INTO IntermediateResults VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(Element_ID, Reinforcement_Type) DO UPDATE SET
            Max_Value = excluded.Max_Value, Source_DB = excluded.Source_DB,
            Source_Table = excluded.Source_Table, Source_SetN = excluded.Source_SetN
        WHERE excluded.Max_Value > Max_Value;
    )";
    sqlite3_stmt* upsertStmt;
    if (sqlite3_prepare_v2(tempDbHandle, upsertSql, -1, &upsertStmt, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Failed to prepare UPSERT statement", tempDbHandle);
        sqlite3_close(sourceDbHandle);
        return;
    }

    for (const auto& tableName : GetTableNames(sourceDbHandle))
    {
        ProcessTableOnDisk(tableName, sourceDbHandle, upsertStmt, dbPath.filename().string());
    }
    
    sqlite3_finalize(upsertStmt);
    sqlite3_close(sourceDbHandle);
}

void EnvelopeAnalyzer::ProcessTableOnDisk(const std::string& tableName, sqlite3* sourceDbHandle, sqlite3_stmt* upsertStmt, const std::string& sourceDbName)
{
    std::cout << "  - Reading table: '" << tableName << "'" << std::endl;
    
    std::string query = "SELECT * FROM \"" + tableName + "\";";
    sqlite3_stmt* selectStmt;
    if (sqlite3_prepare_v2(sourceDbHandle, query.c_str(), -1, &selectStmt, nullptr) != SQLITE_OK) return;

    int colCount = sqlite3_column_count(selectStmt);
    int elemIdIdx = -1, setNIdx = -1;
    std::vector<std::pair<std::string, int>> reinfCols;
    for (int i = 0; i < colCount; ++i)
    {
        std::string colName = sqlite3_column_name(selectStmt, i);
        if (colName == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;
        else if (colName == config_.SET_N_COLUMN) setNIdx = i;
        else if (colName.rfind("As", 0) == 0) reinfCols.push_back({colName, i});
    }

    if (elemIdIdx == -1 || setNIdx == -1)
    {
        sqlite3_finalize(selectStmt);
        return;
    }

    while (sqlite3_step(selectStmt) == SQLITE_ROW)
    {
        long long elementId = sqlite3_column_int64(selectStmt, elemIdIdx);
        long long setN = sqlite3_column_int64(selectStmt, setNIdx);
        for (const auto& reinfCol : reinfCols)
        {
            sqlite3_bind_int64(upsertStmt, 1, elementId);
            sqlite3_bind_text(upsertStmt, 2, reinfCol.first.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(upsertStmt, 3, sqlite3_column_double(selectStmt, reinfCol.second));
            sqlite3_bind_text(upsertStmt, 4, sourceDbName.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(upsertStmt, 5, tableName.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(upsertStmt, 6, setN);
            sqlite3_step(upsertStmt);
            sqlite3_reset(upsertStmt);
        }
    }
    sqlite3_finalize(selectStmt);
}

void EnvelopeAnalyzer::SaveFinalResultsOnDisk(sqlite3* tempDbHandle, const fs::path& targetPath)
{
    std::cout << "\nWriting final results..." << std::endl;
    
    std::ofstream csvFile(targetPath / config_.OUTPUT_CSV_FILENAME);
    csvFile << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    sqlite3* finalDbHandle;
    sqlite3_open((targetPath / config_.OUTPUT_DB_FILENAME).string().c_str(), &finalDbHandle);
    char* errMsg = nullptr;
    sqlite3_exec(finalDbHandle, "CREATE TABLE IF NOT EXISTS EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);", 0, 0, &errMsg);
    sqlite3_exec(finalDbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);
    sqlite3_stmt* insertStmt;
    sqlite3_prepare_v2(finalDbHandle, "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);", -1, &insertStmt, nullptr);

    sqlite3_stmt* selectStmt;
    sqlite3_prepare_v2(tempDbHandle, "SELECT * FROM IntermediateResults ORDER BY Element_ID, Reinforcement_Type;", -1, &selectStmt, nullptr);

    while (sqlite3_step(selectStmt) == SQLITE_ROW)
    {
        long long elementId = sqlite3_column_int64(selectStmt, 0);
        const char* reinfType = reinterpret_cast<const char*>(sqlite3_column_text(selectStmt, 1));
        double maxValue = sqlite3_column_double(selectStmt, 2);
        const char* sourceDb = reinterpret_cast<const char*>(sqlite3_column_text(selectStmt, 3));
        const char* sourceTable = reinterpret_cast<const char*>(sqlite3_column_text(selectStmt, 4));
        long long sourceSetN = sqlite3_column_int64(selectStmt, 5);

        csvFile << elementId << ";" << reinfType << ";" << maxValue << ";" << sourceDb << ";" << sourceTable << ";" << sourceSetN << "\n";
        
        sqlite3_bind_int64(insertStmt, 1, elementId);
        sqlite3_bind_text(insertStmt, 2, reinfType, -1, SQLITE_STATIC);
        sqlite3_bind_double(insertStmt, 3, maxValue);
        sqlite3_bind_text(insertStmt, 4, sourceDb, -1, SQLITE_STATIC);
        sqlite3_bind_text(insertStmt, 5, sourceTable, -1, SQLITE_STATIC);
        sqlite3_bind_int64(insertStmt, 6, sourceSetN);
        sqlite3_step(insertStmt);
        sqlite3_reset(insertStmt);
    }

    sqlite3_finalize(selectStmt);
    sqlite3_finalize(insertStmt);
    sqlite3_exec(finalDbHandle, "COMMIT;", 0, 0, &errMsg);
    if (errMsg) sqlite3_free(errMsg);
    sqlite3_close(finalDbHandle);

    std::cout << "OK: Results successfully saved." << std::endl;
}

#else
// =================================================================
//      РЕАЛИЗАЦИЯ ДЛЯ РЕЖИМА В ОПЕРАТИВНОЙ ПАМЯТИ (БЫСТРЫЙ)
// =================================================================

void EnvelopeAnalyzer::RunInMemory(const fs::path& targetPath)
{
    std::cout << "\n>> Running in HIGH PERFORMANCE (In-Memory) mode." << std::endl;
    
    int fileCount = 0;
    for (const auto& entry : fs::directory_iterator(targetPath))
    {
        if (IsProcessableDbFile(entry))
        {
            ProcessDatabaseInMemory(entry.path());
            fileCount++;
        }
    }

    if (allMaxResults_.empty())
    {
        std::cout << "\nERROR: No data was collected. Check .db files in the specified directory." << std::endl;
    }
    else
    {
        SaveResultsInMemory(targetPath);
    }
}

void EnvelopeAnalyzer::ProcessDatabaseInMemory(const fs::path& dbPath)
{
    std::cout << "\nProcessing file: " << dbPath.filename().string() << std::endl;
    sqlite3* dbHandle;
    if (sqlite3_open_v2(dbPath.string().c_str(), &dbHandle, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Could not open file", dbHandle);
        sqlite3_close(dbHandle);
        return;
    }

    for (const auto& tableName : GetTableNames(dbHandle))
    {
        ProcessTableInMemory(tableName, dbHandle, dbPath.filename().string());
    }
    sqlite3_close(dbHandle);
}

void EnvelopeAnalyzer::ProcessTableInMemory(const std::string& tableName, sqlite3* dbHandle, const std::string& sourceDbName)
{
    std::cout << "  - Reading table: '" << tableName << "'" << std::endl;
    std::string query = "SELECT * FROM \"" + tableName + "\";";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(dbHandle, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        LogSqliteError("Failed to prepare query", dbHandle);
        return;
    }

    int colCount = sqlite3_column_count(stmt);
    int elemIdIdx = -1, setNIdx = -1;
    std::vector<std::pair<std::string, int>> reinfCols;
    for (int i = 0; i < colCount; ++i)
    {
        std::string colName = sqlite3_column_name(stmt, i);
        if (colName == config_.ELEMENT_ID_COLUMN) elemIdIdx = i;
        else if (colName == config_.SET_N_COLUMN) setNIdx = i;
        else if (colName.rfind("As", 0) == 0) reinfCols.push_back({ colName, i });
    }

    if (elemIdIdx == -1 || setNIdx == -1)
    {
        std::cout << "    WARNING: Skipping table. Missing '" << config_.ELEMENT_ID_COLUMN << "' or '" << config_.SET_N_COLUMN << "' columns." << std::endl;
        sqlite3_finalize(stmt);
        return;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        long long elementId = sqlite3_column_int64(stmt, elemIdIdx);
        long long setN = sqlite3_column_int64(stmt, setNIdx);
        for (const auto& reinfCol : reinfCols)
        {
            const std::string& colName = reinfCol.first;
            int colIdx = reinfCol.second;
            double currentValue = sqlite3_column_double(stmt, colIdx);
            if (allMaxResults_[elementId].find(colName) == allMaxResults_[elementId].end() || currentValue > allMaxResults_[elementId][colName].value)
            {
                allMaxResults_[elementId][colName] = { currentValue, sourceDbName, tableName, setN };
            }
        }
    }
    sqlite3_finalize(stmt);
}

void EnvelopeAnalyzer::SaveResultsInMemory(const fs::path& targetPath)
{
    std::cout << "\nWriting results..." << std::endl;
    std::ofstream csvFile(targetPath / config_.OUTPUT_CSV_FILENAME);
    csvFile << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    sqlite3* dbHandle;
    if (sqlite3_open((targetPath / config_.OUTPUT_DB_FILENAME).string().c_str(), &dbHandle) != SQLITE_OK)
    {
        LogSqliteError("Could not create output database", dbHandle);
        sqlite3_close(dbHandle);
        return;
    }

    char* errMsg = nullptr;
    sqlite3_exec(dbHandle, "CREATE TABLE IF NOT EXISTS EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);", 0, 0, &errMsg);
    sqlite3_exec(dbHandle, "BEGIN TRANSACTION;", 0, 0, &errMsg);

    const char* insertSql = "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);";
    sqlite3_stmt* insertStmt;
    sqlite3_prepare_v2(dbHandle, insertSql, -1, &insertStmt, nullptr);

    std::vector<long long> sortedKeys;
    for (const auto& pair : allMaxResults_) sortedKeys.push_back(pair.first);
    std::sort(sortedKeys.begin(), sortedKeys.end());

    for (const auto& elementId : sortedKeys)
    {
        const auto& reinfMap = allMaxResults_.at(elementId);
        for (const auto& pair : reinfMap)
        {
            const std::string& reinfType = pair.first;
            const ResultInfo& info = pair.second;
            csvFile << elementId << ";" << reinfType << ";" << info.value << ";" << info.source_db << ";" << info.source_table << ";" << info.source_setN << "\n";
            sqlite3_bind_int64(insertStmt, 1, elementId);
            sqlite3_bind_text(insertStmt, 2, reinfType.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(insertStmt, 3, info.value);
            sqlite3_bind_text(insertStmt, 4, info.source_db.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(insertStmt, 5, info.source_table.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_int64(insertStmt, 6, info.source_setN);
            sqlite3_step(insertStmt);
            sqlite3_clear_bindings(insertStmt);
            sqlite3_reset(insertStmt);
        }
    }

    sqlite3_finalize(insertStmt);
    sqlite3_exec(dbHandle, "COMMIT;", 0, 0, &errMsg);
    if (errMsg) sqlite3_free(errMsg);
    sqlite3_close(dbHandle);

    std::cout << "OK: Results successfully saved." << std::endl;
}

#endif

