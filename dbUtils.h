#pragma once // Защита от двойного включения

#include <filesystem>
#include <string>
#include <vector>
#include "sqlite3.h"

namespace fs = std::filesystem;

/// <summary>
/// Получает список имен всех таблиц из открытой базы данных.
/// </summary>
std::vector<std::string> GetTableNames(sqlite3* dbHandle);

/// <summary>
/// Конвертирует одну таблицу из базы данных в CSV файл.
/// </summary>
void ConvertTableToCsv(sqlite3* dbHandle, const std::string& tableName, const fs::path& outputFilePath);

/// <summary>
/// Обрабатывает один DB файл: находит все таблицы и запускает их конвертацию.
/// </summary>
void ProcessDatabaseFile(const fs::path& dbPath, const fs::path& outputDir);

