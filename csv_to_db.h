#pragma once // Защита от двойного включения

#include <filesystem>
#include <string>
#include <vector>
#include <map>

namespace fs = std::filesystem;

/// <summary>
/// Группирует CSV файлы в указанной директории по их префиксу.
/// Ключ карты - префикс (имя будущей БД), значение - список путей к CSV файлам.
/// </summary>
std::map<std::string, std::vector<fs::path>> GroupCsvFilesByPrefix(const fs::path& directory);

/// <summary>
/// Создает одну базу данных из группы CSV файлов.
/// </summary>
void CreateDatabaseFromGroup(const fs::path& targetDir, const std::string& dbName, const std::vector<fs::path>& csvFiles);
