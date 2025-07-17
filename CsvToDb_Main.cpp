#include <iostream>
#include <string>
#include <filesystem>
#include "DbUtils.h" // Подключаем наши утилиты

namespace fs = std::filesystem;

int main()
{
    std::cout << "--- CSV to DB Converter ---" << std::endl;

    std::cout << "Enter path to directory with .csv files (or '.' for current directory): ";
    std::string inputPathStr;
    std::getline(std::cin, inputPathStr);

    fs::path targetPath = inputPathStr.empty() || inputPathStr == "." ? "." : inputPathStr;

    if (!fs::exists(targetPath) || !fs::is_directory(targetPath))
    {
        std::cerr << "ERROR: Path does not exist or is not a directory: " << inputPathStr << std::endl;
        return 1;
    }

    try
    {
        // 1. Группируем все CSV файлы по префиксам
        auto fileGroups = GroupCsvFilesByPrefix(targetPath);

        if (fileGroups.empty())
        {
            std::cout << "No suitable .csv files found to process." << std::endl;
        }
        else
        {
            // 2. Для каждой группы создаем свою базу данных
            for (const auto& group : fileGroups)
            {
                CreateDatabaseFromGroup(targetPath, group.first, group.second);
            }
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "An unexpected error occurred: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\nConversion complete!" << std::endl;
    std::cout << "Press Enter to exit...";
    std::cin.get();
    return 0;
}
