#include <iostream>
#include <string>
#include <filesystem>
#include "DbUtils.h" // Подключаем наши утилиты

namespace fs = std::filesystem;

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
        if (!fs::exists(outputDir))
        {
            fs::create_directory(outputDir);
            std::cout << "Created output directory: " << outputDir.string() << std::endl;
        }
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

