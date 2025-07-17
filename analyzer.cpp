#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem> // Для работы с файловой системой (C++17)
#include <fstream>    // Для записи в файл

// Подключаем удобную обертку для SQLite.
// Этот файл должен лежать в той же папке.
#include "sqlite_modern_cpp.h"

namespace fs = std::filesystem;

// --- ГЛАВНЫЕ НАСТРОЙКИ ---
const std::string ELEMENT_ID_COLUMN = "elemId";
const std::string SET_N_COLUMN = "setN";
const std::string OUTPUT_CSV_FILENAME = "Enveloped_Reinforcement_Analysis.csv";
const std::string OUTPUT_DB_FILENAME = "Enveloped_Reinforcement_Analysis.db";
// --------------------------

// Структура для хранения информации об источнике максимального значения
struct ResultInfo {
    double value = 0.0;
    std::string source_db;
    std::string source_table;
    long long source_setN = 0;
};

// Основной тип для хранения данных: { elementId -> { reinfType -> ResultInfo } }
using MaxResultsMap = std::unordered_map<long long, std::unordered_map<std::string, ResultInfo>>;

void processDatabase(const fs::path& db_path, MaxResultsMap& results) {
    std::cout << "\n🔄 Обрабатывается файл: " << db_path.filename().string() << std::endl;
    try {
        sqlite::database db(db_path.string());

        // 1. Получаем список всех таблиц
        std::vector<std::string> table_names;
        db << "SELECT name FROM sqlite_master WHERE type='table';"
           >> [&](std::string name) {
               table_names.push_back(name);
           };

        for (const auto& table_name : table_names) {
            std::cout << "  - Чтение таблицы: '" << table_name << "'" << std::endl;
            
            // 2. Читаем данные из таблицы
            db << "SELECT * FROM \"" + table_name + "\";"
               >> [&](sqlite::query_result::row_type row) {
                   
                   // Получаем доступ к данным по именам столбцов
                   try {
                       long long element_id = row.get<long long>(ELEMENT_ID_COLUMN);
                       long long set_n = row.get<long long>(SET_N_COLUMN);

                       // Итерируемся по всем столбцам в строке
                       for(const auto& field : row) {
                           std::string col_name = field.first;
                           if (col_name.rfind("As", 0) == 0) { // Проверяем, начинается ли имя столбца с "As"
                               double current_value = field.second.get<double>();

                               // Проверяем и обновляем максимум
                               if (results[element_id].find(col_name) == results[element_id].end() || current_value > results[element_id][col_name].value) {
                                   results[element_id][col_name] = {current_value, db_path.filename().string(), table_name, set_n};
                               }
                           }
                       }
                   } catch (const std::exception& e) {
                       // Пропускаем строки, где нет нужных столбцов или типы не совпадают
                   }
               };
        }
    } catch (const std::exception& e) {
        std::cerr << "  ❌ Ошибка при обработке файла " << db_path.string() << ": " << e.what() << std::endl;
    }
}

void saveResults(const MaxResultsMap& results) {
    std::cout << "\n✍️ Запись результатов..." << std::endl;
    
    // --- Сохранение в CSV ---
    std::ofstream csv_file(OUTPUT_CSV_FILENAME);
    // Для корректного отображения кириллицы в Excel на Windows
    // csv_file.imbue(std::locale("ru_RU.CP1251")); 
    csv_file << "Element_ID;Reinforcement_Type;Max_Value;Source_DB;Source_Table;Source_SetN\n";

    // --- Сохранение в DB ---
    if (fs::exists(OUTPUT_DB_FILENAME)) {
        fs::remove(OUTPUT_DB_FILENAME);
    }
    sqlite::database db(OUTPUT_DB_FILENAME);
    db << "CREATE TABLE EnvelopedReinforcement (Element_ID INTEGER, Reinforcement_Type TEXT, Max_Value REAL, Source_DB TEXT, Source_Table TEXT, Source_SetN INTEGER);";
    auto transaction = db.transaction();

    // Сортируем по ID элемента для наглядности (собираем ключи и сортируем)
    std::vector<long long> sorted_keys;
    for(const auto& pair : results) sorted_keys.push_back(pair.first);
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (const auto& element_id : sorted_keys) {
        const auto& reinf_map = results.at(element_id);
        for (const auto& pair : reinf_map) {
            const std::string& reinf_type = pair.first;
            const ResultInfo& info = pair.second;

            // Запись в CSV
            csv_file << element_id << ";"
                     << reinf_type << ";"
                     << info.value << ";"
                     << info.source_db << ";"
                     << info.source_table << ";"
                     << info.source_setN << "\n";
            
            // Запись в DB
            db << "INSERT INTO EnvelopedReinforcement VALUES (?, ?, ?, ?, ?, ?);"
               << element_id << reinf_type << info.value << info.source_db << info.source_table << info.source_setN;
        }
    }

    transaction.commit();
    csv_file.close();

    std::cout << "✅ Результаты успешно сохранены в " << OUTPUT_CSV_FILENAME << " и " << OUTPUT_DB_FILENAME << std::endl;
}

int main() {
    // Устанавливаем русскую локаль для корректного вывода в консоль Windows
    #ifdef _WIN32
        std::system("chcp 65001 > nul");
    #endif
    
    std::cout << "--- Запуск анализатора огибающей армирования (C++ версия) ---" << std::endl;
    
    MaxResultsMap all_max_results;
    const fs::path current_path(".");

    for (const auto& entry : fs::directory_iterator(current_path)) {
        if (entry.is_regular_file() && entry.path().extension() == ".db") {
            // Исключаем наш собственный выходной файл
            if (entry.path().filename().string() != OUTPUT_DB_FILENAME) {
                processDatabase(entry.path(), all_max_results);
            }
        }
    }

    if (all_max_results.empty()) {
        std::cout << "\n❌ Не удалось собрать данные. Проверьте .db файлы в папке." << std::endl;
    } else {
        saveResults(all_max_results);
    }

    std::cout << "\n🎉 Анализ завершен!" << std::endl;
    return 0;
}

