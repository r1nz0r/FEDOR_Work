import sqlite3
import os
import csv

# --- ГЛАВНЫЕ НАСТРОЙКИ ---
ELEMENT_ID_COLUMN = 'elemId'
SET_N_COLUMN = 'setN'
OUTPUT_CSV_FILENAME = 'Enveloped_Reinforcement_Analysis.csv'
OUTPUT_DB_FILENAME = 'Enveloped_Reinforcement_Analysis.db'

def process_db_files(directory):
    """
    Основная функция для обработки .db файлов и поиска максимумов.
    Использует только стандартную библиотеку sqlite3.
    """
    max_reinforcement_data = {}
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    
    print(f"✅ Найдено {len(db_files)} .db файлов для обработки в '{directory}'.")

    for db_file in db_files:
        print(f"\n🔄 Обрабатывается файл: {db_file}")
        file_path = os.path.join(directory, db_file)
        try:
            con = sqlite3.connect(file_path)
            cursor = con.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            for table_name in tables:
                print(f"  - Чтение таблицы: '{table_name}'")
                cursor.execute(f'SELECT * FROM "{table_name}"')
                
                headers = [description[0] for description in cursor.description]
                col_to_idx = {name: i for i, name in enumerate(headers)}

                if ELEMENT_ID_COLUMN not in col_to_idx or SET_N_COLUMN not in col_to_idx:
                    print(f"    ⚠️ Пропуск: в таблице нет столбцов '{ELEMENT_ID_COLUMN}' или '{SET_N_COLUMN}'.")
                    continue

                reinf_cols_map = {name: i for name, i in col_to_idx.items() if name.startswith('As')}
                if not reinf_cols_map:
                    print(f"    ⚠️ Пропуск: в таблице нет столбцов с армированием ('As...').")
                    continue
                
                for row in cursor.fetchall():
                    element_id = row[col_to_idx[ELEMENT_ID_COLUMN]]
                    if element_id not in max_reinforcement_data:
                        max_reinforcement_data[element_id] = {}

                    for col_name, col_idx in reinf_cols_map.items():
                        current_value = row[col_idx]
                        
                        if col_name not in max_reinforcement_data[element_id] or current_value > max_reinforcement_data[element_id][col_name]['value']:
                            max_reinforcement_data[element_id][col_name] = {
                                'value': current_value,
                                'db': db_file,
                                'table': table_name,
                                'setN': row[col_to_idx[SET_N_COLUMN]]
                            }
        except Exception as e:
            print(f"  ❌ Ошибка при обработке файла {db_file}: {e}")
        finally:
            if 'con' in locals():
                con.close()
                
    return max_reinforcement_data

def prepare_results_list(data):
    """Преобразует словарь с результатами в плоский список для записи."""
    results = []
    for element_id in sorted(data.keys()):
        for reinf_type, details in data[element_id].items():
            results.append([
                element_id,
                reinf_type,
                details['value'],
                details['db'],
                details['table'],
                details['setN']
            ])
    return results

def save_to_csv(results_list, header, filename):
    """Сохраняет итоговые данные в CSV файл."""
    with open(filename, 'w', newline='', encoding='cp1251') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)
        writer.writerows(results_list)
    print(f"\n✅ Результаты успешно сохранены в CSV файл: {filename}")

def save_to_db(results_list, header, filename):
    """Сохраняет итоговые данные в новый DB файл."""
    if os.path.exists(filename):
        os.remove(filename)
    con = sqlite3.connect(filename)
    cur = con.cursor()
    table_name = "EnvelopedReinforcement"
    safe_header = [h.replace(' ', '_').replace(',', '') for h in header]
    columns_def = ", ".join([f'"{col}" TEXT' for col in safe_header])
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})")
    placeholders = ", ".join(["?"] * len(header))
    cur.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", results_list)
    con.commit()
    con.close()
    print(f"✅ Результаты успешно сохранены в DB файл: {filename} (таблица: {table_name})")

def main():
    """Основная исполняющая функция."""
    print("--- Запуск анализатора огибающей армирования ---")
    
    final_data_dict = process_db_files('.')
    
    if not final_data_dict:
        print("\n❌ Не удалось собрать данные. Проверьте .db файлы в папке.")
    else:
        header = ['Element_ID', 'Reinforcement_Type', 'Max_Value', 'Source_DB', 'Source_Table', 'Source_SetN']
        results = prepare_results_list(final_data_dict)
        
        save_to_csv(results, header, OUTPUT_CSV_FILENAME)
        save_to_db(results, header, OUTPUT_DB_FILENAME)
        
        print("\n🎉 Анализ завершен!")

# Этот блок гарантирует, что main() вызывается только при запуске файла напрямую
if __name__ == "__main__":
    main()

