import sqlite3
import os
import csv

# --- ГЛАВНЫЕ НАСТРОЙКИ ---
# Папка, в которой лежат ваши .db файлы
DIRECTORY_PATH = '.'  # '.' означает 'текущая папка'

# Имя столбца, по которому идентифицируются элементы
ELEMENT_ID_COLUMN = 'elemId'

# Имя столбца с номером комбинации (сетом)
SET_N_COLUMN = 'setN'

# Имена итоговых файлов
OUTPUT_CSV_FILENAME = 'Enveloped_Reinforcement_Analysis.csv'
OUTPUT_DB_FILENAME = 'Enveloped_Reinforcement_Analysis.db'
# --- КОНЕЦ НАСТРОЕК ---


def process_db_files(directory):
    """
    Основная функция для обработки .db файлов и поиска максимумов.
    Использует только стандартную библиотеку sqlite3.
    """
    max_reinforcement_data = {}
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    
    print(f"OK: Found {len(db_files)} .db files to process.")

    for db_file in db_files:
        print(f"\nProcessing file: {db_file}")
        file_path = os.path.join(directory, db_file)
        try:
            con = sqlite3.connect(file_path)
            cursor = con.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            for table_name in tables:
                print(f"  - Reading table: '{table_name}'")
                
                cursor.execute(f'SELECT * FROM "{table_name}"')
                
                headers = [description[0] for description in cursor.description]
                col_to_idx = {name: i for i, name in enumerate(headers)}

                if ELEMENT_ID_COLUMN not in col_to_idx or SET_N_COLUMN not in col_to_idx:
                    print(f"    WARNING: Skipping table. Missing '{ELEMENT_ID_COLUMN}' or '{SET_N_COLUMN}'.")
                    continue

                reinf_cols_map = {name: i for name, i in col_to_idx.items() if name.startswith('As')}
                if not reinf_cols_map:
                    print(f"    WARNING: Skipping table. No reinforcement columns ('As...') found.")
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
            print(f"  ERROR processing file {db_file}: {e}")
        finally:
            if 'con' in locals():
                con.close()
                
    return max_reinforcement_data

def prepare_results_list(data):
    """Преобразует словарь с результатами в плоский список для записи."""
    results = []
    # Сортируем ключи, чтобы обеспечить предсказуемый порядок вывода
    for element_id in sorted(data.keys()):
        for reinf_type in sorted(data[element_id].keys()):
            details = data[element_id][reinf_type]
            results.append([
                element_id,
                reinf_type,
                details['value'],
                details['db'],
                details['table'],
                details['setN']
            ])
    return results

def save_to_csv(results_list, header):
    """Сохраняет итоговые данные в CSV файл."""
    with open(OUTPUT_CSV_FILENAME, 'w', newline='', encoding='cp1251') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)
        writer.writerows(results_list)
    print(f"\nOK: Results successfully saved to CSV file: {OUTPUT_CSV_FILENAME}")

def save_to_db(results_list):
    """Сохраняет итоговые данные в новый DB файл с корректными типами данных."""
    if os.path.exists(OUTPUT_DB_FILENAME):
        os.remove(OUTPUT_DB_FILENAME)
        
    con = sqlite3.connect(OUTPUT_DB_FILENAME)
    cur = con.cursor()
    
    table_name = "EnvelopedReinforcement"
    
    # --- ГЛАВНОЕ ИЗМЕНЕНИЕ ---
    # Создаем таблицу с правильными типами данных для корректной сортировки
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Element_ID          INTEGER,
        Reinforcement_Type  TEXT,
        Max_Value           REAL,
        Source_DB           TEXT,
        Source_Table        TEXT,
        Source_SetN         INTEGER
    );
    """
    cur.execute(create_table_sql)

    # Вставляем данные
    insert_sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)"
    cur.executemany(insert_sql, results_list)
    
    con.commit()
    con.close()
    print(f"OK: Results successfully saved to DB file: {OUTPUT_DB_FILENAME}")


# --- ГЛАВНЫЙ БЛОК ЗАПУСКА ---
if __name__ == "__main__":
    print("--- Reinforcement Envelope Analyzer (Python In-Memory Version) ---")
    
    final_data_dict = process_db_files(DIRECTORY_PATH)
    
    if not final_data_dict:
        print("\nERROR: No data was collected. Check .db files in the directory.")
    else:
        header = ['Element_ID', 'Reinforcement_Type', 'Max_Value', 'Source_DB', 'Source_Table', 'Source_SetN']
        results = prepare_results_list(final_data_dict)
        
        save_to_csv(results, header)
        save_to_db(results)
        
        print("\nAnalysis complete!")

