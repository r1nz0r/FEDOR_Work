import sqlite3
import csv
import os
import re

# --- НАСТРОЙКА ---
# Укажите имя вашего файла базы данных
db_filename = 'your_database.db' 
# -----------------

def sanitize_filename(name):
    """
    Удаляет недопустимые символы из имени файла 
    и заменяет пробелы на подчеркивания.
    """
    name = name.replace(' ', '_')
    return re.sub(r'[\\/*?:"<>|]', "", name)

# Проверяем, существует ли файл базы данных
if not os.path.exists(db_filename):
    print(f"❌ Ошибка: Файл '{db_filename}' не найден.")
else:
    try:
        # Подключаемся к базе данных
        con = sqlite3.connect(db_filename)
        cursor = con.cursor()

        # 1. Получаем список всех таблиц в базе данных
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            print("ℹ️ В базе данных не найдено таблиц.")
        else:
            print(f"✅ Найдено таблиц: {len(tables)}. Начинаю конвертацию...")
            
            # 2. Проходим в цикле по каждой найденной таблице
            for table in tables:
                table_name = table[0]
                
                # Создаем безопасное имя для CSV файла
                csv_filename = f"{sanitize_filename(table_name)}.csv"
                
                print(f"   - Конвертирую таблицу '{table_name}' в файл '{csv_filename}'...", end='')

                try:
                    table_cursor = con.cursor()
                    # 3. Экранируем имя таблицы двойными кавычками для запроса
                    table_cursor.execute(f'SELECT * FROM "{table_name}"')
                    
                    # Получаем заголовки столбцов
                    headers = [description[0] for description in table_cursor.description]
                    
                    # Открываем CSV файл для записи
                    with open(csv_filename, 'w', newline='', encoding='cp1251') as f:
                        # 4. Указываем разделитель ';'
                        writer = csv.writer(f, delimiter=';')
                        
                        # Записываем заголовки
                        writer.writerow(headers)
                        
                        # Записываем все строки данных
                        writer.writerows(table_cursor.fetchall())
                    
                    print(" Готово.")

                except sqlite3.Error as table_error:
                    print(f" Ошибка при обработке таблицы: {table_error}")

            print("\n🎉 Все таблицы успешно сконвертированы!")

    except sqlite3.Error as error:
        print(f"❌ Произошла ошибка при работе с SQLite: {error}")
    finally:
        # Закрываем соединение
        if 'con' in locals() and con:
            con.close()
