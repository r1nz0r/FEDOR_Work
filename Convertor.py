import sqlite3
import csv
import os

# --- НАСТРОЙКИ ---
# Имя вашего файла базы данных
db_filename = 'your_database.db' 
# Имя таблицы внутри базы данных, которую нужно конвертировать.
# Если вы не знаете имя таблицы, оставьте '' пустым.
table_name = '' 
# Имя файла, в который будут сохранены данные
csv_filename = 'output.csv'
# -----------------

# Проверяем, существует ли файл базы данных
if not os.path.exists(db_filename):
    print(f"Ошибка: Файл '{db_filename}' не найден.")
else:
    try:
        # Подключаемся к базе данных
        con = sqlite3.connect(db_filename)
        cursor = con.cursor()

        # Если имя таблицы не указано, выводим список всех таблиц
        if not table_name:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            print("Имя таблицы не указано. Найдены следующие таблицы:")
            if not tables:
                print("- В базе данных нет таблиц.")
            else:
                for table in tables:
                    print(f"- {table[0]}")
            print("\nПожалуйста, укажите одну из этих таблиц в переменной 'table_name' в скрипте и запустите его снова.")
        else:
            # Выполняем запрос на выборку всех данных из указанной таблицы
            cursor.execute(f"SELECT * FROM {table_name}")
            
            # Получаем заголовки столбцов
            headers = [description[0] for description in cursor.description]
            
            # Открываем CSV файл для записи
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Записываем заголовки
                writer.writerow(headers)
                
                # Записываем все строки данных
                writer.writerows(cursor.fetchall())

            print(f"Успешно! Данные из таблицы '{table_name}' сохранены в файл '{csv_filename}'.")

    except sqlite3.Error as error:
        print(f"Произошла ошибка при работе с SQLite: {error}")
    finally:
        # Закрываем соединение
        if con:
            con.close()

