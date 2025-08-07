import os

# --- НАСТРОЙКИ ---
# 1. Укажите путь к вашей папке.
#    Пример для Windows: 'C:\\Users\\User\\Desktop\\MyFolder'
#    Пример для macOS/Linux: '/Users/user/Desktop/MyFolder'
folder_path = './my_folder'  # Используйте './' для папки, где лежит скрипт

# 2. Укажите строку, после которой нужно вставить текст.
target_string = '### INSERT_AFTER_THIS_LINE ###'

# 3. Укажите содержимое, которое нужно вставить.
#    \n в конце добавит перенос строки после вставленного текста.
content_to_insert = 'Это новая строка, которая была успешно вставлена!\n'
# --- КОНЕЦ НАСТРОЕК ---


def process_files_in_directory(directory, target, content):
    """
    Обрабатывает файлы в директории, находит последнюю строку `target`
    и вставляет `content` после неё.
    """
    # Проверяем, существует ли папка
    if not os.path.isdir(directory):
        print(f"Ошибка: Папка '{directory}' не найдена.")
        # Создаем папку для примера, если она не существует
        print(f"Создаю папку '{directory}' для демонстрации.")
        os.makedirs(directory)
        # Создаем тестовые файлы
        with open(os.path.join(directory, 'file1.txt'), 'w', encoding='utf-8') as f:
            f.write("Первая строка\n")
            f.write("Вторая строка\n")
            f.write(f"{target}\n")
            f.write("Последняя строка\n")
        with open(os.path.join(directory, 'file2.txt'), 'w', encoding='utf-8') as f:
            f.write("Какой-то текст\n")
            f.write(f"{target}\n")
            f.write("Еще текст\n")
            f.write(f"{target}\n") # Второе вхождение
        with open(os.path.join(directory, 'file3_no_target.txt'), 'w', encoding='utf-8') as f:
            f.write("В этом файле нет целевой строки.\n")

    # Получаем список всех файлов в директории
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # Убеждаемся, что это файл, а не папка
        if os.path.isfile(file_path):
            print(f"--- Обрабатываю файл: {filename} ---")
            try:
                # Шаг 1: Читаем все строки из файла
                with open(file_path, 'r', encoding='utf-8') as file:
                    lines = file.readlines()

                # Шаг 2: Находим индекс последней строки, содержащей target_string
                last_occurrence_index = -1
                for i in range(len(lines) - 1, -1, -1):
                    if target in lines[i]:
                        last_occurrence_index = i
                        break # Нашли последнее вхождение, выходим из цикла

                # Шаг 3: Если строка найдена, вставляем новый контент
                if last_occurrence_index != -1:
                    lines.insert(last_occurrence_index + 1, content)
                    print(f"Найдено вхождение в строке {last_occurrence_index + 1}. Вставляю содержимое.")

                    # Шаг 4: Перезаписываем файл с обновленным содержимым
                    with open(file_path, 'w', encoding='utf-8') as file:
                        file.writelines(lines)
                    print(f"Файл '{filename}' успешно обновлен.")
                else:
                    print(f"Строка '{target}' не найдена в файле. Пропускаю.")

            except Exception as e:
                print(f"Не удалось обработать файл '{filename}'. Ошибка: {e}")
            print("-" * (len(filename) + 22))


# Запуск основной функции
if __name__ == "__main__":
    process_files_in_directory(folder_path, target_string, content_to_insert)

