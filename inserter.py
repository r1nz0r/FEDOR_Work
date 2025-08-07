import os

# --- НАСТРОЙКИ ---
# 1. Укажите путь к вашей папке.
folder_path = './my_folder'

# 2. Укажите строку, после которой нужно вставить текст.
target_string = '### INSERT_AFTER_THIS_LINE ###'

# 3. Укажите содержимое, которое нужно вставить.
#    \n в конце добавит перенос строки после вставленного текста.
content_to_insert = 'Это новая строка, которая была успешно вставлена!\n'
# --- КОНЕЦ НАСТРОЕК ---


def process_files_in_directory(directory, target, content):
    """
    Обрабатывает файлы в директории: находит последнюю строку `target`,
    вставляет `content` после неё, обрабатывает ошибки кодировки и
    предоставляет итоговую статистику.
    """
    if not os.path.isdir(directory):
        print(f"Ошибка: Папка '{directory}' не найдена. Пожалуйста, проверьте путь.")
        return

    # Статистика
    stats = {
        'total': 0,
        'successful': 0,
        'skipped_duplicate': 0,
        'skipped_no_target': 0,
        'failed': 0
    }

    files_to_process = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    stats['total'] = len(files_to_process)

    print(f"Найдено файлов для обработки: {stats['total']}\n")

    for i, filename in enumerate(files_to_process):
        file_path = os.path.join(directory, filename)
        print(f"[{i + 1}/{stats['total']}] Обрабатываю файл: {filename}")

        # --- Шаг 1: Чтение файла с автоопределением кодировки ---
        lines = []
        try:
            # Пытаемся прочитать как UTF-8
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
        except UnicodeDecodeError:
            # Если не вышло, пробуем cp1251 (стандартная для Windows)
            print("  - Предупреждение: не удалось прочитать в UTF-8. Пробую cp1251...")
            try:
                with open(file_path, 'r', encoding='cp1251') as file:
                    lines = file.readlines()
                print("  - Успешно прочитано в cp1251. Файл будет пересохранен в UTF-8.")
            except Exception as e:
                print(f"  - ОШИБКА: Не удалось прочитать файл ни в одной из кодировок. {e}")
                stats['failed'] += 1
                continue # Переходим к следующему файлу
        except Exception as e:
            print(f"  - ОШИБКА: Произошла непредвиденная ошибка при чтении файла. {e}")
            stats['failed'] += 1
            continue

        # --- Шаг 2: Проверка, не была ли строка уже вставлена ---
        file_content_str = "".join(lines)
        if content in file_content_str:
            print("  - Пропущено: контент для вставки уже существует в файле.")
            stats['skipped_duplicate'] += 1
            continue

        # --- Шаг 3: Поиск последней строки для вставки ---
        last_occurrence_index = -1
        for j in range(len(lines) - 1, -1, -1):
            if target in lines[j]:
                last_occurrence_index = j
                break

        if last_occurrence_index != -1:
            # --- Шаг 4: Вставка и перезапись файла в UTF-8 ---
            lines.insert(last_occurrence_index + 1, content)
            try:
                # Всегда сохраняем в UTF-8 для стандартизации
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.writelines(lines)
                print(f"  - Успех: Содержимое вставлено после строки {last_occurrence_index + 1}.")
                stats['successful'] += 1
            except Exception as e:
                print(f"  - ОШИБКА: Не удалось записать изменения в файл. {e}")
                stats['failed'] += 1
        else:
            print("  - Пропущено: целевая строка для вставки не найдена.")
            stats['skipped_no_target'] += 1

    # --- Итоговая статистика ---
    print("\n" + "="*30)
    print("      ОБРАБОТКА ЗАВЕРШЕНА")
    print("="*30)
    print(f"Всего файлов обработано: {stats['total']}")
    print(f"✅ Успешно обновлено:      {stats['successful']}")
    print(f"⏭️  Пропущено (уже есть):  {stats['skipped_duplicate']}")
    print(f"⏭️  Пропущено (нет цели):  {stats['skipped_no_target']}")
    print(f"❌ Ошибок обработки:        {stats['failed']}")
    print("="*30)


# Запуск основной функции
if __name__ == "__main__":
    process_files_in_directory(folder_path, target_string, content_to_insert)

