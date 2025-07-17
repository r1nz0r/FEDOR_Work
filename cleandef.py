import sys

# --- НАСТРОЙКИ ---
# Имя исходного .def файла, созданного dumpbin
INPUT_FILENAME = "sqlite3.def"
# Имя итогового, "чистого" .def файла
OUTPUT_FILENAME = "sqlite3.clean.def"
# -----------------

def clean_def_file():
    """
    Читает "грязный" .def файл, извлекает имена функций
    и создает "чистый" .def файл, готовый для утилиты lib.
    """
    print(f"Читаю исходный файл: {INPUT_FILENAME}...")
    
    try:
        with open(INPUT_FILENAME, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"ОШИБКА: Файл '{INPUT_FILENAME}' не найден. Убедись, что он лежит в этой же папке.")
        sys.exit(1)

    function_names = []
    parsing_started = False

    for line in lines:
        # Ищем начало списка функций
        if "ordinal hint RVA" in line:
            parsing_started = True
            continue # Пропускаем саму строку-заголовок

        # Ищем конец списка
        if "Summary" in line:
            break

        # Если мы находимся внутри списка и строка не пустая
        if parsing_started and line.strip():
            # Разбиваем строку по пробелам и берем последнее слово - это и есть имя функции
            parts = line.split()
            if len(parts) > 0:
                function_name = parts[-1]
                # Иногда dumpbin добавляет странные символы, отсекаем их
                if function_name.isidentifier():
                     function_names.append(function_name)

    if not function_names:
        print("ОШИБКА: Не удалось найти имена функций в файле. Проверьте содержимое " + INPUT_FILENAME)
        sys.exit(1)

    print(f"Найдено функций: {len(function_names)}. Создаю чистый файл: {OUTPUT_FILENAME}...")

    # Записываем результат в новый файл
    with open(OUTPUT_FILENAME, 'w') as f:
        f.write("EXPORTS\n")
        for name in function_names:
            f.write(name + "\n")

    print("Готово! Файл успешно очищен.")

if __name__ == "__main__":
    clean_def_file()


