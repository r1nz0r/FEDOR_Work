import sys

# --- НАСТРОЙКИ ---
INPUT_FILENAME = "sqlite3.def"
OUTPUT_FILENAME = "sqlite3.clean.def"
# -----------------

def clean_def_file_paranoid():
    """
    Финальная, "параноидальная" версия.
    Создает .def файл в максимально простом формате ASCII с Windows-переносами строк.
    """
    print(f"Читаю исходный файл: {INPUT_FILENAME}...")
    
    try:
        with open(INPUT_FILENAME, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"ОШИБКА: Файл '{INPUT_FILENAME}' не найден.")
        sys.exit(1)

    function_names = []
    in_exports_section = False

    for line in lines:
        if "ordinal hint RVA" in line:
            in_exports_section = True
            continue
        
        if in_exports_section and ("Summary" in line or not line.strip()):
            break

        if in_exports_section:
            parts = line.split()
            if len(parts) >= 4:
                function_name = parts[-1]
                if function_name.startswith("sqlite3_"):
                    function_names.append(function_name)

    if not function_names:
        print("\nОШИБКА: Не удалось найти имена функций в файле!")
        sys.exit(1)

    print(f"Найдено функций: {len(function_names)}. Создаю чистый файл: {OUTPUT_FILENAME}...")

    # --- САМОЕ ВАЖНОЕ ИЗМЕНЕНИЕ ---
    # Открываем файл с явным указанием кодировки ASCII и переносов строк для Windows
    try:
        with open(OUTPUT_FILENAME, 'w', encoding='ascii', newline='\r\n') as f:
            f.write("EXPORTS\n")
            for name in sorted(function_names):
                f.write(f"{name}\n")
    except UnicodeEncodeError as e:
        print(f"\nОШИБКА КОДИРОВКИ: В имени функции найден не-ASCII символ: {e}")
        print("Это очень странно и не должно происходить с файлом sqlite3.dll.")
        sys.exit(1)

    print("Готово! Файл успешно создан в формате, который точно поймет lib.exe.")

if __name__ == "__main__":
    clean_def_file_paranoid()

