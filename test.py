import unittest
import os
import sqlite3
import shutil

# Импортируем тестируемую функцию из нашего основного скрипта
from envelope_analyzer import process_db_files

class TestEnvelopeAnalyzer(unittest.TestCase):

    def setUp(self):
        """
        Эта функция выполняется ПЕРЕД каждым тестом.
        Она создает временную папку и тестовые базы данных.
        """
        self.test_dir = "temp_test_data"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)

        # --- Создаем тестовую базу данных №1 ---
        db1_path = os.path.join(self.test_dir, "test1.db")
        con = sqlite3.connect(db1_path)
        cur = con.cursor()
        # Таблица 1
        cur.execute("CREATE TABLE Slabs (elemId INTEGER, setN INTEGER, Asx REAL, Asy REAL)")
        cur.execute("INSERT INTO Slabs VALUES (101, 5, 150.5, 200.0)")
        cur.execute("INSERT INTO Slabs VALUES (102, 6, 100.0, 300.0)")
        # Таблица 2 (с новым максимумом для elemId=101)
        cur.execute("CREATE TABLE Beams (elemId INTEGER, setN INTEGER, Asx REAL, Asy REAL)")
        cur.execute("INSERT INTO Beams VALUES (101, 8, 180.0, 190.0)")
        con.commit()
        con.close()

        # --- Создаем тестовую базу данных №2 ---
        db2_path = os.path.join(self.test_dir, "test2.db")
        con = sqlite3.connect(db2_path)
        cur = con.cursor()
        # Таблица 1 (с новым максимумом для elemId=102)
        cur.execute("CREATE TABLE Columns (elemId INTEGER, setN INTEGER, Asx REAL, Asy REAL)")
        cur.execute("INSERT INTO Columns VALUES (102, 10, 90.0, 350.5)")
        cur.execute("INSERT INTO Columns VALUES (103, 1, 50.0, 50.0)")
        # Таблица 2 (невалидная, без столбца setN)
        cur.execute("CREATE TABLE InvalidTable (elemId INTEGER, Asx REAL)")
        cur.execute("INSERT INTO InvalidTable VALUES (999, 1000.0)")
        con.commit()
        con.close()

    def tearDown(self):
        """
        Эта функция выполняется ПОСЛЕ каждого теста.
        Она удаляет временную папку и все ее содержимое.
        """
        shutil.rmtree(self.test_dir)

    def test_processing_logic(self):
        """
        Основной тест, проверяющий логику нахождения максимумов.
        """
        # Запускаем нашу основную функцию на тестовых данных
        results = process_db_files(self.test_dir)

        # --- НАЧИНАЕМ ПРОВЕРКИ (ASSERTIONS) ---

        # Проверяем, что результаты вообще есть
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3) # Должно быть найдено 3 элемента: 101, 102, 103

        # --- Проверки для Элемента 101 ---
        self.assertIn(101, results)
        # Проверяем максимум по Asx
        self.assertEqual(results[101]['Asx']['value'], 180.0)
        self.assertEqual(results[101]['Asx']['db'], 'test1.db')
        self.assertEqual(results[101]['Asx']['table'], 'Beams')
        self.assertEqual(results[101]['Asx']['setN'], 8)
        # Проверяем максимум по Asy
        self.assertEqual(results[101]['Asy']['value'], 200.0)
        self.assertEqual(results[101]['Asy']['db'], 'test1.db')
        self.assertEqual(results[101]['Asy']['table'], 'Slabs')
        self.assertEqual(results[101]['Asy']['setN'], 5)

        # --- Проверки для Элемента 102 ---
        self.assertIn(102, results)
        # Проверяем максимум по Asx
        self.assertEqual(results[102]['Asx']['value'], 100.0)
        # Проверяем максимум по Asy
        self.assertEqual(results[102]['Asy']['value'], 350.5)
        self.assertEqual(results[102]['Asy']['db'], 'test2.db')
        self.assertEqual(results[102]['Asy']['table'], 'Columns')
        self.assertEqual(results[102]['Asy']['setN'], 10)
        
        # --- Проверки для Элемента 103 ---
        self.assertIn(103, results)
        self.assertEqual(results[103]['Asx']['value'], 50.0)
        self.assertEqual(results[103]['Asy']['value'], 50.0)
        
        # --- Проверка, что невалидные данные не попали в результат ---
        self.assertNotIn(999, results)

# Этот блок позволяет запустить тесты, просто выполнив файл test_analyzer.py
if __name__ == '__main__':
    unittest.main()


