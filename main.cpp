#include "EnvelopeAnalyzer.h" // Подключаем наш класс
#include <iostream>

int main()
{
    // Устанавливаем кодировку консоли для корректного отображения вывода
#ifdef _WIN32
    // Эта команда нужна для корректной работы std::cout с некоторыми системами
    std::system("chcp 65001 > nul");
#endif

    try
    {
        // Создаем экземпляр нашего анализатора
        EnvelopeAnalyzer analyzer;
        // Запускаем его
        analyzer.Run();
    }
    catch (const std::exception& e)
    {
        // Ловим любые непредвиденные критические ошибки
        std::cerr << "An unexpected critical error occurred: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

