Это консольное приложение .NET 8, использующее инструкции верхнего уровня (top-level statement feature).

Для тестирования программы я использовал набор данных 2019-Nov.csv, доступный по ссылке (https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), как и требовалось в техническом задании.

Результатом выполнения этого приложения является:
- сумма выручки
- наиболее популярный бренд
- самая популярная категория
- самый популярный товар

Приложение использует асинхронную обработку данных, благодаря чему мне удалось добиться высокой скорости (~25-30 МБ/с) обработки данных "на лету", без чтения всего файла целиком. Благодаря этому программа использует около 30-40 МБ оперативной памяти, а это значит, что моя программа совсем не требовательна для конечного пользователя.

Также реализована возможность остановки обработки набора данных путём ввода команды "stop" в консоль. Это сделано для обеспечения полного контроля пользователем над происходящим.

Обработка данных происходит вне зависимости от последовательности столбцов благодаря использованию библиотеки CsvHelper.

Во время процесса обработки набора данных приложение каждую секунду предоставляет полезную информацию пользователю о прогрессе обработки данных:
- размер обработанной информации
- полный размер файла
- скорость обработки данных
- процент выполнения задачи
