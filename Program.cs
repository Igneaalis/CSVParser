using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using CsvHelper;

// Массив суффиксов для обозначения размеров файлов
string[] SizeSuffixes = { "bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" };

// Функция для преобразования размера файла в удобочитаемый формат с использованием подходящего суффикса
string SizeSuffix(long value, int decimalPlaces = 2)
{
    if (decimalPlaces < 0) { throw new ArgumentOutOfRangeException("decimalPlaces"); }
    if (value < 0) { return "-" + SizeSuffix(-value, decimalPlaces); } 
    if (value == 0) { return string.Format("{0:n" + decimalPlaces + "} bytes", 0); }

    // Определение магнитуды размера
    int mag = (int)Math.Log(value, 1024);
    decimal adjustedSize = (decimal)value / (1L << (mag * 10));

    // Корректировка при значении, округляющемся до 1000 и более
    if (Math.Round(adjustedSize, decimalPlaces) >= 1000)
    {
        mag += 1;
        adjustedSize /= 1024;
    }

    return string.Format("{0:n" + decimalPlaces + "} {1}", 
        adjustedSize, 
        SizeSuffixes[mag]);
}

// Получение абсолютного пути к папке с исполняемым файлом
string exePath = AppDomain.CurrentDomain.BaseDirectory;

// Построение пути к корневой папке проекта, предполагая стандартную структуру .NET проектов
string projectRootPath = Path.GetFullPath(Path.Combine(exePath, @"..\..\..\"));

// Строим путь к нужному файлу данных
string datasetPath = Path.Combine(projectRootPath, "2019-Nov.csv");

// Инициализация переменных для хранения агрегированных данных
decimal revenue = 0; // Выручка
(string? name, int count) mostPopularBrand = new (null, 0); // Самый популярный бренд
var brands = new ConcurrentDictionary<string, int>();
(string? id, string? code, int count) mostPopularCategory = new (null, null, 0); // Самая популярная категория
var categories = new ConcurrentDictionary<string, int>();
(string? id, int count) mostPopularProduct = new (null, 0); // Самый популярный товар
var products = new ConcurrentDictionary<string, int>();

// Объекты блокировки для безопасности потоков
var recordLock = new Object();
var recordProgessLock = new Object();

List<Task> tasks = new List<Task>(); // Список задач для асинхронной обработки

try
{
    using var reader = new StreamReader(datasetPath);
    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

    // Инициализация переменных для чтения и обработки данных
    var recordsBatch = new List<dynamic>();
    int recordCount = 0;
    int recordProgressCount = 0;
    int batchSize = 128; // Размер батча
    int limiter = 1000000000; // Лимит обработки батчей (можно удалить, если предполагается чтение всего csv файла)
    long recordsBatchSize = 0;
    double averageRecordSize = 0;
    long averageSpeed = 0;
    DateTime startTime = DateTime.Now;
    long lastProgressSize = 0;
    long estimatedRecords = 0;
    long estimatedFileSize = 0;

    // Получение размера файла
    var fileInfo = new FileInfo(datasetPath);
    long fileSizeInBytes = fileInfo.Length;

    // Завершена ли обработка всего CSV файла?
    bool IsDone = false;

    // Задача для отображения прогресса обработки данных
    var progressTask = Task.Run(async () =>
    {
        while (!IsDone)
        {
            try
            {
                long currentProgressSize = (long)(recordProgressCount * averageRecordSize);
                long currentSpeed = currentProgressSize - lastProgressSize;
                TimeSpan timeSpent = TimeSpan.FromSeconds((DateTime.Now - startTime).TotalSeconds);
                averageSpeed = (long)(currentProgressSize / timeSpent.TotalSeconds);
                TimeSpan estimatedFinishTime = TimeSpan.FromSeconds(estimatedFileSize / averageSpeed);
                lastProgressSize = currentProgressSize;

                Console.WriteLine(
                    $"{SizeSuffix(currentProgressSize)} / {SizeSuffix(estimatedFileSize)}\t" +
                    $"{SizeSuffix(Math.Max(averageSpeed, 0))}/s\t" + 
                    ((double)recordProgressCount / estimatedRecords).ToString("0.00%\t") + 
                    timeSpent.ToString("hh\\:mm\\:ss") + "/" + estimatedFinishTime.ToString("hh\\:mm\\:ss"));
            }
            catch (System.DivideByZeroException)
            {
                Console.WriteLine($"0 bytes / ? bytes\t0.00 bytes/s\t" + "0.00%");
            }
            await Task.Delay(1000);
        }
    });

    // Задача для экстренной остановки обработки
    var abortTask = Task.Run(() =>
    {
        if (Console.ReadLine() == "stop")
        {
            System.Environment.Exit(-1);
        };
    });
    
    // Чтение и обработка данных из CSV файла
    while (csv.Read() && limiter > 0)
    {
        var record = csv.GetRecord<dynamic>();

        // Расчёт размера записи для оценки прогресса
        int recordSize = Encoding.UTF8.GetByteCount(record.event_time + record.event_type + record.product_id + record.category_id + record.category_code + record.brand + record.price + record.user_id + record.user_session);
        recordsBatchSize += recordSize;
        if (averageRecordSize == 0)
        {
            averageRecordSize = recordSize;
        }

        recordsBatch.Add(record);
        recordCount++;

        // Обработка данных после накопления batchSize записей
        if (recordCount % batchSize == 0)
        {
            var recordsToProcess = new List<dynamic>(recordsBatch);

            // Оценка среднего размера одной записи
            averageRecordSize = ((double)recordsBatchSize / batchSize + averageRecordSize) / 2;

            // Оценка приблизительного количества записей во всем CSV файле
            estimatedRecords = (long)(fileSizeInBytes / averageRecordSize);

            // Оценка размера CSV файла
            if (estimatedFileSize == 0 ) {
                estimatedFileSize = (long)(estimatedRecords * averageRecordSize);
            }

            // Обработка набора (batch) записей
            tasks.Add(Task.Run(() => ProcessRecords(recordsToProcess)).ContinueWith(_ =>
            {
               recordProgressCount += batchSize;
            }));

            recordsBatch.Clear();
            recordsBatchSize = 0;
            limiter--;
        }
    }

    // Обработка оставшихся записей после выхода из цикла
    if (recordsBatch.Count > 0)
    {
        tasks.Add(Task.Run(() => ProcessRecords(recordsBatch)).ContinueWith(_ =>
           {
               recordProgressCount++;
           }));
    }

    // Ожидание завершения всех задач обработки и вывод интересуемой информации
    await Task.WhenAll(tasks).ContinueWith(_ =>
    {
        IsDone = true;
        Console.WriteLine($"Выручка: {revenue}, Наиболее популярный бренд: {mostPopularBrand.name}, Наиболее популярная категория: {mostPopularCategory.code}, Наиболее популярный товар: {mostPopularProduct.id}");
    });
}
catch (Exception ex)
{
    Console.WriteLine($"Произошла ошибка: {ex.Message}");
}

// Функция для обработки записей
void ProcessRecords(List<dynamic> records)
{
    // Сумма выручки, частота появления брендов, категорий и товаров в наборе (batch) записей
    decimal revenueBatch = 0;
    var brandsBatch = new Dictionary<string, int>();
    var categoriesBatch = new Dictionary<string, int>();
    var productsBatch = new Dictionary<string, int>();

    foreach (var record in records)
    {
        // Обработка каждой записи, например, подсчёт выручки и популярности брендов

        revenueBatch += decimal.Parse(record.price, new NumberFormatInfo() { NumberDecimalSeparator = "." });

        if (record.brand != "")
        {
            UpdateDictionary(brandsBatch, record.brand);
        }
        if (record.category_id != "")
        {
            string categoryKey = $"{record.category_id}::{record.category_code}";
            UpdateDictionary(categoriesBatch, categoryKey);
        }

        if (record.product_id != "")
        {
            UpdateDictionary(productsBatch, record.product_id);
        }
    }

    // Блокировка для безопасного обновления глобальных переменных из разных потоков
    lock (recordLock)
    {
        revenue += revenueBatch;
    }

    UpdateGlobalDictionary(brands, brandsBatch);
    UpdateGlobalDictionary(categories, categoriesBatch);
    UpdateGlobalDictionary(products, productsBatch);
}

// Вспомогательная функция для обновления словаря
void UpdateDictionary(Dictionary<string, int> dictionary, string key)
{
    if (dictionary.TryGetValue(key, out int count))
    {
        dictionary[key] = count + 1;
    }
    else
    {
        dictionary[key] = 1;
    }
}

// Вспомогательная функция для обновления глобальных словарей
void UpdateGlobalDictionary(ConcurrentDictionary<string, int> globalDictionary, Dictionary<string, int> batchDictionary)
{
    foreach (var item in batchDictionary)
    {
        globalDictionary.AddOrUpdate(item.Key, item.Value, (key, oldValue) => oldValue += item.Value);

        // Обновление метрик популярности
        if (ReferenceEquals(globalDictionary, brands) && mostPopularBrand.count < globalDictionary[item.Key])
        {
            mostPopularBrand.name = item.Key;
            mostPopularBrand.count = globalDictionary[item.Key];
        }

        if (ReferenceEquals(globalDictionary, categories) && mostPopularCategory.count < globalDictionary[item.Key])
        {
            string[] parts = item.Key.Split("::");
            mostPopularCategory.id = parts[0];
            mostPopularCategory.code = parts[1];
            mostPopularCategory.count = globalDictionary[item.Key];
        }

        if (ReferenceEquals(globalDictionary, products) && mostPopularProduct.count < globalDictionary[item.Key])
        {
            mostPopularProduct.id = item.Key;
            mostPopularProduct.count = globalDictionary[item.Key];
        }
    }
}
