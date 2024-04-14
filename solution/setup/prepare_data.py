import pandas as pd


def get_unique_messages(filepath, output_filepath):
    # Загрузка данных из CSV файла
    data_df = pd.read_csv(filepath)

    # Нормализация названий столбцов для унификации вариантов написания
    data_df.columns = data_df.columns.str.strip()  # удаляем лишние пробелы

    # Проверка наличия необходимых столбцов
    required_columns = ['ChannelID', 'MessageID']
    if not all(col in data_df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in data_df.columns]
        raise KeyError(f"Missing required columns: {missing_cols}")

    # Удаление дубликатов с учетом 'ChannelID' и 'MessageID'
    unique_df = data_df.drop_duplicates(subset=required_columns, keep='first')

    # Сохранение обработанных данных в новый файл
    unique_df.to_csv(output_filepath, index=False)


# Пути к файлам
input_filepath = '../processed/mentions_texts.csv'
output_filepath = '../processed/unique_mentions_texts.csv'

# Вызов функции
get_unique_messages(input_filepath, output_filepath)
