import pandas as pd

# Путь к файлу Excel
xlsx_path = '../dataset/moex.xlsx'
# xlsx_path = '../dataset/issuers.xlsx'

# Путь для сохранения файла CSV
csv_path = '../processed/moex.csv'
# csv_path = '../processed/issuers.csv'

# Загрузка Excel файла
df = pd.read_excel(xlsx_path)

# Сохранение данных в формате CSV
df.to_csv(csv_path, index=False)

print("Файл успешно сохранен в формате CSV:", csv_path)
