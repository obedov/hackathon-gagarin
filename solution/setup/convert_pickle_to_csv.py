import pickle

# Путь к файлу pickle
pickle_path = '../dataset/sentiment_texts.pickle'
# pickle_path = '../dataset/mentions_texts.pickle'

# Загрузка данных из pickle файла
with open(pickle_path, 'rb') as file:
    df = pickle.load(file)

# Сохранение DataFrame в CSV файл, предполагая, что df уже является pandas DataFrame
csv_path = '../processed/sentiment_texts.csv'
# csv_path = '../processed/mentions_texts.csv'

df.to_csv(csv_path, index=False)
print(f"Данные сохранены в файл: {csv_path}")
