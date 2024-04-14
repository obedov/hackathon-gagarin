import pandas as pd
import json

# Загрузите данные из CSV
data = pd.read_csv('../processed/mentions_texts.csv')

# Извлечение текстов сообщений
message_texts = data['MessageText'].tolist()[:10]

# Сохранение списка в JSON файл
with open('test_texts.json', 'w', encoding='utf-8') as f:
    json.dump(message_texts, f, ensure_ascii=False, indent=4)

print("Тексты сообщений сохранены в файл 'test_texts.json'")
