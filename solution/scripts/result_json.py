import pandas as pd


def prepare_data_for_json():
    # Загрузка данных
    data = pd.read_csv('mentions_&_sentiments.csv')

    # Инициализация словаря для группировки
    grouped_by_message_id = {}

    # Проходим по каждой строке данных и группируем по message_id
    for idx, row in data.iterrows():
        key = row['message_id']
        value = [row['issuer_id'], row['sentiment_score']]
        if key in grouped_by_message_id:
            grouped_by_message_id[key].append(value)
        else:
            grouped_by_message_id[key] = [value]

    # Преобразуем словарь в список списков
    final_grouped_list = list(grouped_by_message_id.values())
    print(final_grouped_list)
    return final_grouped_list


prepare_data_for_json()
