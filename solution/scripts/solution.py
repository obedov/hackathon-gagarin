import aiofiles
import csv
import pandas as pd
import asyncio
import aiohttp
from io import StringIO
from aiohttp import ClientSession, ClientTimeout
from tqdm.asyncio import tqdm_asyncio


async def load_tickers(filepath):
    """Асинхронная загрузка данных о тикерах из CSV файла с указанием кодировки utf-8."""
    try:
        async with aiofiles.open(filepath, mode='r', encoding='utf-8') as file:
            data = await file.read()  # Асинхронно читаем весь файл в строку
    except UnicodeDecodeError:
        try:
            # Попытка чтения с альтернативной кодировкой, если utf-8 не сработала
            async with aiofiles.open(filepath, mode='r', encoding='cp1252') as file:
                data = await file.read()
        except Exception as e:
            print(f"Ошибка при чтении файла с альтернативной кодировкой: {e}")
            return pd.DataFrame()  # Возвращаем пустой DataFrame при неудаче
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame при неудаче

    try:
        # Используем StringIO для имитации файла, на который pandas может вызвать read_csv
        tickers_df = pd.read_csv(StringIO(data))
        tickers_df['BGTicker'] = tickers_df['BGTicker'].fillna(tickers_df['BGTicker.1'])
        return tickers_df
    except pd.errors.ParserError as e:
        print(f"Ошибка при парсинге данных: {e}")
        return pd.DataFrame()


async def load_api_keys(filename):
    """Асинхронная загрузка API ключей из файла."""
    api_keys = []
    async with aiofiles.open(filename, 'r') as file:
        async for line in file:
            stripped_line = line.strip()
            if stripped_line:
                api_keys.append(stripped_line)
    return api_keys


async def load_proxy_config(filename):
    """Асинхронная загрузка конфигурации прокси из файла."""
    config = {}
    async with aiofiles.open(filename, 'r') as file:
        async for line in file:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip()
    return config


async def read_mentions_from_csv(file_path):
    """Асинхронное чтение упоминаний из CSV файла."""
    return pd.read_csv(file_path)


def prepare_company_names(tickers_df):
    """Prepare a dictionary mapping issuerid to company names extracted from specified columns."""
    company_names = {}
    for index, row in tickers_df.iterrows():
        possible_names = [
            row[col] for col in tickers_df.columns if "Unnamed" in col or col == "EMITENT_FULL_NAME"
        ]
        possible_names = {name.strip() for name in possible_names if isinstance(name, str)}
        company_names[row['issuerid']] = possible_names
    return company_names


async def call_openai_api(session, api_key, request_data):
    api_url = 'https://api.openai.com/v1/chat/completions'
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    # Загружаем настройки прокси
    proxy_config = await load_proxy_config('../env/proxy_configs.txt')
    proxy_address = proxy_config.get('proxy_address', '')
    proxy_type = proxy_config.get('proxy_type', 'http')
    proxy_username = proxy_config.get('proxy_username', '')
    proxy_password = proxy_config.get('proxy_password', '')

    if proxy_address:
        proxy_auth = f"{proxy_username}:{proxy_password}@" if proxy_username and proxy_password else ""
        proxy_url = f"{proxy_type}://{proxy_auth}{proxy_address}"
    else:
        proxy_url = None

    # Асинхронный запрос с использованием предоставленной сессии и настройкой прокси
    try:
        async with session.post(api_url, json=request_data, headers=headers, proxy=proxy_url) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Ошибка при отправке запроса через прокси {proxy_url if proxy_url else 'без прокси'}: {e}")
        return None


async def process_and_evaluate_single_message(semaphore, session, row, api_key):
    """Asynchronously process and evaluate sentiment of a message in a single API call."""
    await semaphore.acquire()
    try:
        instruction = (
            f"Раздели этот текст на список тикеров и сообщений, относящимся к ним, строго в формате: "
            f"#TICKER (название компании): сообщение (в одно предложение без переноса), с новой строки для каждого тикера, "
            f"без дублирования и без генерации своего текста. Затем оцени сентимент каждой пары #TICKER (компания): сообщение, на шкале от 0 до 5, "
            f"добавив после сообщения 'Оценка: число', где: 0 означает отсутствие информации, 1 - очень негативная информация, "
            f"2 - скорее негативная информация, 3 - нейтральная информация, 4 - положительная информация, 5 - очень положительная информация."
            f"ВНИМАНИЕ: Оценку следует добавить строго в этой же строке, без переноса на другу строку, даже если строка длинная и в ней есть непонятные символы. Это очень строгое условие. \n\n{row['MessageText']}"
        )
        messages = [{'role': 'user', 'content': instruction}]
        request_data = {
            'model': 'gpt-3.5-turbo',
            'messages': messages
        }
        # Вызываем функцию call_openai_api для отправки запроса
        api_response = await call_openai_api(session, api_key, request_data)

        if api_response:
            return {
                'message_id': row['MessageID'],
                'channel_id': row['ChannelID'],
                'issuer_id': row['issuerid'],
                'message_text': row['MessageText'],
                'api_response': api_response
            }
        else:
            return {
                'message_id': row['MessageID'],
                'channel_id': row['ChannelID'],
                'issuer_id': row['issuerid'],
                'message_text': row['MessageText'],
                'api_response': {'error': 'Failed to process message', 'status_code': 'API Failure'}
            }
    finally:
        semaphore.release()


async def parallel_process_messages(messages_df, api_keys):
    """Асинхронно обрабатывает сообщения, используя несколько API ключей."""
    connector = aiohttp.TCPConnector(limit_per_host=10)
    timeout = ClientTimeout(total=60)  # Увеличение времени ожидания
    semaphore = asyncio.Semaphore(20)  # Ограничение на 20 одновременных операций

    async with ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            process_and_evaluate_single_message(semaphore, session, row, api_keys[index % len(api_keys)])
            for index, row in messages_df.iterrows()
        ]

        processed_results = []
        for result in tqdm_asyncio(asyncio.as_completed(tasks), total=len(tasks), desc="Processing messages"):
            processed_results.append(await result)
        return processed_results


async def parse_and_save_processed_messages(processed_results, company_names, filename):
    """Асинхронно фильтрует обработанные сообщения, оценивает сентименты и сохраняет оригинальный текст ответа в файл."""
    save_interval = 100
    name_to_issuerid = {}

    # Заполнение словаря для быстрого поиска issuerid по названию компании или тикеру
    for issuerid, names in company_names.items():
        for name in names:
            cleaned_name = ' '.join(name.split()).upper()
            name_to_issuerid[cleaned_name] = issuerid

    def find_issuerid_by_name(name):
        """Ищет и возвращает issuerid по имени компании или тикеру."""
        return name_to_issuerid.get(name.upper(), None)

    async with aiofiles.open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['channel_id', 'message_id', 'issuer_id', 'sentiment_score', 'processed_text'])
        await file.write(','.join(writer.fieldnames) + '\n')  # Записываем заголовки

        count = 0
        for processed in processed_results:
            message_id = processed['message_id']
            channel_id = processed['channel_id']
            api_response = processed.get('api_response')
            if api_response and 'choices' in api_response and len(api_response['choices']) > 0:
                content = api_response['choices'][0]['message']['content']
            else:
                print(f"Недостаточно данных для обработки сообщения {message_id}")
                continue

            for line in content.split('\n'):
                if line.strip():
                    try:
                        parts = line.rsplit(' Оценка: ', 1)
                        if len(parts) == 2:
                            description_part, sentiment_score = parts
                            if ': ' in description_part:
                                ticker_info, description = description_part.split(': ', 1)
                                sentiment_score = int(sentiment_score)

                                # Поиск issuerid по названию компании или тикеру
                                ticker_info = ' '.join(ticker_info.split())
                                ticker, company = ticker_info.split(' ', 1) if ' ' in ticker_info else (ticker_info, '')
                                ticker = ticker.strip('#')
                                company = company.strip('()').upper()

                                issuerid_ticker = find_issuerid_by_name(ticker)
                                issuerid_company = find_issuerid_by_name(company) if company else None
                                issuerid = issuerid_ticker if issuerid_ticker else issuerid_company

                                if issuerid:
                                    result = {
                                        'channel_id': channel_id,
                                        'message_id': message_id,
                                        'issuer_id': issuerid,
                                        'sentiment_score': sentiment_score,
                                        'processed_text': line.replace('"', '""')  # Экранируем кавычки
                                    }
                                    await file.write('"' + '","'.join(str(result[field]) for field in writer.fieldnames) + '"\n')
                                    count += 1

                                    if count % save_interval == 0:
                                        await file.flush()
                    except ValueError as e:
                        print(f"Ошибка в строке '{line}': {e}. Строка будет пропущена.")
            await file.flush()


async def main():
    # Загрузка сообщений и тикеров moex
    messages_df = pd.read_csv('../processed/unique_mentions_texts.csv')
    tickers_df = await load_tickers('../processed/moex.csv')

    # Загрузка API ключей из файла
    api_keys = await load_api_keys('../env/openai_api_keys.txt')

    # Подготовка словаря с названиями компаний из DataFrame, полученного из файла
    company_names = prepare_company_names(tickers_df)

    # Получение первых сообщений из DataFrame с сообщениями для обработки
    sample_messages_df = messages_df.head(1000)

    # Обработка выбранных сообщений для разбиения текста на компании и сообщения
    processed_messages = await parallel_process_messages(sample_messages_df, api_keys)

    # Фильтрация и сохранение обработанных сообщений в CSV-файл
    await parse_and_save_processed_messages(processed_messages, company_names, 'mentions_&_sentiments.csv')


# Запуск асинхронного main
if __name__ == "__main__":
    asyncio.run(main())
