import os
from datetime import datetime

# Настройки парсера
BASE_URL = "https://cbr.ru/currency_base/daily/"
MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = 10

# Список валют для мониторинга (можно оставить пустым для всех валют)
CURRENCIES = [
]

# Заголовки для запросов
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Настройки базы данных
DB_CONFIG = {
    'table_name': 'currency_rates',
    'unique_fields': ['digital_code', 'date']  # Поля для проверки дубликатов
}