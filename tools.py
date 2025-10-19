import pandas as pd
from typing import Any, List, Dict
from datetime import datetime
import requests
from environs import env


class Pipeline:
    """
    `pipe = Pipeline(f, g, h); pipe(v) ~ h(g(f(v)))`
    """
    def __init__(self, *funcs) -> None:
        self.funcs = funcs

    def __call__(self, r: Any) -> Any:
        for func in self.funcs:
            r = func(r)
        return r


class CurrencyDataProcessor:
    """
    Класс для обработки и анализа данных о курсах валют.
    """
    
    @staticmethod
    def filter_by_currencies(data: List[Dict], currencies: List[str]) -> List[Dict]:
        """Фильтрует данные по списку валют."""
        return [item for item in data if item.get('letter_code') in currencies]
    
    @staticmethod
    def convert_to_rubles(amount: float, currency_code: str, rates_data: List[Dict]) -> float:
        """Конвертирует сумму в рубли по текущему курсу."""
        for rate in rates_data:
            if rate['letter_code'] == currency_code:
                return amount * rate['exchange_rate'] / rate['units']
        raise ValueError(f"Курс для валюты {currency_code} не найден")
    
    @staticmethod
    def calculate_changes(current_rates: List[Dict], previous_rates: List[Dict]) -> List[Dict]:
        """Рассчитывает изменения курсов по сравнению с предыдущими данными."""
        changes = []
        
        for current in current_rates:
            currency_code = current['letter_code']
            previous = next((item for item in previous_rates 
                           if item['letter_code'] == currency_code), None)
            
            change_data = current.copy()
            if previous:
                change = current['exchange_rate'] - previous['exchange_rate']
                change_percent = (change / previous['exchange_rate']) * 100
                change_data.update({
                    'change': change,
                    'change_percent': change_percent,
                    'previous_rate': previous['exchange_rate']
                })
            else:
                change_data.update({
                    'change': None,
                    'change_percent': None,
                    'previous_rate': None
                })
            
            changes.append(change_data)
        
        return changes
    
    @staticmethod
    def remove_duplicates(data: List[Dict], unique_fields: List[str]) -> List[Dict]:
        """Удаляет дубликаты записей."""
        seen = set()
        unique_data = []
        
        for item in data:
            # Создаем ключ из уникальных полей
            key = tuple(str(item.get(field, '')) for field in unique_fields)
            
            if key not in seen:
                seen.add(key)
                unique_data.append(item)
        
        return unique_data


class DatabaseClient:
    """
    Клиент для работы с базой данных через API.
    """
    
    def __init__(self):
        self.base_url = env("CURRENCY_RATES_ENDPOINT")
        self.api_token = env("API_TOKEN")
    
    def send_rates(self, rates_data: List[Dict]) -> bool:
        """Отправляет курсы валют в базу данных."""
        try:
            response = requests.post(
                url=self.base_url,
                headers={"API-Token": self.api_token},
                json=rates_data,
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"Успешно отправлено {len(rates_data)} записей в базу данных")
                return True
            else:
                print(f"Ошибка при отправке в базу данных. Статус: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Ошибка подключения к API: {e}")
            return False


# Пайплайн для очистки числовых данных
clean_numeric_pipe = Pipeline(
    lambda x: str(x).strip(),
    lambda x: x.replace(',', '.'),
    float
)