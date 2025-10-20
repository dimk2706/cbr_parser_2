from datetime import datetime
import logging
import time
from typing import List, Dict, Any, Optional

import logfire
import pandas as pd
import requests
from bs4 import BeautifulSoup
from environs import Env

from settings import *

# Загружаем env
env = Env()
env.read_env()

# Настройка логирования
logfire_token = env("LOGFIRE_TOKEN", None)
if logfire_token:
    logfire.configure(token=logfire_token, service_name="currency_parser")
    logging.basicConfig(handlers=[logfire.LogfireLoggingHandler()])
    
logger = logging.getLogger("currency_parser_logger")
logger.setLevel(logging.INFO)


class CurrencyRatesParser:
    """
    Парсер курсов валют с сайта ЦБ РФ.
    """
    
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.current_date: str = datetime.now().strftime("%d.%m.%Y")
    
    def parse(self, date: Optional[str] = None) -> 'CurrencyRatesParser':
        """
        Парсит курсы валют на указанную дату.
        По умолчанию - текущая дата.
        """
        self.current_date = date or self.current_date
        
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = self.send_request(self.current_date)
                if response and response.status_code == 200:
                    self._parse_html(response.text)
                    logger.info(f"Успешно получены курсы валют на {self.current_date}")
                    break
                else:
                    logger.error(f"Попытка {attempt}: Не удалось получить данные")
            except Exception as e:
                logger.error(f"Попытка {attempt}: Ошибка парсинга - {e}")
            
            if attempt < MAX_ATTEMPTS:
                time.sleep(REQUEST_TIMEOUT)
        
        return self
    
    def parse_date_range(self, start_date: str, end_date: str) -> 'CurrencyRatesParser':
        """Парсит данные за указанный период"""
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(start_date, "%d.%m.%Y")
        end_dt = datetime.strptime(end_date, "%d.%m.%Y")

        current_dt = start_dt
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%d.%m.%Y")

            # Пропускаем будущие даты
            if current_dt > datetime.now():
                print(f"⏩ Пропускаем будущую дату: {date_str}")
                current_dt += timedelta(days=1)
                continue

            print(f"📅 Парсим {date_str}...")
            self.parse(date_str)
            time.sleep(1)  # Пауза между запросами
            current_dt += timedelta(days=1)
    
        return self

    
    def _parse_html(self, html: str) -> None:
        """Парсит HTML и извлекает данные о курсах валют."""
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', class_='data')
        
        if not table:
            logger.error("Таблица с курсами валют не найдена")
            return
        
        tbody = table.find('tbody')
        if not tbody:
            logger.error("Тело таблицы не найдено")
            return
        
        for row in tbody.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 5:
                currency_data = {
                    'digital_code': cols[0].text.strip(),
                    'letter_code': cols[1].text.strip(),
                    'units': int(cols[2].text.strip()),
                    'currency_name': cols[3].text.strip(),
                    'exchange_rate': float(cols[4].text.strip().replace(',', '.')),
                    'date': self.current_date,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'cbr.ru'
                }
                
                # Если задан список валют для мониторинга, фильтруем
                if not CURRENCIES or currency_data['letter_code'] in CURRENCIES:
                    self.records.append(currency_data)
                    logger.debug(f"Добавлена валюта: {currency_data['letter_code']}")
    
    @property
    def rates_dataframe(self) -> pd.DataFrame:
        """Возвращает данные в виде DataFrame."""
        return pd.DataFrame(self.records)
    
    @property
    def rates_for_db(self) -> List[Dict[str, Any]]:
        """Возвращает данные в формате для отправки в базу данных."""
        return self.records
    
    def save_to_excel(self, filename: str) -> None:
        """Сохраняет данные в Excel файл с форматированием."""
        if not self.records:
            logger.warning("Нет данных для сохранения")
            return
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                self.rates_dataframe.to_excel(
                    writer, 
                    sheet_name='Курсы валют', 
                    index=False
                )
                
                # Автонастройка ширины колонок
                worksheet = writer.sheets['Курсы валют']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Ограничиваем максимальную ширину
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Данные сохранены в файл: {filename}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в Excel: {e}")
            raise
    
    def get_currency_rate(self, currency_code: str) -> Optional[float]:
        """Возвращает курс конкретной валюты."""
        for record in self.records:
            if record['letter_code'] == currency_code:
                return record['exchange_rate']
        return None
    
    def send_to_database(self) -> bool:
        """Отправляет данные в базу данных через API."""
        if not self.records:
            logger.warning("Нет данных для отправки в базу данных")
            return False
        
        try:
            response = requests.post(
                url=env("CURRENCY_RATES_ENDPOINT"),
                headers={"API-Token": env("API_TOKEN")},
                json=self.rates_for_db,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"Данные успешно отправлены в базу данных. Записей: {len(self.records)}")
                return True
            else:
                logger.error(f"Ошибка при отправке в базу данных. Статус: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка подключения к API базы данных: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке в базу данных: {e}")
            return False
    
    @staticmethod
    def send_request(date: str) -> Optional[requests.Response]:
        """Отправляет запрос к API ЦБ РФ."""
        params = {
            'UniDbQuery.Posted': 'True',
            'UniDbQuery.To': date
        }
        
        try:
            response = requests.get(
                BASE_URL, 
                params=params, 
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            return response
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса: {e}")
            return None


# Упрощенная функция для обратной совместимости
def save_currency_rates_to_excel(date=None, filename=None, save_to_db=False):
    """
    Упрощенная функция для обратной совместимости.
    """
    parser = CurrencyRatesParser()
    
    if date:
        parser.parse(date)
    else:
        parser.parse()
    
    if filename is None:
        filename = f"currency_rates_{parser.current_date.replace('.', '_')}.xlsx"
    
    # Сохраняем в Excel
    parser.save_to_excel(filename)
    
    # Отправляем в базу данных если требуется
    if save_to_db:
        parser.send_to_database()
    
    return filename