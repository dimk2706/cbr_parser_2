import time
import schedule
from environs import env

from parser import CurrencyRatesParser
from settings import CURRENCIES


def job() -> None:
    """
    Parse currency rates and upload to the database and save to Excel.
    """
    parser = CurrencyRatesParser()
    
    # Парсим данные
    parser.parse()
    
    # Сохраняем в Excel
    filename = f"currency_rates_{parser.current_date.replace('.', '_')}.xlsx"
    parser.save_to_excel(filename)
    
    # Отправляем в базу данных через API
    if parser.records:
        success = parser.send_to_database()
        if success:
            print(f"Данные успешно отправлены в базу данных. Записей: {len(parser.records)}")
        else:
            print("Ошибка при отправке данных в базу данных")
    
    print(f"Курсы валют на {parser.current_date} сохранены в файл: {filename}")


if __name__ == "__main__":
    # Запускаем каждый день в 09:00 по Москве (после обновления данных ЦБ)
    schedule.every().day.at("09:00", "Europe/Moscow").do(job)
    
    # Для тестирования - запустить сразу
    job()
    
    while True:
        schedule.run_pending()
        time.sleep(1)