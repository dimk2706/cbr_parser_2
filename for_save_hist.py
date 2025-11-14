from parser import CurrencyRatesParser

parser = CurrencyRatesParser()

# Парсим данные за период
parser.parse_date_range("01.01.2020", "31.01.2020")

# СОХРАНЯЕМ в базу данных
if parser.records:
    success = parser.send_to_database()
    if success:
        print(f"✅ Данные сохранены в базу! Записей: {len(parser.records)}")
    else:
        print("❌ Ошибка сохранения в базу")
else:
    print("ℹ️ Нет данных для сохранения")