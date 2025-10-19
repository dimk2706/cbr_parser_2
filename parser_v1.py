import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def save_currency_rates_to_excel(date=None, filename=None):
    """
    Простая функция для получения курсов валют и сохранения в Excel
    """
    if date is None:
        date = datetime.now().strftime("%d.%m.%Y")
    
    url = "https://cbr.ru/currency_base/daily/"
    params = {
        'UniDbQuery.Posted': 'True',
        'UniDbQuery.To': date
    }
    
    try:
        response = requests.get(url, params=params)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='data')
        
        data = []
        for row in table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 5:
                row_data = {
                    'Цифровой код': cols[0].text.strip(),
                    'Буквенный код': cols[1].text.strip(),
                    'Единиц': cols[2].text.strip(),
                    'Валюта': cols[3].text.strip(),
                    'Курс': float(cols[4].text.strip().replace(',', '.'))
                }
                data.append(row_data)
        
        df = pd.DataFrame(data)
        
        if filename is None:
            filename = f"курсы_валют_{date.replace('.', '_')}.xlsx"
        
        # Сохраняем в Excel с простым форматированием
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Курсы валют', index=False)
            
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
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Курсы валют на {date} сохранены в файл: {filename}")
        return filename
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return None

# Пример использования упрощенной версии
if __name__ == "__main__":
    # Сохраняем текущие курсы
    save_currency_rates_to_excel()
    
    # Сохраняем курсы на конкретную дату
    save_currency_rates_to_excel('14.10.2025', 'курсы_14_10_2025.xlsx')