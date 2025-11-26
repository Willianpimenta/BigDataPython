from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

def run():
    data = []
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get('http://books.toscrape.com/')
        books = driver.find_elements(By.CSS_SELECTOR, 'article.product_pod')[:5]
        for b in books:
            title = b.find_element(By.CSS_SELECTOR, 'h3 a').get_attribute('title')
            price = b.find_element(By.CSS_SELECTOR, '.price_color').text
            data.append({'Titulo': title, 'Preco': price})
        driver.quit()
    except Exception as e:
        print('Erro no Selenium:', e)
        print('Usando dados mock.')
        data = [
            {'Titulo': 'A Light in the Attic', 'Preco': '£51.77'},
            {'Titulo': 'Tipping the Velvet', 'Preco': '£53.74'},
            {'Titulo': 'Soumission', 'Preco': '£50.10'},
            {'Titulo': 'Sharp Objects', 'Preco': '£47.82'},
            {'Titulo': 'Sapiens', 'Preco': '£54.23'}
        ]

    df = pd.DataFrame(data)
    try:
        df['Preco_Num'] = df['Preco'].astype(str).str.replace('£', '', regex=False).astype(float)
    except Exception:
        df['Preco_Num'] = 0.0

    print('\nResultado:')
    print(df.to_string(index=False))

if __name__ == '__main__':
    run()
