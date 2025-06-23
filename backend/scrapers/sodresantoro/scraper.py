from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
from selenium.webdriver.chrome.service import Service

class SodreSantoroScraper:
    def __init__(self, delay=2.0):
        options = Options()
        options.headless = False
        options.add_argument("--disable-gpu")
        from selenium.webdriver.chrome.service import Service
        service = Service("/usr/bin/chromedriver")  # ou onde estiver instalado
        self.driver = webdriver.Chrome(service=service, options=options)
        self.delay = delay

    def run(self):
        url = "https://www.sodresantoro.com.br/imoveis/lotes?page=1"
        self.driver.get(url)
        time.sleep(self.delay)  # espera o JS carregar

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        cards = soup.find_all('li')
        if not cards:
            print("Nenhum card de imóvel encontrado.")
            self.driver.quit()
            return

        results = []
        for card in cards:
            title = card.find('h2')
            price = card.find(lambda tag: tag.name in ['p','span'] and 'R$' in tag.text)
            address = card.find('p', class_='text-xs')
            link_tag = card.find('a', href=True)

            results.append({
                'titulo': title.get_text(strip=True) if title else None,
                'preco': price.get_text(strip=True) if price else None,
                'endereco': address.get_text(strip=True) if address else None,
                'link': link_tag['href'] if link_tag else None,
            })

        self.driver.quit()
        df = pd.DataFrame(results)
        df.to_csv('sodre.csv', index=False, encoding='utf-8-sig')
        print(f"Extraídos {len(results)} imóveis.")