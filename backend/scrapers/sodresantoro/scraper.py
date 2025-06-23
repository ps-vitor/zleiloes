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
        cards = soup.find_all('div',class_="relative flex-none h-fit rounded-xl overflow-hidden bg-white border")
        if not cards:
            print("Nenhum card de imóvel encontrado.")
            self.driver.quit()
            return

        results = []
        
        for card in cards:
            leilao=card.find("span",class_="leading-5")
            title = card.find('h2', class_="uppercase font-medium text-neutral-800 my-2 h-12 line-clamp-2")
            price = card.find('p', class_="text-2xl text-blue-700 font-medium")
            address = card.find('span', class_='text-sm line-clamp-1')
            results.append({
                'leilao': leilao.get_text(strip=True),
                'titulo': title.get_text(strip=True) if title else None,
                'preco': price.get_text(strip=True) if price else None,
                'endereco': address.get_text(strip=True) if address else None,
            })

        for imovel in results:
            print(imovel)


        self.driver.quit()
        df = pd.DataFrame(results)
        df.to_csv('sodre.csv', index=False, encoding='utf-8-sig')
        print(f"Extraídos {len(results)} imóveis.")