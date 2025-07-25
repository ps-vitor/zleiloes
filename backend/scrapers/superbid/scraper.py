from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup

class SuperbidScraper:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:109.0) Gecko/20100101 Firefox/110.0",
        ]

        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        self.url="https://www.superbid.net/categorias/imoveis?searchType=opened"
        self.url_base="https://www.superbid.net"

        self.driver = webdriver.Chrome(options=self.options)

    def get_pages(self):
        self.driver.get(self.url)
        time.sleep(3)

        soup=BeautifulSoup(self.driver.page_source, "html.parser")
        urls=set()
        pages_el=soup.find_all("a",class_="sc-71689b74-2")
        for a   in  pages_el:
            if  a    and a.has_attr("href"):
                urls.add(a["href"])
        return  urls

    def ordena_get_pages(self,lista):
        def extrair_numero(i):
            try:
                parte=i.split("pageNumber=")
                if  len(parte)>1:
                    return  int(parte[1].split("&")[0])
            except  ValueError:
                pass
            return  0
        return  sorted(lista,key=extrair_numero)

    def get_homelinks(self):
        urls = self.get_pages()
        urls_ordenada=self.ordena_get_pages(urls)
        for url in urls_ordenada:
            self.driver.get(self.url_base + url)
            print(f"\nscrap da pagina {url}\n")
            time.sleep(3)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            a_tags = soup.find_all("a", id=lambda x: x and x.startswith("offer-card-"))
            for a in a_tags:
                href = a.get("href")
                full_url = self.url_base + href
                print(full_url)



if __name__ == "__main__":
    superbid = SuperbidScraper()
    links = superbid.get_homelinks()
    