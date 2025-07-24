from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup

class PortalBayit:
    def __init__(self):
        # Lista de User-Agents para rotação
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:109.0) Gecko/20100101 Firefox/110.0",
        ]
        
        # Configurações do ChromeDriver
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize the driver
        self.driver = webdriver.Chrome(options=self.options)

    def get_pages(self,url):
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        nav_el  =   soup.find_all("nav",class_="dg-paginacao")
        buscas=[]
        for a   in  nav_el:
            href=a.get('href')
            if  href:
                buscas.append(href)
        return  len(buscas)

    def get_links(self, url):
        self.driver.get(url)
        time.sleep(3)
    
        last_height = self.driver.execute_script("return document.body.scrollHeight")
    
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # aguarda carregar novos itens
    
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        row_leiloes = soup.find_all("a", class_="dg-leiloes-img")
    
        links_propriedades = set()
        for item in row_leiloes:
            href = item.get('href')
            if href:
                links_propriedades.add(href)
    
        return list(links_propriedades)

    def change_btw_pagesindexes(self,qtd_pagina,qtd_index):
        urls=set()
        for i   in  range   (1,qtd_index):
            for j   in  range(1,qtd_pagina):
                url=f"https://www.portalbayit.com.br/busca/#Engine=Start&Pagina={j}&Busca=&Mapa=&Ordem=10&PaginaIndex={i}"
                if  url:
                    urls.add(url)
        for _   in  urls:
            print(_)
        return  list(urls)

    def retorna_links(self):
        url="https://www.portalbayit.com.br/busca/#Engine=Start&Pagina=1&Busca=&Mapa=&Ordem=10&PaginaIndex=1"
        buscas_len=self.get_pages(url)
        links = self.get_links(url)
        urls=self.change_btw_pagesindexes(buscas_len,3)
        print(f"Found {len(links)} links:")
        for link in links:
            print(link)
        return links

    def __del__(self):
        # Close the driver when the object is destroyed
        if hasattr(self, 'driver'):
            self.driver.quit()

if __name__ == "__main__":
    portalbayit = PortalBayit()
    portalbayit.retorna_links()