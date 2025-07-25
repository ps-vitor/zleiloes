from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup

class PortalBayitScraper:
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

        self.driver = webdriver.Chrome(options=self.options)

    def get_pages(self, url):
        self.driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        # Scroll to ensure all elements are loaded
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        nav_el = soup.find("nav", class_="dg-paginacao")
        
        if not nav_el:
            return 1
            
        # Find all pagination links
        page_links = nav_el.find_all("a", onclick=lambda x: x and "BuscaPaginacao" in x)
        
        if not page_links:
            return 1
            
        # Extract all page numbers from onclick attributes
        pages = set()
        for link in page_links:
            onclick = link.get("onclick", "")
            try:
                page_num = int(onclick.split("BuscaPaginacao(")[1].split(")")[0])
                pages.add(page_num)
            except (IndexError, ValueError):
                continue
        
        # Also check the current page which might be a span instead of a link
        current_page = nav_el.find("span")
        if current_page:
            try:
                pages.add(int(current_page.get_text(strip=True)))
            except ValueError:
                pass
        
        return max(pages) if pages else 1

    def get_links(self, url):
        self.driver.get(url)
        time.sleep(3)
        
        # Scroll to load all content
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        
        # Get total count if available
        count_el = soup.find("span", id="CountTotal")
        if count_el:
            try:
                self.count_total = int(count_el.get_text(strip=True))
                print(f"Total properties found: {self.count_total}")
            except ValueError:
                self.count_total = 0
        
        # Get all property links
        row_leiloes = soup.find_all("a", class_="dg-leiloes-img")
        links_propriedades = set()
        for item in row_leiloes:
            href = item.get('href')
            if href:
                links_propriedades.add(href)
        
        return list(links_propriedades)

    def retorna_links(self):
        base_url = "https://www.portalbayit.com.br/busca/?Engine=Start&Pagina=1&Busca=&Mapa=&Ordem=10&PaginaIndex=1"
        qtd_paginas = self.get_pages(base_url)
        print(f"Total de páginas encontradas: {qtd_paginas}")
        
        all_links = set()
        for pagina in range(1, qtd_paginas + 1):
            print(f"Processando página {pagina} de {qtd_paginas}")
            url = f"https://www.portalbayit.com.br/busca/?Engine=Start&Pagina={pagina}&Busca=&Mapa=&Ordem=10&PaginaIndex=1"
            try:
                links = self.get_links(url)
                print(f"Links encontrados na página {pagina}: {len(links)}")
                for link in links:
                    full_link = f"https://www.portalbayit.com.br{link}" if not link.startswith('http') else link
                    all_links.add(full_link)
                    print(full_link)
                print(f"Total acumulado: {len(all_links)}")
            except Exception as e:
                print(f"Erro ao processar página {pagina}: {str(e)}")
                continue
            
        print(f"\nTotal de links únicos encontrados: {len(all_links)}")
        return list(all_links)

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()

if __name__ == "__main__":
    portalbayit = PortalBayitScraper()
    links = portalbayit.retorna_links()
    # Optionally save links to a file
    with open("portalbayit_links.txt", "w") as f:
        for link in links:
            f.write(link + "\n")