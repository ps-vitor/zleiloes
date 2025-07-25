from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup

class MegaleiloesScraper:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        self.url = "https://www.megaleiloes.com.br/imoveis"
        self.driver = webdriver.Chrome(options=self.options)
        self.wait = WebDriverWait(self.driver, 15)
        self.unique_links = set()  # Armazena links únicos

    def get_homelinks(self):
        self.driver.get(self.url)
        time.sleep(5)  # Espera inicial maior para carregar tudo
        
        while True:
            # Processa a página atual
            self.process_current_page()
            
            # Tenta avançar para a próxima página
            if not self.go_to_next_page():
                break  # Sai do loop se não conseguir avançar
            
            time.sleep(3)  # Espera entre páginas

        # Após processar todas as páginas, imprime os links únicos
        print("\n=== LINKS ÚNICOS ENCONTRADOS ===")
        for link in self.unique_links:
            print(link)
        print(f"\nTotal de links únicos: {len(self.unique_links)}")

    def process_current_page(self):
        try:
            # Espera os elementos carregarem
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[class*='card-title']")))
            
            # Obtém todos os links de ofertas
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            a_tags = soup.find_all("a", class_="card-title", href=True)
            
            for a in a_tags:
                href = a.get("href")
                if href:
                    full_url = "https://www.megaleiloes.com.br" + href
                    self.unique_links.add(full_url)  # Adiciona ao conjunto (evita duplicatas)
                    
        except Exception as e:
            print(f"Erro ao processar página: {e}")

    def go_to_next_page(self):
        try:
            # Localiza o link de paginação (ícone ">" ou "chevron-right")
            next_button = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='pagina='] span.fa-chevron-right"))
            )
            
            # Verifica se o botão está desabilitado (última página)
            parent_link = next_button.find_element(By.XPATH, "./..")  # Pega o elemento <a> pai
            if "disabled" in parent_link.get_attribute("class"):
                print("Chegou à última página")
                return False
                
            # Clica no botão de próxima página
            self.driver.execute_script("arguments[0].scrollIntoView();", parent_link)
            self.driver.execute_script("arguments[0].click();", parent_link)
            
            # Espera a nova página carregar
            self.wait.until(EC.staleness_of(parent_link))
            time.sleep(2) 
            
            return True
            
        except Exception as e:
            print("Erro ao tentar avançar para próxima página:", e)
            return False

    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    scraper = MegaleiloesScraper()
    try:
        scraper.get_homelinks()
    except Exception as e:
        print(f"Erro no processo principal: {e}")
    finally:
        scraper.close()