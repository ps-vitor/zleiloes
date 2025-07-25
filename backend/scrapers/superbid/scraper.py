from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup

class SuperbidScraper:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        self.url = "https://www.superbid.net/categorias/imoveis?searchType=opened"
        self.url_base = "https://www.superbid.net"
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
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[id^='offer-card-']")))
            
            # Obtém todos os links de ofertas
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            a_tags = soup.find_all("a", id=lambda x: x and x.startswith("offer-card-"))
            
            for a in a_tags:
                href = a.get("href")
                if href:
                    full_url = self.url_base + href
                    self.unique_links.add(full_url)  # Adiciona ao conjunto (evita duplicatas)
                    
        except Exception as e:
            print(f"Erro ao processar página: {e}")

    def go_to_next_page(self):
        try:
            pagination = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.sc-71689b74-0.iqPXXn"))
            )
            
            next_button = pagination.find_element(By.XPATH, ".//button[contains(text(), 'Próximo')]")
            
            if "disabled" in next_button.get_attribute("class"):
                print("Chegou à última página")
                return False
                
            self.driver.execute_script("arguments[0].scrollIntoView();", next_button)
            self.driver.execute_script("arguments[0].click();", next_button)
            
            self.wait.until(EC.staleness_of(next_button))
            time.sleep(2) 
            
            return True
            
        except Exception as e:
            print("Erro ao tentar avançar para próxima página")
            return False

    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    superbid = SuperbidScraper()
    try:
        superbid.get_homelinks()
    except Exception as e:
        print(f"Erro no processo principal: {e}")
    finally:
        superbid.close()