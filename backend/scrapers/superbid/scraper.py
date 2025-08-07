from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, traceback
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading

class SuperbidScraper:
    def __init__(self, max_workers=6):
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
        self.max_workers = max_workers
        self.driver = None
        self.unique_links = set()
        self.property_data = []
        self.lock = threading.Lock()
        self.task_queue = queue.Queue()
        self.results = []
        self.drivers = []
        self.all_characteristics = set()

    def init_driver(self):
        driver = webdriver.Chrome(options=self.options)
        self.drivers.append(driver)
        return driver

    def get_homelinks(self):
        try:
            self.driver = self.init_driver()
            self.driver.get(self.url)
            time.sleep(5)
            
            try:
                cookie_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Aceitar')]"))
                )
                cookie_btn.click()
                time.sleep(1)
            except:
                pass
            
            while True:
                self.process_current_page()
                if not self.go_to_next_page():
                    break
                break
                time.sleep(3)
                
            print(f"\nTotal de imóveis encontrados: {len(self.unique_links)}")
            
            for link in self.unique_links:
                self.task_queue.put(link)
            
        except Exception as e:
            print(f"Erro no get_homelinks: {e}")
            traceback.print_exc()

    def process_current_page(self):
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[id^='offer-card-']"))
            )
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            for a in soup.find_all("a", id=lambda x: x and x.startswith("offer-card-")):
                if href := a.get("href"):
                    with self.lock:
                        self.unique_links.add(self.url_base + href)
            
        except Exception as e:
            print(f"Erro no process_current_page: {e}")

    def go_to_next_page(self):
        try:
            next_btn = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(., 'Próximo') and not(contains(@class, 'disabled'))]")
            ))
            self.driver.execute_script("arguments[0].click();", next_btn)
            WebDriverWait(self.driver, 10).until(EC.staleness_of(next_btn))
            time.sleep(3)
            return True
        except:
            print("Não há mais páginas")
            return False

    def worker(self):
        driver = self.init_driver()
        while True:
            try:
                url = self.task_queue.get_nowait()
                property_info = self.get_property_info(url, driver)
                if property_info:
                    with self.lock:
                        self.property_data.append(property_info)
                self.task_queue.task_done()
            except queue.Empty:
                break

    def extract_all_sections(self, driver):
        sections_data = {}
        relevant_sections = [
            "Características do Imóvel",
            "Documentos",
            "Informações do processo",
            "Detalhes do Imóvel",
            "Descrição",
            "Valores"
        ]
        
        try:
            sections = driver.find_elements(By.CSS_SELECTOR, "div.sc-29469d5b-2.idDXIs")
            
            for section in sections:
                try:
                    title_element = section.find_element(By.CSS_SELECTOR, "h3.sc-29469d5b-3.hrVoCP")
                    title = title_element.text.strip()
                    
                    if title not in relevant_sections:
                        continue
                    
                    is_closed = section.get_attribute("data-state") == "closed"
                    
                    if is_closed:
                        button = section.find_element(By.CSS_SELECTOR, "div.sc-29469d5b-1.eQnVdT")
                        driver.execute_script("arguments[0].scrollIntoView();", button)
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                    
                    content_div = section.find_element(By.CSS_SELECTOR, "div.sc-29469d5b-4")
                    content_html = content_div.get_attribute("innerHTML")
                    
                    if "Características do Imóvel" in title:
                        sections_data[title] = self.process_characteristics_section(content_html)
                    elif "Documentos" in title:
                        sections_data[title] = self.process_documents_section(content_html)
                    elif "Informações do processo" in title:
                        sections_data[title] = self.process_process_info_section(content_html)
                    elif "Detalhes do Imóvel" in title:
                        sections_data[title] = self.process_property_details_section(content_html)
                    elif "Descrição" in title:
                        sections_data[title] = self.process_description_section(content_html)
                    elif "Valores" in title:
                        sections_data[title] = self.process_values_section(content_html)
                        
                except Exception as e:
                    print(f"Erro ao processar seção via Selenium: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Erro ao localizar seções via Selenium: {str(e)}")
        
        if not sections_data:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                section_divs = soup.find_all("div", class_="sc-29469d5b-2")
                
                for section in section_divs:
                    try:
                        title_element = section.find("h3", class_="sc-29469d5b-3")
                        if not title_element:
                            continue
                            
                        title = title_element.get_text(strip=True)
                        
                        if title not in relevant_sections:
                            continue
                            
                        content_div = section.find("div", class_="sc-29469d5b-4")
                        
                        if not content_div:
                            continue
                            
                        content_html = str(content_div)
                        
                        if "Características do Imóvel" in title:
                            sections_data[title] = self.process_characteristics_section(content_html)
                        elif "Documentos" in title:
                            sections_data[title] = self.process_documents_section(content_html)
                        elif "Informações do processo" in title:
                            sections_data[title] = self.process_process_info_section(content_html)
                        elif "Detalhes do Imóvel" in title:
                            sections_data[title] = self.process_property_details_section(content_html)
                        elif "Descrição" in title:
                            sections_data[title] = self.process_description_section(content_html)
                        elif "Valores" in title:
                            sections_data[title] = self.process_values_section(content_html)
                            
                    except Exception as e:
                        print(f"Erro ao processar seção via BeautifulSoup: {str(e)}")
                        continue
                        
            except Exception as e:
                print(f"Erro no fallback BeautifulSoup: {str(e)}")
        
        return sections_data

    def process_characteristics_section(self, html_content):
        data = {}
        soup = BeautifulSoup(html_content, 'html.parser')
        
        items = soup.find_all("li")
        for item in items:
            spans = item.find_all("span")
            if len(spans) >= 2:
                key = spans[0].get_text(strip=True).replace(":", "")
                value = spans[1].get_text(strip=True)
                data[key] = value
        
        if not data:
            paragraphs = soup.find_all("p")
            for p in paragraphs:
                strong = p.find("strong")
                if strong:
                    key = strong.get_text(strip=True).replace(":", "")
                    value = p.get_text().replace(key, "").strip()
                    data[key] = value
        
        return data

    def process_documents_section(self, html_content):
        return self.process_characteristics_section(html_content)

    def process_process_info_section(self, html_content):
        data = {}
        soup = BeautifulSoup(html_content, 'html.parser')
        
        paragraphs = soup.find_all("p")
        for p in paragraphs:
            strong = p.find("span", style="font-weight: bold;")
            if not strong:
                strong = p.find("strong")
                
            if strong:
                key = strong.get_text(strip=True).replace(":", "")
                value = p.get_text().replace(key, "").strip()
                data[key] = value
        
        return data

    def process_property_details_section(self, html_content):
        data = {}
        soup = BeautifulSoup(html_content, 'html.parser')
        
        paragraphs = soup.find_all("p")
        for p in paragraphs:
            strong = p.find("strong")
            if strong:
                key = strong.get_text(strip=True).replace(":", "")
                value = p.get_text().replace(key, "").strip()
                data[key] = value
        
        return data

    def process_description_section(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator="\n", strip=True)

    def process_values_section(self, html_content):
        return self.process_characteristics_section(html_content)

    def get_property_info(self, url, driver):
        data = {
            "url": url,
            "titulo": None,
            "endereco_completo": None,
            "ultimo_lance": None,
            "leiloeiro": None,
            "vendido_por": None,
            "descricao": None,
        }

        try:
            driver.get(url)
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)

            sections_data = self.extract_all_sections(driver)
            
            for section_name, section_content in sections_data.items():
                clean_section_name = self.clean_column_name(section_name.lower())
                
                if "características do imóvel" in clean_section_name:
                    for key, value in section_content.items():
                        clean_key = self.clean_column_name(f"caract_{key}")
                        data[clean_key] = value
                elif "documentos" in clean_section_name:
                    for key, value in section_content.items():
                        clean_key = self.clean_column_name(f"doc_{key}")
                        data[clean_key] = value
                elif "informações do processo" in clean_section_name:
                    for key, value in section_content.items():
                        clean_key = self.clean_column_name(f"processo_{key}")
                        data[clean_key] = value
                elif "detalhes do imóvel" in clean_section_name:
                    for key, value in section_content.items():
                        clean_key = self.clean_column_name(f"detalhe_{key}")
                        data[clean_key] = value
                elif "descrição" in clean_section_name:
                    data["descricao_completa"] = section_content
                elif "valores" in clean_section_name:
                    for key, value in section_content.items():
                        clean_key = self.clean_column_name(f"valor_{key}")
                        data[clean_key] = value

            soup = BeautifulSoup(driver.page_source, "html.parser")

            if title := soup.find("h1"):
                data["titulo"] = title.get_text(strip=True)

            if loc_div := soup.find("div", class_="sc-8126a53f-4"):
                data["endereco_completo"] = loc_div.get_text(separator=" | ", strip=True)

            if lance := soup.find("span", class_="lance-atual"):
                data["ultimo_lance"] = self.clean_value(lance.get_text(strip=True))

            div_dados_leilao = soup.find_all("div", class_="sc-8126a53f-3 jZSJxj")
            for dado in div_dados_leilao:
                titles = dado.find_all("p", class_="sc-8126a53f-6 uegjp")
                values = dado.find_all("p", class_="sc-8126a53f-7 fygozL")
                for title, value in zip(titles, values):
                    t = title.get_text(strip=True)
                    v = value.get_text(strip=True)
                    if "Vendido por" in t:
                        data["vendido_por"] = v
                    elif "Leiloeiro" in t:
                        data["leiloeiro"] = v
            
            image_containers = soup.select('div[class*="sc-4db409e9-8"]')  # Pega divs com classe parcial
            images = []

            for i, container in enumerate(image_containers[:10]):  # Limita a 10 imagens
                img = container.find("img", class_="offer-image")  # Classe fixa das imagens
                if img and img.get("src"):
                    images.append(img["src"])

            # Fallback - Abordagem 2 (se a primeira não pegar)
            if not images:
                images = [img["src"] for img in soup.select('img.offer-image[src]')[:10]]

            # Fallback - Abordagem 3 (último recurso)
            if not images:
                images = [img["src"] for img in soup.find_all("img", src=True) 
                         if "sbwebservices.net/photos/" in img["src"]][:10]

            # Adiciona as imagens ao dicionário de dados
            for idx, img_url in enumerate(images, 1):
                data[f"imagem_{idx}"] = img_url

            print(f"Processado: {url}")
            return data

        except Exception as e:
            print(f"Erro ao processar {url}: {e}")
            traceback.print_exc()
            return None

    def clean_column_name(self, name):
        cleaned = re.sub(r'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ _-]', '', name)
        cleaned = cleaned.replace(" ", "_").replace("-", "_").lower()
        return cleaned.strip('_')

    def clean_value(self, text):
        if not text:
            return None
        return text.replace("R$", "").replace(".", "").replace(",", ".").strip()

    def run_parallel(self):
        print("Iniciando coleta paralela...")
        self.get_homelinks()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.worker) for _ in range(self.max_workers)]
            for future in as_completed(futures):
                future.result()
        
        print("Coleta paralela concluída!")

    def save_to_csv(self, filename="superbid_imoveis.csv"):
        import csv
        try:
            if not self.property_data:
                print("Nenhum dado para salvar")
                return
            
            fieldnames = set()
            for item in self.property_data:
                fieldnames.update(item.keys())
            
            standard_fields = ['url', 'titulo', 'endereco_completo', 'ultimo_lance', 
                              'leiloeiro', 'vendido_por', 'descricao']
            other_fields = sorted(f for f in fieldnames if f not in standard_fields)
            ordered_fields = standard_fields + other_fields
            
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fields)
                writer.writeheader()
                
                for item in self.property_data:
                    complete_row = {field: item.get(field, "") for field in ordered_fields}
                    writer.writerow(complete_row)
            
            print(f"Dados salvos em {filename} (total: {len(self.property_data)} registros)")
            
        except Exception as e:
            print(f"Erro ao salvar CSV: {e}")

    def close_all_drivers(self):
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass

    def run(self):
        try:
            self.run_parallel()
            self.save_to_csv()
        except Exception as e:
            print(f"Erro principal: {e}")
            traceback.print_exc()
        finally:
            self.close_all_drivers()

if __name__ == "__main__":
    scraper = SuperbidScraper(max_workers=4)
    scraper.run()