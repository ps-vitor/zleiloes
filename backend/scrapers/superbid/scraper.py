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
        self.all_characteristics = set()  # Para armazenar todos os nomes de características encontradas

    def init_driver(self):
        """Inicializa um driver por thread"""
        driver = webdriver.Chrome(options=self.options)
        self.drivers.append(driver)
        return driver

    def get_homelinks(self):
        """Coleta links de todas as páginas"""
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
                # V remover para carregar todas paginas
                break
                time.sleep(3)
                
            print(f"\nTotal de imóveis encontrados: {len(self.unique_links)}")
            
            for link in self.unique_links:
                self.task_queue.put(link)
            
        except Exception as e:
            print(f"Erro no get_homelinks: {e}")
            traceback.print_exc()

    def process_current_page(self):
        """Processa links da página atual"""
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
        """Navega para próxima página"""
        try:
            next_btn = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(., 'Próximo') and not(contains(@class, 'disabled'))]"))
            )
            self.driver.execute_script("arguments[0].click();", next_btn)
            WebDriverWait(self.driver, 10).until(EC.staleness_of(next_btn))
            time.sleep(3)
            return True
        except:
            print("Não há mais páginas")
            return False

    def worker(self):
        """Processa tarefas da fila em paralelo"""
        driver = self.init_driver()
        while True:
            try:
                url = self.task_queue.get_nowait()
                property_info = self.get_property_info(url, driver)
                if property_info:
                    with self.lock:
                        # Atualiza o conjunto de todas as características encontradas
                        for key in property_info.keys():
                            if key.startswith('caract_') or key.startswith('valor_'):
                                self.all_characteristics.add(key)
                        self.property_data.append(property_info)
                self.task_queue.task_done()
            except queue.Empty:
                break

    def get_property_info(self, url, driver):
        """Extrai dados de um imóvel específico com características estruturadas"""
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

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Informações básicas
            if title := soup.find("h1"):
                data["titulo"] = title.get_text(strip=True)

            # Localização
            if loc_div := soup.find("div", class_="sc-8126a53f-4"):
                data["endereco_completo"] = loc_div.get_text(separator=" | ", strip=True)

            # Valores
            if lance := soup.find("span", class_="lance-atual"):
                data["ultimo_lance"] = self.clean_value(lance.get_text(strip=True))

            # Informações do leilão
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

            # Descrição completa
                try:
                    driver.find_element(By.XPATH, "//button[contains(., 'Continuar lendo')]").click()
                    time.sleep(1)
                except:
                    pass
                
                if desc_div := soup.find("div", class_="sc-8126a53f-13"):
                    desc_text = desc_div.get_text(separator="\n", strip=True)
                    # Processa a descrição para extrair campos estruturados
                    desc_data = self.parse_description(desc_text)
                    for key, value in desc_data.items():
                        clean_key = self.clean_column_name(f"desc_{key}")
                        data[clean_key] = value
                # Extrair características estruturadas
                characteristics = self.extract_structured_section(driver, "Características do Imóvel")
                for key, value in characteristics.items():
                    clean_key = self.clean_column_name(f"caract_{key}")
                    data[clean_key] = value
                # Extrair valores estruturados
                values = self.extract_structured_section(driver, "Valores")
                for key, value in values.items():
                    clean_key = self.clean_column_name(f"valor_{key}")
                    data[clean_key] = value
                print(f"Processado: {url}")
                return data

        except Exception as e:
            print(f"Erro ao processar {url}: {e}")
            traceback.print_exc()
            return None

    def parse_description(self, description_text):
        """Extrai campos estruturados da descrição do imóvel"""
        result = {}
        if not description_text:
            return result

        # Padrões comuns em descrições (adaptar conforme necessário)
        patterns = {
            "Área Total": r"Área [Tt]otal[:]?\s*([\d,.]+)\s*m²",
            "Área Privativa": r"Área [Pp]rivativa[:]?\s*([\d,.]+)\s*m²",
            "Área Construída":r"Área Construída[:]?\s*([\d,.]+)\s*m²",
            "Status":r"Status da obra",
            "Quartos": r"Quartos[:]?\s*(\d+)",
            "Banheiros": r"Banheiros[:]?\s*(\d+)",
            "Vagas": r"Vagas[:]?\s*(\d+)",
            "Andar": r"Andar[:]?\s*(\d+|Térreo|[\wçã]+)",
            "Situação": r"Situação[:]?\s*R?\$?\s*([\d,.]+)",
            "Apartamento": r"Apartamento[:]?\s*R?\$?\s*([\d,.]+)",
            "Tipo de Venda":r"Tipo de Venda[:]",
            "Torre":r"Torre[:]"
        }

        # Procurar por cada padrão
        for key, pattern in patterns.items():
            match = re.search(pattern, description_text)
            if match:
                result[key] = match.group(1).strip()

        # Adicionar também a descrição completa
        result["texto_completo"] = description_text

        return result

    def extract_structured_section(self, driver, section_title):
        """Versão mais robusta para extrair seções com fallbacks múltiplos"""
        result = {}
        try:
            # Espera dinâmica - aguarda até 10 segundos com polling de 0.5s
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState === 'complete'"))
            
            # Tentativa 1: Localizar pelo título exato
            try:
                button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, 
                        f"//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{section_title.lower()}')]" +
                        "/ancestor-or-self::div[contains(@class, 'sc-')]"))
                )
            except:
                button = None
    
            # Tentativa 2: Localizar por classe específica do botão
            if not button:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, "div.sc-29469d5b-1.eQnVdT")
                    for btn in buttons:
                        if section_title.lower() in btn.text.lower():
                            button = btn
                            break
                except:
                    pass
                
            # Se ainda não encontrou, tentar localizar pelo conteúdo
            if not button:
                print(f"Botão '{section_title}' não encontrado - tentando extrair diretamente do HTML")
                return self.extract_from_html_fallback(driver, section_title)
    
            # Interação com o botão
            try:
                current_state = button.get_attribute("data-state")
                if current_state == "closed":
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
            except Exception as e:
                print(f"Erro ao interagir com botão: {str(e)}")
                return self.extract_from_html_fallback(driver, section_title)
    
            # Extração do conteúdo
            content_id = button.get_attribute("aria-controls")
            content = None
            
            if content_id:
                try:
                    content = WebDriverWait(driver, 5).until(
                        EC.visibility_of_element_located((By.ID, content_id)))
                except:
                    pass
                
            # Fallback para extração sem ID
            if not content:
                return self.extract_from_html_fallback(driver, section_title)
    
            # Processamento do conteúdo
            return self.process_section_content(content.get_attribute("innerHTML"))
    
        except Exception as e:
            print(f"Erro crítico ao extrair seção: {str(e)}")
            return self.extract_from_html_fallback(driver, section_title)

    def extract_from_html_fallback(self, driver, section_title):
        """Fallback quando não é possível encontrar os elementos interativos"""
        result = {}
        try:
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            # Tentar encontrar a seção pelo título
            section_header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'div'] and 
                                     section_title.lower() in tag.get_text().lower())

            if not section_header:
                return result

            # Encontrar o conteúdo associado
            section_content = None
            parent = section_header.find_parent()

            # Procurar o conteúdo em irmãos seguintes
            for sibling in section_header.find_next_siblings():
                if sibling.get('class') and any('sc-' in c for c in sibling.get('class')):
                    section_content = sibling
                    break

            if not section_content:
                return result

            return self.process_section_content(str(section_content))

        except Exception as e:
            print(f"Erro no fallback HTML: {str(e)}")
            return result

    def process_section_content(self, html_content):
        """Processa o conteúdo HTML de uma seção"""
        result = {}
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Tentar extrair itens com padrão de classe
            items = soup.find_all("div", class_=lambda x: x and "sc-" in x)

            for item in items:
                # Tentar extrair por estrutura de título e valor
                title = item.find(["p", "span"], class_=lambda x: x and "title" in x.lower())
                value = item.find(["p", "span"], class_=lambda x: x and "value" in x.lower())

                if title and value:
                    key = title.get_text(strip=True).replace(":", "")
                    result[key] = value.get_text(strip=True)
                else:
                    # Tentar extrair por texto contendo ":"
                    text = item.get_text(separator=":", strip=True)
                    if ":" in text:
                        parts = [p.strip() for p in text.split(":", 1)]
                        if len(parts) == 2:
                            result[parts[0]] = parts[1]

        except Exception as e:
            print(f"Erro ao processar conteúdo: {str(e)}")

        return result

    def clean_column_name(self, name):
        """Limpa o nome da coluna para ser válido em CSV"""
        # Remove caracteres especiais e substitui espaços por underscore
        cleaned = re.sub(r'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ _-]', '', name)
        cleaned = cleaned.replace(" ", "_").replace("-", "_").lower()
        return cleaned.strip('_')

    def clean_value(self, text):
        """Limpa valores monetários"""
        if not text:
            return None
        return text.replace("R$", "").replace(".", "").replace(",", ".").strip()

    def run_parallel(self):
        """Executa o scraping em paralelo"""
        print("Iniciando coleta paralela...")
        self.get_homelinks()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.worker) for _ in range(self.max_workers)]
            for future in as_completed(futures):
                future.result()
        
        print("Coleta paralela concluída!")

    def save_to_csv(self, filename="superbid_imoveis.csv"):
        """Salva os resultados em CSV com todas as colunas de características"""
        import csv
        try:
            if not self.property_data:
                print("Nenhum dado para salvar")
                return
            
            # Criar conjunto completo de fieldnames
            fieldnames = set()
            for item in self.property_data:
                fieldnames.update(item.keys())
            
            # Adicionar todas as características encontradas (mesmo que vazias)
            fieldnames.update(self.all_characteristics)
            
            # Ordenar as colunas
            standard_fields = ['url', 'titulo', 'endereco_completo', 'ultimo_lance', 
                              'leiloeiro', 'vendido_por', 'descricao']
            other_fields = sorted(f for f in fieldnames if f not in standard_fields)
            ordered_fields = standard_fields + other_fields
            
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fields)
                writer.writeheader()
                
                # Escrever cada linha, garantindo que todas as colunas existam
                for item in self.property_data:
                    # Criar linha com todas as colunas possíveis
                    complete_row = {field: item.get(field, "") for field in ordered_fields}
                    writer.writerow(complete_row)
            
            print(f"Dados salvos em {filename} (total: {len(self.property_data)} registros)")
            
        except Exception as e:
            print(f"Erro ao salvar CSV: {e}")

    def close_all_drivers(self):
        """Fecha todos os drivers Selenium"""
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