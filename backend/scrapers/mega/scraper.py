from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, traceback
from bs4 import BeautifulSoup
import threading
from queue import Queue, Empty
import requests
from urllib.parse import urljoin
import csv
import os,re

class MegaScraper:
    def __init__(self, output_dir="output"):
        # Configurações do navegador
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # Configurações de timeout aumentadas
        self.options.add_argument('--disable-http2')
        self.options.add_argument('--disable-quic')
        
        # URLs base
        self.base_url = "https://www.megaleiloes.com.br"
        self.start_url = urljoin(self.base_url, "/imoveis")
        
        # Configurações de scraping
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.set_page_load_timeout(60)  # Aumenta timeout para 60 segundos
        self.wait = WebDriverWait(self.driver, 30)  # Aumenta timeout para 30 segundos
        self.max_retries = 3
        self.request_timeout = 20  # Aumenta timeout para requests
        
        # Configurações de paralelismo
        self.link_queue = Queue()
        self.results = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # Configurações de output
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "mega_tabs.csv")

    def get_homelinks(self):
        try:
            # print(f"Iniciando scraping em {self.start_url}")
            
            # Tentativa com retry
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(self.start_url)
                    self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    break
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        raise
                    print(f"Tentativa {attempt + 1} falhou, tentando novamente...")
                    time.sleep(5)
            
            time.sleep(5)
            
            # Inicia workers para processamento paralelo
            num_workers = 4
            workers = []
            for i in range(num_workers):
                t = threading.Thread(target=self.property_worker, name=f"Worker-{i+1}")
                t.daemon = True
                t.start()
                workers.append(t)
                print(f"Iniciada thread {t.name}")
            
            # Processa todas as páginas
            page_count = 1
            while True:
                # print(f"\nProcessando página {page_count}")
                self.process_current_page()
                
                if not self.go_to_next_page():
                    break
                
                page_count += 1
                time.sleep(3)

            # Finalização
            print("\nAguardando conclusão do processamento...")
            self.link_queue.join()
            self.stop_event.set()
            
            for worker in workers:
                worker.join()

            # Exporta para CSV
            self.export_to_csv()
            
            print("\n=== RESUMO ===")
            print(f"Total de propriedades processadas: {len(self.results)}")
            print(f"Arquivo CSV gerado: {self.output_file}")

        except Exception as e:
            print(f"Erro em get_homelinks: {e}")
            traceback.print_exc()
        finally:
            self.close()

    def property_worker(self):
        while not self.stop_event.is_set():
            try:
                link = self.link_queue.get(timeout=5)
                # print(f"{threading.current_thread().name} processando: {link}")
                
                for attempt in range(self.max_retries):
                    try:
                        property_data = self.get_property_info(link)
                        with self.lock:
                            self.results.append(property_data)
                        break
                    except Exception as e:
                        if attempt == self.max_retries - 1:
                            print(f"Falha ao processar {link} após {self.max_retries} tentativas")
                            with self.lock:
                                self.results.append({
                                    "url": link,
                                    "error": str(e),
                                })
                        time.sleep(2)
                
                self.link_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                print(f"Erro no worker {threading.current_thread().name}: {e}")

    def process_current_page(self):
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[class*='card-title']")))
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            for a in soup.find_all("a", class_="card-title", href=True):
                href = a.get("href")
                if href:
                    full_url = urljoin(self.base_url, href)
                    self.link_queue.put(full_url)
                    # print(f"Adicionado à fila: {full_url}")
                    
        except Exception as e:
            print(f"Erro ao processar página: {e}")

    def go_to_next_page(self):
        try:
            next_button = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='pagina='] span.fa-chevron-right"))
            )
            
            parent_link = next_button.find_element(By.XPATH, "./..")
            if "disabled" in parent_link.get_attribute("class"):
                print("Última página alcançada")
                return False
                
            self.driver.execute_script("arguments[0].scrollIntoView(); arguments[0].click();", parent_link)
            self.wait.until(EC.staleness_of(parent_link))
            time.sleep(2)
            return True
            
        except Exception as e:
            print(f"Erro na paginação: {e}")
            return False
    
    def get_images(self, url, max_images):
        print("extraindo imagens")
        imagens = []
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 20)

        try:
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.owl-item img[src]')))
            img_elements = driver.find_elements(By.CSS_SELECTOR, 'div.owl-item img[src]')
            for img in img_elements[:max_images]:
                src = img.get_attribute("src")
                if src:
                    imagens.append(src)
        except Exception as e:
            print(f"Erro ao extrair imagens: {e}")
        finally:
            driver.quit()

        return {f"imagem_{i+1}": img_url for i, img_url in enumerate(imagens)}



    
    def get_row_tabs_data(self, html_content):
        row_tabs_data = {}
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Encontrar todas as tabs (abas)
            tabs = soup.find("ul", class_="nav nav-tabs").find_all("li")
            tab_names = [tab.find("a").text.strip() for tab in tabs]

            # Encontrar todo o conteúdo das tabs
            tab_content = soup.find("div", class_="tab-content")

            # Para cada tab, extrair o conteúdo correspondente
            for name in tab_names:
                # Mapear o nome da tab para o ID correspondente
                tab_id_map = {
                    "Descrição": "tab-description",
                    "Condições de pagamento": "tab-payment-condition",
                    "Parcelamento e Proposta": "tab-parcelamento"
                }

                tab_id = tab_id_map.get(name)
                if tab_id:
                    content_div = tab_content.find("div", id=tab_id)
                    if content_div:
                        # Limpar o conteúdo removendo tags desnecessárias e espaços extras
                        content = content_div.get_text(separator="\n", strip=True)

                        # Processamento especial para a tab "Descrição"
                        if name == "Descrição":
                            # Extrair a matrícula (tudo até o primeiro ponto ou quebra de linha)
                            matricula_match = re.search(r'MATRÍCULA Nº.*?(?=[-\n])', content, re.IGNORECASE)
                            if matricula_match:
                                row_tabs_data["Matrícula"] = matricula_match.group(0).strip()
                                # Remover a matrícula da descrição
                                content = content.replace(matricula_match.group(0), "").strip()

                        row_tabs_data[name] = content
        
            return row_tabs_data

        except Exception as e:
            print(f"Erro ao extrair dados das tabs: {e}")
            return {}

    def get_property_info(self, url):
        """Coleta informações de uma propriedade usando requests + BeautifulSoup"""
        data = {
            "url": url,
            "valor": "N/A",
            "endereco": "N/A",
            "tipo_leilao": "N/A",
            "leiloeiro": "N/A",
            "tipo_imovel":"N/A",
        }

        if not self.is_valid_url(url):
            data["error"] = "URL inválida"
            return data

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(url, headers=headers, timeout=self.request_timeout)
            response.raise_for_status()

            html_content = response.text  # Store the HTML content
            soup = BeautifulSoup(html_content, "html.parser")

                    # Extrair o tipo de imóvel do breadcrumb
            breadcrumb = soup.find("ol", class_="breadcrumb")
            if breadcrumb:
                # O tipo de imóvel está normalmente no terceiro item do breadcrumb
                items = breadcrumb.find_all("li")
                if len(items) >= 3:
                    tipo_imovel = items[2].find("span", itemprop="name")
                    if tipo_imovel:
                        data["tipo_imovel"] = tipo_imovel.get_text(strip=True)

            # Extração dos dados
            if (valor_div := soup.find("div", class_="value")):
                data["valor"] = valor_div.get_text(strip=True).replace("R$", "").replace(".", "").replace(",", ".").strip()

            if (localizacao := soup.find("div", class_="locality item")):
                data["endereco"] = localizacao.get_text(strip=True).split("Localização")[1].strip()

            if (tipo_leilao := soup.find("div", class_="batch-type")):
                data["tipo_leilao"] = tipo_leilao.get_text(strip=True)

            author_sections = soup.find_all("div", class_="author item")
    
            for section in author_sections:
                header = section.find("div", class_="header")
                if not header:
                    continue
                
                header_text = header.get_text(strip=True)
                value_div = section.find("div", class_="value")

                if not value_div:
                    continue

                value_text = value_div.get_text(strip=True)

                if "Comitente" in header_text:
                    data["comitente"] = value_text
                elif "Leiloeiro" in header_text:
                    data["leiloeiro"] = value_text

            processo_div=soup.find("div",class_="process-number item")
            if  processo_div:
                processo_link=processo_div.find("a",href=True)
                data["processo_link"]=processo_link["href"]

            downloads = soup.find_all("div", class_="downloads")
            for div in downloads:
                download_buttons = div.find_all("a", class_="btn-download")
                for btn in download_buttons:
                    href = btn.get('href')
                    span_text = btn.find('span').get_text(strip=True) if btn.find('span') else ''
                    
                    if 'Edital' in span_text:
                        data['edital_link'] = href
                    elif 'Matricula' in span_text:
                        data['matricula_link'] = href
            
            tabs_data=self.get_row_tabs_data(html_content)
            data.update(tabs_data)
            
            dados_imagens = self.get_images(url, 3)
            data.update(dados_imagens)


        except requests.exceptions.RequestException as e:
            data["error"] = f"Request error: {str(e)}. Traceback: {traceback.print_exc()}"
        except Exception as e:
            data["error"] = f"Unexpected error: {str(e)}. Traceback: {traceback.print_exc()}"

        return data

    def export_to_csv(self):
        """Exporta os dados coletados para um arquivo CSV"""
        if not self.results:
            print("Nenhum dado para exportar")
            return

        try:
            all_keys = set().union(*(d.keys() for d in self.results))
            fieldnames = sorted(all_keys)

            with open(self.output_file, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.results)

            print(f"Dados exportados com sucesso para {self.output_file}")
        except Exception as e:
            print(f"Erro ao exportar para CSV: {e}")
            traceback.print_exc()

    def is_valid_url(self, url):
        return url and url.startswith(self.base_url)

    def close(self):
        self.stop_event.set()
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
                print("Navegador fechado")
        except Exception as e:
            print(f"Erro ao fechar navegador: {e}")

if __name__ == "__main__":
    scraper = MegaScraper()
    try:
        scraper.get_homelinks()
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário")
    except Exception as e:
        print(f"Erro fatal: {e}")
        traceback.print_exc()
    finally:
        scraper.close()