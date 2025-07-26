from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, traceback
from bs4 import BeautifulSoup
import threading
from queue import Queue, Empty  # Adicionado import Empty
import requests
from urllib.parse import urljoin
import csv
from datetime import datetime
import os

class MegaleiloesScraper:
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

        # URLs base
        self.base_url = "https://www.megaleiloes.com.br"
        self.start_url = urljoin(self.base_url, "/imoveis")
        
        # Configurações de scraping
        self.driver = webdriver.Chrome(options=self.options)
        self.wait = WebDriverWait(self.driver, 15)
        self.max_retries = 3
        self.request_timeout = 10
        
        # Configurações de paralelismo
        self.link_queue = Queue()
        self.results = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # Configurações de output
        self.output_dir = output_dir
        self.output_file = os.path.join(
            f"mega.csv"
        )

    def get_homelinks(self):
        try:
            print(f"Iniciando scraping em {self.start_url}")
            self.driver.get(self.start_url)
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
                print(f"\nProcessando página {page_count}")
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
                print(f"{threading.current_thread().name} processando: {link}")
                
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
                                    "error": str(e)
                                })
                        time.sleep(2)
                
                self.link_queue.task_done()
                
            except Empty:  # Agora usando Empty diretamente
                # Timeout da fila vazia - normal quando não há mais trabalho
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
                    print(f"Adicionado à fila: {full_url}")
                    
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
    
    def get_property_info(self, url):
        """Coleta informações de uma propriedade usando requests + BeautifulSoup"""
        data = {
            "url": url,
            "valor": "N/A",
            "endereco": "N/A",
            "tipo_leilao": "N/A",
            "leiloeiro": "N/A",
            "link_do_processo": "N/A",
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
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extração dos dados
            if (valor_div := soup.find("div", class_="value")):
                data["valor"] = valor_div.get_text(strip=True)

            if (localizacao := soup.find("div", class_="locality-item")):
                data["endereco"] = localizacao.get_text(strip=True)

            if (tipo_leilao := soup.find("div", class_="batch-type")):
                data["tipo_leilao"] = tipo_leilao.get_text(strip=True)
            
            if (leiloeiro := soup.find("div", class_="author item")):
                data["leiloeiro"] = leiloeiro.get_text(strip=True)

            if (processo_div := soup.find("div", class_="process-number item")) and "href" in processo_div.attrs:
                data["link_do_processo"] = urljoin(self.base_url, processo_div["href"])

        except requests.exceptions.RequestException as e:
            data["error"] = f"Request error: {str(e)}"
        except Exception as e:
            data["error"] = f"Unexpected error: {str(e)}"
            
        return data

    def export_to_csv(self):
        """Exporta os dados coletados para um arquivo CSV"""
        if not self.results:
            print("Nenhum dado para exportar")
            return

        try:
            fieldnames = [
                "url", "valor", "endereco", "tipo_leilao", 
                "leiloeiro", "link_do_processo"
            ]
            
            with open(self.output_file, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
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
            self.driver.quit()
            print("Navegador fechado")
        except Exception as e:
            print(f"Erro ao fechar navegador: {e}")

if __name__ == "__main__":
    scraper = MegaleiloesScraper()
    try:
        scraper.get_homelinks()
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário")
    except Exception as e:
        print(f"Erro fatal: {e}")
        traceback.print_exc()
    finally:
        scraper.close()