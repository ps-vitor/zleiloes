from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import csv
import time
import random
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
import traceback
import requests


class PortalzukScraper:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        )

        self.driver = webdriver.Chrome(options=self.options)
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        })

    def close_popups(self):
        try:
            city_dropdown = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.select2-selection__rendered')))
            self.driver.execute_script("arguments[0].style.display = 'none';", city_dropdown)
            time.sleep(1)
        except:
            pass

        try:
            close_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.close, .modal-close')
            for button in close_buttons:
                try:
                    button.click()
                    time.sleep(0.5)
                except:
                    continue
        except:
            pass

    def is_valid_url(self, url):
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def scrapItensPages(self, url):
        extra_data = {}
        try:
            if not self.is_valid_url(url):
                print(f"[ERRO] URL inválida: {url}")
                return extra_data

            print(f"Scraping: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Dados básicos
            basic_info = {}
            features = soup.find_all("div", class_="property-featured-item")
            for feature in features:
                label = feature.find("span", class_="property-featured-item-label")
                value = feature.find("span", class_="property-featured-item-value")
                if label and value:
                    basic_info[label.get_text(strip=True)] = value.get_text(strip=True)
            
            # Dados adicionais
            additional_info = {}
            
            # Status
            status = soup.find("span", class_="property-status-title")
            if status:
                additional_info["Status"] = status.get_text(strip=True)
            
            # Matrícula
            matricula = soup.find("p", {"id": "itens_matricula"})
            if matricula:
                additional_info["Matrícula"] = matricula.get_text(strip=True)
            
            # Observações
            observacoes = soup.find("div", class_="div-text-observacoes")
            if observacoes:
                additional_info["Observações"] = observacoes.get_text(strip=True)
            
            # Link do processo
            processo = soup.find("a", class_="glossary-link")
            if processo and processo.has_attr("href"):
                additional_info["Link do Processo"] = processo["href"]
            
            # Visitação
            visita = soup.find("div", class_="property-info-text")
            if visita:
                additional_info["Visitação"] = visita.get_text(strip=True)
            
            # Formas de pagamento
            pagamentos = soup.find("h3", class_="property-info-title")
            if pagamentos and "Formas de Pagamento" in pagamentos.get_text():
                items = soup.find_all("li", class_="property-payments-item")
                pagamento_info = []
                for item in items:
                    text = item.find("p", class_="property-payments-item-text")
                    if text:
                        pagamento_info.append(text.get_text(strip=True))
                if pagamento_info:
                    additional_info["Formas de Pagamento"] = " | ".join(pagamento_info)
            
            # Juntando todos os dados
            extra_data = {**basic_info, **additional_info}
            return extra_data

        except Exception as e:
            print(f"Erro ao processar {url}: {str(e)}")
            traceback.print_exc()
            return extra_data
    
    def load_all_properties(self):
        print("Carregando todos os imóveis...")
        self.driver.get(self.base_url)
        time.sleep(3)
        self.close_popups()

        current_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
        max_attempts = 35
        print(f"Imóveis carregados inicialmente: {current_count}")

        while current_count < 989 and max_attempts > 0:
            try:
                load_more_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//button[contains(text(), "Carregar mais")]'))
                )
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_button)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", load_more_button)
                
                WebDriverWait(self.driver, 15).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, 'div.card-property')) > current_count)

                new_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
                if new_count == current_count:
                    print("Nenhum novo imóvel carregado. Parando...")
                    break

                current_count = new_count
                max_attempts -= 1
                print(f"Imóveis carregados: {current_count}")
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"Erro ao carregar mais imóveis: {str(e)}")
                break

        return self.driver.page_source

    def scrapMainPage(self, html):
        properties = []
        try:
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.find_all("div", class_="card-property")

            for card in cards:
                try:
                    # Link da propriedade
                    link_tag = card.find("a", href=True)
                    link = link_tag["href"] if link_tag else None
                    if link and not link.startswith("http"):
                        link = urljoin(self.base_url, link)
                    
                    # Informações básicas
                    lote = card.find(class_="card-property-price-lote")
                    address = card.find(class_="card-property-address")
                    
                    # Preços (pode ter múltiplos)
                    prices = []
                    price_blocks = card.find_all("ul", class_="card-property-prices")
                    for block in price_blocks:
                        items = block.find_all("li", class_="card-property-price")
                        for item in items:
                            label = item.find(class_="card-property-price-label")
                            value = item.find(class_="card-property-price-value")
                            date = item.find(class_="card-property-price-data")
                            
                            if label and value and date:
                                prices.append({
                                    "Tipo": label.get_text(strip=True),
                                    "Valor": value.get_text(strip=True).replace("R$", "").replace(".", "").replace(",", ".").strip(),
                                    "Data": date.get_text(strip=True)
                                })
                    
                    # Criar entrada única para a propriedade com todos os preços
                    if link and prices:
                        property_data = {
                            "Lote": lote.get_text(strip=True) if lote else "",
                            "Endereço": address.get_text(separator=" ", strip=True) if address else "",
                            "Link": link,
                            "Preços": prices  # Lista de todos os preços
                        }
                        properties.append(property_data)
                
                except Exception as e:
                    print(f"Erro ao processar card: {str(e)}")
                    continue

        except Exception as e:
            print(f"[ERRO] scrapMainPage: {str(e)}")
            traceback.print_exc()

        return properties

    def enrich_with_details(self, properties):
        print(f"Enriquecendo {len(properties)} propriedades com detalhes...")
        
        # Filtra apenas propriedades com links válidos
        valid_properties = [p for p in properties if self.is_valid_url(p.get("Link", ""))]
        print(f"Propriedades com links válidos: {len(valid_properties)}")
        
        with ThreadPoolExecutor(max_workers=min(10, cpu_count())) as executor:
            futures = []
            for prop in valid_properties:
                futures.append(executor.submit(self.scrapItensPages, prop["Link"]))
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    details = future.result()
                    if details:
                        valid_properties[i].update(details)
                except Exception as e:
                    print(f"Erro ao enriquecer propriedade {i}: {str(e)}")
        
        return valid_properties

    def prepare_for_export(self, properties):
        flat_properties = []
        for prop in properties:
            # Para cada preço, cria uma entrada separada no CSV
            for price in prop.get("Preços", []):
                flat_prop = {
                    "Lote": prop.get("Lote", ""),
                    "Endereço": prop.get("Endereço", ""),
                    "Link": prop.get("Link", ""),
                    "Tipo de Preço": price.get("Tipo", ""),
                    "Valor (R$)": price.get("Valor", ""),
                    "Data": price.get("Data", "")
                }
                
                # Adiciona os detalhes extras
                for key, value in prop.items():
                    if key not in ["Lote", "Endereço", "Link", "Preços"]:
                        flat_prop[key] = value
                
                flat_properties.append(flat_prop)
        
        return flat_properties

    def export_to_csv(self, properties, filename="portalzuk.csv"):
        if not properties:
            print("Nenhum dado para exportar")
            return False

        # Prepara os dados para exportação
        flat_data = self.prepare_for_export(properties)
        
        # Obtém todos os campos possíveis
        fieldnames = set()
        for row in flat_data:
            fieldnames.update(row.keys())
        
        # Ordena os campos para consistência
        fieldnames = sorted(fieldnames)
        
        try:
            with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flat_data)
            
            print(f"Dados exportados com sucesso para {filename}")
            print(f"Total de registros: {len(flat_data)}")
            return True
        except Exception as e:
            print(f"Erro ao exportar CSV: {str(e)}")
            traceback.print_exc()
            return False

    def run(self):
        try:
            print("Iniciando scraping...")
            
            # Passo 1: Carregar todas as propriedades
            html = self.load_all_properties()
            
            # Passo 2: Extrair dados básicos
            properties = self.scrapMainPage(html)
            print(f"Propriedades encontradas: {len(properties)}")
            
            # Passo 3: Enriquecer com detalhes
            enriched_properties = self.enrich_with_details(properties)
            
            # Passo 4: Exportar para CSV
            self.export_to_csv(enriched_properties)
            
            print("Processo concluído com sucesso!")
            
        except Exception as e:
            print(f"Erro durante a execução: {str(e)}")
            traceback.print_exc()
        finally:
            self.driver.quit()
