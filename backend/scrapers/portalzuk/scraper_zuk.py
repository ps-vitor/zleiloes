from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import traceback
import requests

class PortalzukScraper:
    def __init__(self):
        # Configuração do Selenium
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')

        self.driver = webdriver.Chrome(options=self.options)
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis"
        self.session = requests.Session()  # Sessão para requests HTTP

        # Script para ocultar o WebDriver
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            window.chrome = {
                runtime: {},
            };
            """
        })

    def close_popups(self):
        """Fecha qualquer pop-up ou elemento que possa interferir"""
        try:
            # Fechar pop-up de cidade
            city_dropdown = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.select2-selection__rendered')))
            self.driver.execute_script("arguments[0].style.display = 'none';", city_dropdown)
            time.sleep(1)
        except:
            pass

        try:
            # Fechar outros pop-ups
            close_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.close, .modal-close')
            for button in close_buttons:
                try:
                    button.click()
                    time.sleep(0.5)
                except:
                    continue
        except:
            pass

    def scrapItensPages(self, url):
        """Extrai informações adicionais de uma página de imóvel específica"""
        extra_data = {}
        try:
            if url is None:
                print("[ERRO] URL está None! Não é possível fazer a requisição.")
                return extra_data

            html = self.session.get(url, timeout=20)
            soup = BeautifulSoup(html.text, "html.parser")

            # Extrair informações dos itens de características
            itens_div = soup.find_all("div", class_="property-featured-items")
            for itens in itens_div:
                all_itens = itens.find_all("div", class_="property-featured-item")
                for iten in all_itens:
                    iten_label_tag = iten.find("span", class_="property-featured-item-label")
                    iten_value_tag = iten.find("span", class_="property-featured-item-value")
                    if iten_label_tag and iten_value_tag:
                        try:
                            iten_label = iten_label_tag.get_text(strip=True)
                            iten_value = iten_value_tag.get_text(strip=True)
                            extra_data[iten_label] = iten_value
                        except Exception as e:
                            print(e)
                            traceback.print_exc()

            # Extrair informações do conteúdo principal
            content = soup.find_all("div", class_="content")
            for c in content:
                try:
                    status_tag = c.find("span", class_="property-status-title")
                    if status_tag:
                        extra_data["Situacao"] = status_tag.get_text(strip=True)
                    
                    matricula_tag = c.find("p", {"class": "text_subtitle", "id": "itens_matricula"})
                    if matricula_tag:
                        extra_data["Matricula do imovel"] = matricula_tag.get_text(strip=True)
                    
                    obs_tag = c.find("div", {"class": "property-info-text div-text-observacoes"})
                    if obs_tag:
                        extra_data["Observacoes"] = obs_tag.get_text(strip=True)
                    
                    link_tag = c.find("a", class_="glossary-link")
                    if link_tag and "href" in link_tag.attrs:
                        extra_data["Link do processo"] = link_tag["href"]
                    
                    visita_tag = c.find("div", class_="property-info-text")
                    if visita_tag:
                        extra_data["Visitacao"] = visita_tag.get_text(strip=True)
                    
                    # Informações de pagamento
                    f_pagamento_h3_tag = c.find("h3", class_="property-info-title")
                    if f_pagamento_h3_tag:
                        f_pagamento_h3 = f_pagamento_h3_tag.get_text(strip=True)
                        f_pagamento_ul = c.find_all(class_="property-payments-items")
                        for f in f_pagamento_ul:
                            item_text = f.find("p", class_="property-payments-item-text")
                            if item_text:
                                extra_data[f_pagamento_h3] = item_text.get_text(strip=True)
                    
                    direitos_tag = c.find("p", class_="property-status-text")
                    if direitos_tag:
                        extra_data["Direitos do Compromissario Comprador"] = direitos_tag.get_text(strip=True)
                    
                    preferencia_tag = c.find("p", class_="text_subtitle")
                    if preferencia_tag:
                        extra_data["Direito de Preferencia"] = preferencia_tag.get_text(strip=True)

                except Exception as e:
                    print(f"Erro ao extrair conteúdo principal: {str(e)}")
                    continue

            return extra_data

        except Exception as e:
            print(f"Erro geral em scrapItensPages: {str(e)}")
            traceback.print_exc()
            return extra_data

    def load_all_properties(self):
        """Carrega todos os imóveis usando Selenium"""
        print("Acessando a página e carregando todos os imóveis...")
        self.driver.get(self.base_url)
        time.sleep(3)  # Espera inicial para a página carregar
        
        # Fechar elementos que podem interferir
        self.close_popups()

        last_count = 0
        current_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
        max_attempts = 35  # 989 imóveis / ~30 por página
        
        print(f"Imóveis iniciais carregados: {current_count}")

        while current_count < 989 and max_attempts > 0:
            try:
                # Encontrar o botão "Carregar mais"
                load_more_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//button[contains(text(), "Carregar mais")]'))
                )
                # Rolar até o botão e clicar via JavaScript para evitar interceptação
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_button)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", load_more_button)
                
                # Esperar carregamento com timeout
                WebDriverWait(self.driver, 15).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, 'div.card-property')) > current_count)
                
                # Atualizar contagem
                new_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
                if new_count == current_count:
                    print("Nenhum novo imóvel carregado. Parando...")
                    break
                
                current_count = new_count
                max_attempts -= 1
                print(f"Total de imóveis carregados: {current_count}")
                
                # Pequeno delay aleatório entre carregamentos
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                print(f"Erro ao carregar mais imóveis: {str(e)}")
                break

        return self.driver.page_source

    def extract_property_data(self, html):
        """Extrai os dados dos imóveis do HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        properties = {}
        
        cards = soup.find_all(class_="card-property")
        print(f"Total de imóveis encontrados: {len(cards)}")

        for card in cards:
            try:
                # Extrair dados básicos
                address_tag = card.find(class_="card-property-address")
                lote_tag = card.find(class_="card-property-price-lote")
                image_wrapper = card.find("div", class_="card-property-image-wrapper")
                link_tag = image_wrapper.find("a") if image_wrapper else None
                link = urljoin(self.base_url, link_tag["href"]) if link_tag else None
                
                if not link or link in properties:
                    continue
                
                property_data = {
                    "Lote": lote_tag.get_text(strip=True) if lote_tag else None,
                    "Endereco": address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                    "link": link,
                    "Precos": []
                }
                
                # Extrair preços
                for li in card.find_all("li", class_="card-property-price"):
                    label = li.find(class_="card-property-price-label")
                    value = li.find(class_="card-property-price-value")
                    date = li.find(class_="card-property-price-data")
                    
                    if label and value and date:
                        price_data = {
                            "Rotulo": label.get_text(strip=True),
                            "Valor (R$)": value.get_text(strip=True)
                                .replace("R$", "").replace(".", "").replace(",", ".").strip(),
                            "Data": date.get_text(strip=True)
                        }
                        property_data["Precos"].append(price_data)
                
                properties[link] = property_data
            
            except Exception as e:
                print(f"Erro ao processar card: {str(e)}")
                continue

        return properties

    def get_property_details(self, url):
        """Obtém detalhes adicionais de um imóvel específico"""
        return self.scrapItensPages(url)

    def export_to_csv(self, properties, filename="portalzuk.csv"):
        """Exporta os dados para CSV"""
        try:
            if not properties:
                print("Nenhum dado para exportar")
                return False

            # Preparar dados para exportação
            rows = []
            for link, data in properties.items():
                base_data = {
                    'Lote': data['Lote'],
                    'Endereco': data['Endereco'],
                    'link': link,
                    **{k: v for k, v in data.items() if k not in ['Lote', 'Endereco', 'link', 'Precos']}
                }
                
                if not data['Precos']:
                    rows.append(base_data)
                else:
                    for price in data['Precos']:
                        rows.append({**base_data, **price})

            # Garantir que o diretório existe
            os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)

            # Escrever arquivo CSV
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            print(f"\n✅ Dados exportados para {os.path.abspath(filename)}")
            return True

        except Exception as e:
            print(f"\n❌ Erro ao exportar dados: {str(e)}")
            return False

    def run(self, output_file="portalzuk.csv"):
        """Executa todo o processo de scraping"""
        start_time = datetime.now()
        print("\n=== INICIANDO SCRAPING PORTALZUK ===\n")
        
        # 1. Carregar todos os imóveis
        html = self.load_all_properties()
        
        # 2. Extrair dados básicos
        properties = self.extract_property_data(html)
        if not properties:
            print("\n❌ Nenhum imóvel encontrado.")
            return {
                "status": "error",
                "message": "Nenhum imóvel encontrado",
                "duration": str(datetime.now() - start_time)
            }

        print(f"\n✅ {len(properties)} imóveis encontrados. Coletando detalhes...")

        # 3. Coletar detalhes em paralelo
        with ThreadPoolExecutor(max_workers=min(cpu_count() * 2, 10)) as executor:  # Limite de 10 threads
            futures = {executor.submit(self.get_property_details, link): link for link in properties}
            
            for future in as_completed(futures):
                link = futures[future]
                try:
                    details = future.result()
                    if details:
                        properties[link].update(details)
                        print(f"✓ Detalhes coletados: {link}")
                except Exception as e:
                    print(f"✗ Erro em {link}: {str(e)}")

        # 4. Exportar dados
        export_result = self.export_to_csv(properties, output_file)
        duration = datetime.now() - start_time
        
        print(f"\n⏱ Tempo total: {duration}")
        return {
            "status": "success" if export_result else "partial_success",
            "total_imoveis": len(properties),
            "arquivo": os.path.abspath(output_file) if export_result else None,
            "duration": str(duration)
        }

    def __del__(self):
        """Fecha o navegador ao destruir o objeto"""
        if hasattr(self, 'driver'):
            self.driver.quit()