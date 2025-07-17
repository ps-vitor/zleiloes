import csv
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from cachetools import TTLCache

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from    .circuitBreaker  import  CircuitBreaker

class PortalzukScraper:
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
        
        # Configura User-Agent inicial
        self.current_user_agent = random.choice(self.user_agents)
        self.options.add_argument(f'user-agent={self.current_user_agent}')

        self.driver = webdriver.Chrome(options=self.options)
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis"
        
        # Inicializa circuit breaker
        self.circuit_breaker = CircuitBreaker(max_failures=3, reset_timeout=120)
        
        # Configurações de intervalo entre requisições
        self.last_request_time = 1
        self.min_request_interval = 5.0
        self.max_request_interval_addition = 10.0
        self.max_workers = 4
        
        # Configura cache
        self.cache = TTLCache(maxsize=1000, ttl=3600)  # Cache de 1 hora
        
        self.session = self._create_requests_session()

    def _create_requests_session(self):
        """Cria e configura uma nova sessão requests com retry e headers rotativos"""
        session = requests.Session()
        
        # Configuração de retry com backoff exponencial
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Headers atualizados
        session.headers.update({
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.portalzuk.com.br/',
            'User-Agent': random.choice(self.user_agents),
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        return session

    def random_delay(self):
        """Adiciona atraso aleatório entre requisições"""
        time_since_last = time.time() - self.last_request_time
        sleep_time = random.uniform(
            self.min_request_interval, 
            self.min_request_interval + self.max_request_interval_addition
        )
        
        if time_since_last < sleep_time:
            time.sleep(sleep_time - time_since_last)
        
        additional_delay = random.uniform(0.5, 2.0)
        time.sleep(additional_delay)
        
        self.last_request_time = time.time()

    def is_valid_url(self, url):
        """Verifica se a URL é válida."""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def close_popups(self):
        """Tenta fechar popups que podem atrapalhar a navegação."""
        try:
            city_dropdown = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.select2-selection__rendered'))
            )
            self.driver.execute_script("arguments[0].style.display = 'none';", city_dropdown)
            print("Popup de cidade ocultado.")
        except (NoSuchElementException, TimeoutException):
            pass

        try:
            close_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.close, .modal-close, [aria-label="Close"]')
            for button in close_buttons:
                try:
                    if button.is_displayed() and button.is_enabled():
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.2)
                        print("Botão de fechar popup clicado.")
                except WebDriverException: 
                    continue
        except Exception:
            pass

    def extract_image_urls(self, html_content):
        """Extrai URLs de imagens do conteúdo HTML com cache manual."""
        cache_key = f"image_urls_{hash(html_content)}"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            image_urls = []
            image_tags = soup.select('figure.property-gallery-image img')
            for img in image_tags:
                src = img.get('src') or img.get('data-src')
                if src:
                    image_urls.append(src)
            
            self.cache[cache_key] = image_urls
            return image_urls
        except Exception as e:
            print(f"Ocorreu um erro ao extrair URLs de imagem: {e}")
            traceback.print_exc()
            return []

    def _scrap_nested_page(self, url):
        """Scrapeia uma URL aninhada com cache manual."""
        cache_key = f"nested_page_{url}"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        nested_data = {
            "leiloeiro": None,
        }
        if not self.is_valid_url(url):
            nested_data["leiloeiro"] = "URL inválida para página aninhada"
            return nested_data

        try:
            self.random_delay()
            
            response = self.session.get(url, timeout=15)
            response.raise_for_status() 
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            leiloeiro_label = soup.find("h1", class_="whitelabel-title")
            if leiloeiro_label:
                leiloeiro_text = leiloeiro_label.get_text(strip=True)
                nested_data["leiloeiro"] = leiloeiro_text.replace("Leilão de imóveis ", "").strip()
            else:
                nested_data["leiloeiro"] = "leiloeiro não encontrado"

            self.cache[cache_key] = nested_data
            return nested_data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = e.response.headers.get('Retry-After', 30)
                print(f"Erro 429 - Esperando {retry_after} segundos antes de tentar novamente...")
                time.sleep(int(retry_after))
                self.circuit_breaker.record_failure()
                self.session = self._create_requests_session()
                return self._scrap_nested_page(url)
            elif e.response.status_code == 403:
                print(f"[BLOQUEADO] Erro 403 para URL aninhada {url}: Tentando com nova sessão...")
                self.session = self._create_requests_session()
                try:
                    response = self.session.get(url, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    leiloeiro_label = soup.find("h1", class_="whitelabel-title")
                    if leiloeiro_label:
                        leiloeiro_text = leiloeiro_label.get_text(strip=True)
                        nested_data["leiloeiro"] = leiloeiro_text.replace("Leilão de imóveis ", "").strip()
                    else:
                        nested_data["leiloeiro"] = "leiloeiro não encontrado (re-tentativa)"
                    self.cache[cache_key] = nested_data
                    return nested_data
                except requests.exceptions.RequestException as retry_e:
                    print(f"Erro de requisição mesmo após recriar sessão para URL aninhada {url}: {retry_e}")
                    nested_data["leiloeiro"] = "Erro ao carregar (re-tentativa falhou)"
                    return nested_data
            else:
                print(f"Erro HTTP {e.response.status_code} para URL aninhada {url}: {e}")
                nested_data["leiloeiro"] = f"Erro HTTP {e.response.status_code}"
                return nested_data
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição para URL aninhada {url}: {e}")
            nested_data["leiloeiro"] = "Erro de requisição"
            return nested_data
        except Exception as e:
            print(f"Erro inesperado ao processar URL aninhada {url}: {e}")
            traceback.print_exc()
            nested_data["leiloeiro"] = "Erro inesperado"
            return nested_data

    def scrapItensPages(self, url):
        """Scrapa detalhes de uma página de item específica."""
        extra_data = {} 
        if not self.is_valid_url(url):
            print(f"[AVISO] URL inválida para scrapItensPages: {url}")
            return extra_data 

        try:
            self.random_delay()
            
            response = self.session.get(url, timeout=20)
            response.raise_for_status() 

            soup = BeautifulSoup(response.text, "html.parser")
            if not soup: 
                print(f"[ERRO] BeautifulSoup não conseguiu parsear: {url}")
                return extra_data
            
            features = soup.find_all("div", class_="property-featured-item")
            for feature in features:
                label = feature.find("span", class_="property-featured-item-label")
                value = feature.find("span", class_="property-featured-item-value")
                if label and value:
                    extra_data[label.get_text(strip=True)] = value.get_text(strip=True)
            
            matricula = soup.find("p", {"id": "itens_matricula"})
            if matricula:
                extra_data["Matrícula"] = matricula.get_text(strip=True)
            
            observacoes_geral = soup.find("div", class_="div-text-observacoes")
            if observacoes_geral:
                extra_data["Observações"] = observacoes_geral.get_text(strip=True)
            
            process_link_found = False
            box_action_bank_figure = soup.find("figure", class_="box-action-bank")
            if box_action_bank_figure:
                process_link_tag = box_action_bank_figure.find("a", href=True)
                if process_link_tag and self.is_valid_url(process_link_tag['href']):
                    extra_data["Link do Processo Judicial"] = process_link_tag['href']
                    process_link_found = True
            
            if not process_link_found:
                processo = soup.find("a", class_="glossary-link")
                if processo and processo.has_attr("href") and self.is_valid_url(processo['href']):
                    extra_data["Link do Processo Judicial"] = processo["href"]
            
            visitacao_h3 = soup.find("h3", class_="property-info-title", string=lambda text: text and "Visitação" in text)
            if visitacao_h3:
                visitacao_text_div = visitacao_h3.find_next_sibling("div", class_="property-info-text")
                if visitacao_text_div:
                    extra_data["Visitação"] = visitacao_text_div.get_text(strip=True)

            pagamento = soup.find("p", class_="property-payments-item-text")
            if pagamento:
                extra_data["Formas de Pagamento"] = pagamento.get_text(strip=True)

            glossary_tags = soup.find_all("div", class_="glossary-content")
            for tag in glossary_tags:
                pref_tag = tag.find("p", class_="text_subtitle")
                if pref_tag and "DIREITO DE PREFERÊNCIA" in pref_tag.get_text(strip=True):
                    extra_data["Direito de Preferência"] = pref_tag.get_text(strip=True)

            description_element_title = soup.find("h3", class_="property-info-title")
            description_element_text = soup.find("p", class_="property-hide-show")
            if description_element_title and "Descrição do imóvel" in description_element_title.get_text(strip=True):
                extra_data["Descrição do imóvel"] = description_element_text.get_text(strip=True)

            status_elements = soup.find_all("div", class_="property-status")
            for status_div in status_elements:
                title_span = status_div.find("span", class_="property-status-title")
                text_p = status_div.find("p", class_="property-status-text")
                if title_span and text_p:
                    title = title_span.get_text(strip=True)
                    text = text_p.get_text(strip=True)
                    
                    if "Imóvel ocupado" in title or "Imóvel desocupado" in title:
                        extra_data["ocupado"] = text
                    elif "Direitos do Compromissário Comprador" in title:
                        extra_data["Direitos do Compromissário"] = text
            
            image_urls = self.extract_image_urls(response.text)
            for idx, url_img in enumerate(image_urls, start=1): 
                extra_data[f"Foto_{idx}"] = url_img
            extra_data["Total_Fotos"] = len(image_urls)

            documents_div = soup.find("div", class_="property-documents-items")
            if documents_div:
                for link in documents_div.find_all('a', class_="property-documents-item"):
                    # Remove espaços extras da classe para evitar problemas
                    label = link.find('span', class_="property-documents-item-label")
                    if label and label.get_text(strip=True).lower() == "edital de venda":
                        href_edital = link.get('href')
                        if href_edital and href_edital.strip():
                            # Adiciona verificação de URL válida
                            if self.is_valid_url(href_edital):
                                extra_data["Edital de venda"] = href_edital
                            else:
                                print(f"URL do edital inválida: {href_edital}")
                            break  # Encontrou o edital, pode parar de procurar


            return extra_data 

        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [403, 429]:
                print(f"[BLOQUEADO] Erro {e.response.status_code} para {url}: Tentando com nova sessão...")
                self.circuit_breaker.record_failure()
                self.session = self._create_requests_session()
                return self.scrapItensPages(url)
            else:
                print(f"Erro HTTP {e.response.status_code} para {url}: {e}")
                return extra_data
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição para {url}: {e}")
            return extra_data
        except Exception as e:
            print(f"Erro inesperado ao processar {url}: {e}")
            traceback.print_exc()
            return extra_data

    def load_all_properties(self, url):
        """Carrega todas as propriedades clicando em 'Carregar mais'."""
        print(f"Carregando todos os imóveis da URL: {url}...")
        self.driver.get(url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.card-property'))
        )
        self.close_popups()

        initial_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
        print(f"Imóveis carregados inicialmente: {initial_count}")
        
        last_count = initial_count
        attempts_without_new_properties = 0
        max_attempts_without_new = 5
        max_total_attempts = 30
        current_total_attempts = 0

        while True:
            current_total_attempts += 1
            if current_total_attempts > max_total_attempts:
                print(f"Limite total de {max_total_attempts} tentativas de carregamento atingido. Parando...")
                break

            try:
                load_more_button = WebDriverWait(self.driver, 7).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Carregar mais")]'))
                )
                
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_button)
                time.sleep(1.0)
                
                self.driver.execute_script("arguments[0].click();", load_more_button)
                
                WebDriverWait(self.driver, 15).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, 'div.card-property')) > last_count
                )
                
                new_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
                
                if new_count > last_count:
                    print(f"Imóveis carregados: {new_count} (Adicionados: {new_count - last_count})")
                    last_count = new_count
                    attempts_without_new_properties = 0 
                else:
                    attempts_without_new_properties += 1
                    print(f"Nenhum novo imóvel carregado nesta tentativa. Tentativas sem sucesso: {attempts_without_new_properties}")
                    if attempts_without_new_properties >= max_attempts_without_new:
                        print(f"Nenhum novo imóvel carregado após {max_attempts_without_new} tentativas. Assumindo que todos os imóveis foram carregados.")
                        break

                time.sleep(random.uniform(1.5, 2.5))

            except (NoSuchElementException, TimeoutException):
                print("Botão 'Carregar mais' não encontrado ou não está mais clicável. Todos os imóveis podem ter sido carregados.")
                break 
            except Exception as e:
                print(f"Erro inesperado durante o carregamento de mais imóveis: {str(e)}")
                traceback.print_exc()
                break

        return self.driver.page_source

    def scrapMainPage(self, html):
        """Extrai as informações básicas das propriedades da página principal."""
        properties = []
        try:
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.find_all("div", class_="card-property")

            for card in cards:
                try:
                    link_tag = card.find("a", href=True)
                    link = link_tag["href"] if link_tag else None
                    if link and not link.startswith("http"):
                        link = urljoin(self.base_url, link)
                    
                    tipo_imovel = card.find(class_="card-property-price-lote")
                    address = card.find(class_="card-property-address")
                    
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
                    
                    property_data = {
                        "tipo_imovel": tipo_imovel.get_text(strip=True) if tipo_imovel else "N/A",
                        "endereco": address.get_text(separator=" ", strip=True) if address else "N/A",
                        "Link": link if link else "N/A",
                        "Preços": prices if prices else [{"Tipo": "N/A", "Valor": "N/A", "Data": "N/A"}]
                    }
                    properties.append(property_data)
                
                except Exception as e:
                    print(f"[ERRO] Erro ao processar card na página principal: {str(e)}")
                    continue

        except Exception as e:
            print(f"[ERRO] scrapMainPage: {str(e)}")
            traceback.print_exc()

        return properties

    def enrich_with_details(self, properties):
        """
        Enriquece a lista de propriedades com dados detalhados usando paralelismo.
        """
        print(f"Enriquecendo {len(properties)} propriedades com detalhes...")

        enriched_properties = [dict(prop) for prop in properties] 
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self.scrapItensPages, prop["Link"]): i 
                for i, prop in enumerate(enriched_properties) 
                if self.is_valid_url(prop.get("Link", ""))
            }
            
            processed_count = 0
            for future in as_completed(future_to_index):
                original_index = future_to_index[future]
                current_property_link = enriched_properties[original_index].get("Link", "URL desconhecida")
                processed_count += 1
                try:
                    details = future.result()
                    if details: 
                        enriched_properties[original_index].update(details)
                    else:
                        print(f"[AVISO] Nenhuma detalhe extraído para {current_property_link}. Mantendo dados parciais.")
                except Exception as e:
                    print(f"[ERRO] Falha ao enriquecer propriedade no índice {original_index} ({current_property_link}): {str(e)}")
                    traceback.print_exc()
                    enriched_properties[original_index]["ErroEnriquecimentoDetalhes"] = str(e)
                
                if processed_count % 10 == 0 or processed_count == len(future_to_index):
                    print(f"Progresso de enriquecimento de detalhes: {processed_count}/{len(future_to_index)} propriedades processadas.")
        
        return enriched_properties 

    def enrich_with_process_details(self, properties):
        """
        Para cada propriedade, se houver um link de processo, scrapeia o conteúdo.
        """
        print(f"Buscando detalhes de páginas de processo para {len(properties)} propriedades...")
        
        tasks_for_process_pages = []
        for prop in properties:
            process_link = prop.get("Link do Processo Judicial")
            if process_link and self.is_valid_url(process_link):
                tasks_for_process_pages.append((prop, process_link))

        if not tasks_for_process_pages:
            print("Nenhum link de processo válido encontrado para enriquecer.")
            return properties

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_prop = {
                executor.submit(self._scrap_nested_page, link): prop_data
                for prop_data, link in tasks_for_process_pages
            }
            
            processed_count = 0
            for future in as_completed(future_to_prop):
                prop_data = future_to_prop[future] 
                processed_count += 1
                try:
                    nested_data = future.result() 
                    if nested_data:
                        prop_data.update(nested_data) 
                except Exception as e:
                    print(f"[ERRO] Falha ao obter ou processar conteúdo da página de processo para {prop_data.get('Link')}: {e}")
                    traceback.print_exc()
                    prop_data["ErroProcessoAninhado"] = str(e)
                
                if processed_count % 10 == 0 or processed_count == len(tasks_for_process_pages):
                    print(f"Progresso de enriquecimento de links de processo: {processed_count}/{len(tasks_for_process_pages)} links processados.")
        return properties

    def prepare_for_export(self, properties):
        """Prepara os dados para exportação CSV, achatando os preços em linhas separadas."""
        flat_properties = []
        for prop in properties:
            if "Preços" not in prop or not prop["Preços"]:
                base_prop = {
                    "Data": "", 
                    "endereco": prop.get("endereco", ""),
                    "Link": prop.get("Link", ""),
                    "tipo_imovel": prop.get("tipo_imovel", ""),
                    "rotulo": "N/A", 
                    "Valor (R$)": "N/A" 
                }
                for key, value in prop.items():
                    if key not in ["Preços", "Data", "endereco", "Link", "tipo_imovel", "rotulo", "Valor (R$)"]:
                        base_prop[key] = value
                flat_properties.append(base_prop)
            else:
                for price in prop["Preços"]:
                    flat_prop = {
                        "Data": price.get("Data", ""),
                        "endereco": prop.get("endereco", ""),
                        "Link": prop.get("Link", ""),
                        "tipo_imovel": prop.get("tipo_imovel", ""),
                        "rotulo": price.get("Tipo", ""),
                        "Valor (R$)": price.get("Valor", "")
                    }
                    
                    for key, value in prop.items():
                        if key not in ["Preços", "Data", "endereco", "Link", "tipo_imovel", "rotulo", "Valor (R$)"]:
                            flat_prop[key] = value
                    
                    flat_properties.append(flat_prop)
        
        return flat_properties

    def export_to_csv(self, properties, filename="portalzuk.csv"):
        """Exporta os dados das propriedades para um arquivo CSV."""
        if not properties:
            print("Nenhum dado para exportar (lista de propriedades vazia).")
            return False

        flat_data = self.prepare_for_export(properties)
        
        if not flat_data:
            print("Nenhum dado válido para exportação após preparo (lista achatada vazia).")
            return False

        fieldnames = set()
        for row in flat_data:
            fieldnames.update(row.keys())
        
        preferred_order = [
            "tipo_imovel", "rotulo", "endereco", "Link", "Valor (R$)", 
            "Data", "Matrícula", "leiloeiro", "Descrição do imóvel",
            "Formas de Pagamento", "Direito de Preferência", 
            "Observações", "Direitos do Compromissário",
            "Link do Processo Judicial", "Visitação"
        ]
        
        photo_fields = sorted([f for f in fieldnames if f.startswith("Foto_")], 
                              key=lambda x: int(x.split('_')[1]))
        if photo_fields:
            preferred_order.extend(photo_fields)
            preferred_order.append("Total_Fotos")

        other_fieldnames = sorted([f for f in fieldnames if f not in preferred_order])
        final_fieldnames = preferred_order + other_fieldnames
        
        try:
            with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
                writer.writeheader()
                writer.writerows(flat_data)
            
            print(f"Dados exportados com sucesso para {filename}")
            print(f"Total de registros no CSV: {len(flat_data)}")
            return True
        except Exception as e:
            print(f"Erro ao exportar CSV: {str(e)}")
            traceback.print_exc()
            return False

    def run(self, start_url=None):
        """Executa o processo de scraping completo."""
        try:
            print("Iniciando scraping com proteções contra bloqueio...")
            
            start_time = time.time()
            
            target_url = start_url if start_url else self.base_url
            
            # Use load_all_properties para garantir que todos os imóveis sejam carregados
            html = self.load_all_properties(target_url)
            properties = self.scrapMainPage(html)


            # rodar sem load_all_properties
            # self.driver.get(target_url)
            # html=self.driver.page_source
            # properties=self.scrapMainPage(html)


            print(f"Propriedades encontradas na página principal: {len(properties)}")
            
            if not properties:
                print("Nenhuma propriedade encontrada na página principal para enriquecer. Encerrando.")
                return

            # Enriquecimento com delays e paralelismo controlado
            enriched_properties = self.enrich_with_details(properties)
            print(f"Propriedades enriquecidas com detalhes da página do imóvel: {len(enriched_properties)}")

            final_properties = self.enrich_with_process_details(enriched_properties)
            
            # Gera nome do arquivo baseado na URL
            parsed_url = urlparse(target_url)
            filename_suffix = parsed_url.path.replace('/', '_').replace('-', '_').strip('_')
            if not filename_suffix or filename_suffix == 'leilao_de_imoveis':
                output_filename = "portalzuk.csv"
            else:
                output_filename = f"portalzuk_{''.join(c if c.isalnum() else '_' for c in filename_suffix)}.csv"

            self.export_to_csv(final_properties, filename=output_filename)
            
            end_time = time.time()
            print(f"Processo concluído com sucesso em {end_time - start_time:.2f} segundos!")
            
        except Exception as e:
            print(f"Erro durante a execução: {str(e)}")
            traceback.print_exc()
        finally:
            if self.driver:
                self.driver.quit()

# scraper = PortalzukScraper()
# scraper.run()