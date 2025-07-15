import csv
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class PortalzukScraper:
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
        
        # Set initial User-Agent for Selenium
        self.current_user_agent = random.choice(self.user_agents)
        self.options.add_argument(f'user-agent={self.current_user_agent}')

        self.driver = webdriver.Chrome(options=self.options)
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis"
        
        self.session = self._create_requests_session()

        # Ajustes nos intervalos de tempo para maior eficiência
        self.last_request_time = 0
        self.min_request_interval = 3.0 # Reduzido para 0.3s como base
        self.max_request_interval_addition = 4.0 # Adição máxima para atraso aleatório, totalizando 0.3s a 1.3s
        self.max_workers = 12 # Aumentando o número de threads para requisições paralelas (experimente com 8 ou 16 também)

    def _create_requests_session(self):
        """Cria e configura uma nova sessão requests com retry e User-Agent rotativo."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5, # Reduzido o número de retries para evitar esperas muito longas em falhas persistentes
            backoff_factor=1.5, # Backoff ligeiramente menor
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.portalzuk.com.br/',
            'User-Agent': random.choice(self.user_agents) # Rotaciona User-Agent para cada nova sessão
        })
        return session

    def random_delay(self):
        """Adiciona atraso aleatório e mais variável entre requisições para evitar ser bloqueado."""
        time_since_last = time.time() - self.last_request_time
        # Aumenta a variabilidade e o tempo mínimo de espera
        sleep_time = random.uniform(self.min_request_interval, self.min_request_interval + self.max_request_interval_addition)
        if time_since_last < sleep_time:
            time.sleep(sleep_time - time_since_last)
        self.last_request_time = time.time()
        
    def close_popups(self):
        """Tenta fechar popups que podem atrapalhar a navegação."""
        # Tentativa de fechar dropdown de cidade se aparecer
        try:
            city_dropdown = WebDriverWait(self.driver, 2).until( # Reduzido timeout
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.select2-selection__rendered'))
            )
            self.driver.execute_script("arguments[0].style.display = 'none';", city_dropdown)
            print("Popup de cidade ocultado.")
        except (NoSuchElementException, TimeoutException):
            pass # Popup de cidade não encontrado ou não apareceu

        # Tentativa de fechar botões genéricos de fechar popups
        try:
            close_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.close, .modal-close, [aria-label="Close"]')
            for button in close_buttons:
                try:
                    if button.is_displayed() and button.is_enabled():
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.2) # Pequena pausa após clicar para o popup fechar
                        print("Botão de fechar popup clicado.")
                except WebDriverException: 
                    continue
        except Exception:
            pass

    def is_valid_url(self, url):
        """Verifica se a URL é válida."""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def extract_image_urls(self, html_content):
        """Extrai URLs de imagens do conteúdo HTML."""
        soup = BeautifulSoup(html_content, "html.parser")
        image_urls = []
        img_tags = soup.find_all("img", class_="img-fluid") 
        for img in img_tags:
            if img.has_attr("src"):
                src = img["src"]
                # Filtra placeholders e imagens padrão
                if "placeholder" not in src and "default" not in src:
                    image_urls.append(src)
        return image_urls
    
    def _scrap_nested_page(self, url):
        """
        Scrapeia uma URL aninhada (e.g., página de processo/banco).
        Retorna um dicionário contendo dados específicos (leiloeiro) e o HTML bruto (limitado).
        Garante um dicionário de retorno mesmo em caso de erro para não perder o registro pai.
        """
        nested_data = {
            "leiloeiro": None,
        }
        if not self.is_valid_url(url):
            nested_data["leiloeiro"] = "URL inválida para página aninhada"
            return nested_data

        # self.random_delay() # O delay será gerenciado pelo ThreadPoolExecutor com a sessão requests
        try:
            response = self.session.get(url, timeout=20) # Reduzido timeout
            response.raise_for_status() 
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            leiloeiro_label = soup.find("h1", class_="whitelabel-title")
            leiloeiro=leiloeiro_label.get_text(strip=True)
            nested_data["leiloeiro"] =  leiloeiro.replace("Leilão de imóveis ","")if leiloeiro_label else "leiloeiro não encontrado"
            max_content_length = 5000 

            return nested_data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"[BLOQUEADO] Erro 403 (Forbidden) para URL aninhada {url}: Tentando com nova sessão...")
                self.session = self._create_requests_session() 
                try:
                    response = self.session.get(url, timeout=20) # Re-tentativa com nova sessão e timeout reduzido
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    leiloeiro_label = soup.find("h1", class_="whitelabel-title")
                    nested_data["leiloeiro"] = leiloeiro_label.get_text(strip=True) if leiloeiro_label else "leiloeiro não encontrado (re-tentativa)"
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
            response = self.session.get(url, timeout=30) 
            response.raise_for_status() 

            soup = BeautifulSoup(response.text, "html.parser")
            if not soup: 
                print(f"[ERRO] BeautifulSoup não conseguiu parsear: {url}")
                return extra_data
            
            # --- Início da extração de dados ---
            features = soup.find_all("div", class_="property-featured-item")
            for feature in features:
                label = feature.find("span", class_="property-featured-item-label")
                value = feature.find("span", class_="property-featured-item-value")
                if label and value:
                    extra_data[label.get_text(strip=True)] = value.get_text(strip=True)
            
            status = soup.find("span", class_="property-status-title")
            if status:
                extra_data["Status Leilão"] = status.get_text(strip=True)

            matricula = soup.find("p", {"id": "itens_matricula"})
            if matricula:
                extra_data["Matrícula"] = matricula.get_text(strip=True)
            
            observacoes_geral = soup.find("div", class_="div-text-observacoes")
            if observacoes_geral:
                extra_data["Observações Gerais"] = observacoes_geral.get_text(strip=True)
            
            box_action_bank_figure = soup.find("figure", class_="box-action-bank")
            if box_action_bank_figure:
                process_link_tag = box_action_bank_figure.find("a", href=True)
                if process_link_tag and self.is_valid_url(process_link_tag['href']):
                    extra_data["Link do Processo Judicial"] = process_link_tag['href']
            else: 
                processo = soup.find("a", class_="glossary-link")
                if processo and processo.has_attr("href") and self.is_valid_url(processo['href']):
                    extra_data["Link do Processo"] = processo["href"]
            
            # --- MODIFICAÇÃO PARA EXTRAIR "Visitação" com seletor específico ---
            visitation_element = soup.select_one('div.visit-content p')
            if visitation_element:
                extra_data["Visitação"] = visitation_element.get_text(strip=True)
            else:
                extra_data["Visitação"] = "N/A" # Definir como N/A se não encontrado
            
            pagamentos_section = soup.find("h3", class_="property-info-title", string=lambda text: text and "Formas de Pagamento" in text)
            if pagamentos_section:
                payment_list = pagamentos_section.find_next_sibling("ul", class_="property-payments")
                if payment_list:
                    items = payment_list.find_all("li", class_="property-payments-item")
                    pagamento_info = [item.find("p", class_="property-payments-item-text").get_text(strip=True) for item in items if item.find("p", class_="property-payments-item-text")]
                    if pagamento_info:
                        extra_data["Formas de Pagamento"] = " | ".join(pagamento_info)

            pref_tag = soup.find("div", class_="property-info-text div-text-preferencia")
            if pref_tag:
                extra_data["Direito de Preferência"] = pref_tag.get_text(strip=True)
      
            # --- MODIFICAÇÃO PARA EXTRAIR "Descrição do imóvel" com seletor específico ---
            description_element = soup.select_one('div.description-content p')
            if description_element:
                extra_data["Descrição do imóvel"] = description_element.get_text(strip=True)
            else:
                extra_data["Descrição do imóvel"] = "N/A" # Definir como N/A se não encontrado

            comments_div = soup.find("div", class_="property-info property-info-comments")
            if comments_div:
                texto_comments = comments_div.find("p", class_="property-hide-show")
                if texto_comments:
                    extra_data["Observações Adicionais"] = texto_comments.get_text(strip=True)
            
            status_elements = soup.find_all("div", class_="property-status")
            for status_div in status_elements:
                title_div = status_div.find("span", class_="property-status-title")
                text_div = status_div.find("p", class_="property-status-text")
                if title_div and text_div:
                    title = title_div.get_text(strip=True)
                    text = text_div.get_text(strip=True)
                    
                    if "Imóvel ocupado" in title or "Imóvel desocupado" in title:
                        extra_data["ocupado"] = title
                        extra_data["ocupado"] = text
                    elif "Direitos do Compromissário Comprador" in title:
                        extra_data["Direitos do Compromissário"] = title
                        extra_data["Detalhes dos Direitos"] = text
                        # saiba_mais = text_div.find("a")
                        # if saiba_mais and "href" in saiba_mais.attrs:
                            # extra_data["Link Saiba Mais"] = saiba_mais["href"]
            
            image_urls = self.extract_image_urls(response.text)
            for idx, url_img in enumerate(image_urls, start=1): 
                extra_data[f"Foto_{idx}"] = url_img
            extra_data["Total_Fotos"] = len(image_urls)
            # --- Fim da extração de dados ---

            return extra_data 

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"[BLOQUEADO] Erro 403 (Forbidden) para {url}: O site pode ter detectado o scraper. Tentando com nova sessão...")
                self.session = self._create_requests_session()
                try:
                    response = self.session.get(url, timeout=30) 
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    if soup:
                        # Re-extrai todos os dados com a nova sessão
                        features = soup.find_all("div", class_="property-featured-item")
                        for feature in features:
                            label = feature.find("span", class_="property-featured-item-label")
                            value = feature.find("span", class_="property-featured-item-value")
                            if label and value:
                                extra_data[label.get_text(strip=True)] = value.get_text(strip=True)
                        
                        status = soup.find("span", class_="property-status-title")
                        if status: extra_data["Status Leilão"] = status.get_text(strip=True)
                        matricula = soup.find("p", {"id": "itens_matricula"})
                        if matricula: extra_data["Matrícula"] = matricula.get_text(strip=True)
                        observacoes_geral = soup.find("div", class_="div-text-observacoes")
                        if observacoes_geral: extra_data["Observações Gerais"] = observacoes_geral.get_text(strip=True)
                        box_action_bank_figure = soup.find("figure", class_="box-action-bank")
                        if box_action_bank_figure:
                            process_link_tag = box_action_bank_figure.find("a", href=True)
                            if process_link_tag and self.is_valid_url(process_link_tag['href']):
                                extra_data["Link do Processo Judicial"] = process_link_tag['href']
                        else: 
                            processo = soup.find("a", class_="glossary-link")
                            if processo and processo.has_attr("href") and self.is_valid_url(processo['href']):
                                extra_data["Link do Processo"] = processo["href"]
                        
                        # --- MODIFICAÇÃO PARA "Visitação" na re-tentativa ---
                        visitation_element = soup.select_one('div.visit-content p')
                        if visitation_element:
                            extra_data["Visitação"] = visitation_element.get_text(strip=True)
                        else:
                            extra_data["Visitação"] = "N/A (re-tentativa)"

                        pagamentos_section = soup.find("h3", class_="property-info-title", string=lambda text: text and "Formas de Pagamento" in text)
                        if pagamentos_section:
                            payment_list = pagamentos_section.find_next_sibling("ul", class_="property-payments")
                            if payment_list:
                                items = payment_list.find_all("li", class_="property-payments-item")
                                pagamento_info = [item.find("p", class_="property-payments-item-text").get_text(strip=True) for item in items if item.find("p", class_="property-payments-item-text")]
                                if pagamento_info: extra_data["Formas de Pagamento"] = " | ".join(pagamento_info)
                        pref_tag = soup.find("div", class_="property-info-text div-text-preferencia")
                        if pref_tag: extra_data["Direito de Preferência"] = pref_tag.get_text(strip=True)
                        
                        # --- MODIFICAÇÃO PARA "Descrição do imóvel" na re-tentativa ---
                        description_element = soup.select_one('div.description-content p')
                        if description_element:
                            extra_data["Descrição do imóvel"] = description_element.get_text(strip=True)
                        else:
                            extra_data["Descrição do imóvel"] = "N/A (re-tentativa)"

                        comments_div = soup.find("div", class_="property-info property-info-comments")
                        if comments_div:
                            texto_comments = comments_div.find("p", class_="property-hide-show")
                            if texto_comments: extra_data["Observações Adicionais"] = texto_comments.get_text(strip=True)
                        status_elements = soup.find_all("div", class_="property-status")
                        for status_div in status_elements:
                            title_div = status_div.find("span", class_="property-status-title")
                            text_div = status_div.find("p", class_="property-status-text")
                            if title_div and text_div:
                                title = title_div.get_text(strip=True)
                                text = text_div.get_text(strip=True)
                                if "Imóvel ocupado" in title or "Imóvel desocupado" in title:
                                    extra_data["Status de Ocupação"] = title
                                    extra_data["ocupado"] = text
                                elif "Direitos do Compromissário Comprador" in title:
                                    extra_data["Direitos do Compromissário"] = title
                                    extra_data["Detalhes dos Direitos"] = text
                                    saiba_mais = text_div.find("a")
                                    if saiba_mais and "href" in saiba_mais.attrs:
                                        extra_data["Link Saiba Mais"] = saiba_mais["href"]
                        image_urls = self.extract_image_urls(response.text)
                        for idx, url_img in enumerate(image_urls, start=1): 
                            extra_data[f"Foto_{idx}"] = url_img
                        extra_data["Total_Fotos"] = len(image_urls)
                        return extra_data
                    else:
                        print(f"[ERRO] BeautifulSoup não conseguiu parsear após retentativa: {url}")
                        return extra_data
                except requests.exceptions.RequestException as retry_e:
                    print(f"Erro de requisição mesmo após recriar sessão para {url}: {retry_e}")
                    return extra_data
            else:
                print(f"Erro de requisição para {url}: {e}")
                return extra_data
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição para {url}: {e}")
            return extra_data
        except Exception as e:
            print(f"Erro inesperado ao processar {url}: {e}")
            traceback.print_exc()
            return extra_data


    def load_all_properties(self, url):
        """Navega para a página principal e carrega todas as propriedades clicando em 'Carregar mais'."""
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
        max_attempts_without_new = 5 # Reduzido para 5 tentativas sem novos imóveis
        max_total_attempts = 50 # Reduzido limite absoluto
        current_total_attempts = 0

        while True:
            current_total_attempts += 1
            if current_total_attempts > max_total_attempts:
                print(f"Limite total de {max_total_attempts} tentativas de carregamento atingido. Parando...")
                break

            try:
                load_more_button = WebDriverWait(self.driver, 10).until( # Reduzido timeout para 10s
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Carregar mais")]'))
                )
                
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_button)
                time.sleep(0.5) # Pausa menor após o scroll
                
                # Tenta clicar com JS, que é mais robusto
                self.driver.execute_script("arguments[0].click();", load_more_button)
                
                # Espera por um novo elemento ou até que o contador de tentativas esgote
                WebDriverWait(self.driver, 20).until( # Reduzido timeout para 20s
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
                        print(f"Nenhum novo imóvel carregado após {max_attempts_without_new} tentativas. Assumindo que todos os imóveis foram carregados ou o botão parou de funcionar.")
                        break

                time.sleep(random.uniform(1.0, 2.0)) # Pausa menor e variável

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
                    
                    # Garante que a propriedade é adicionada mesmo se faltar tipo_imovel ou endereco
                    property_data = {
                        "tipo_imovel": tipo_imovel.get_text(strip=True) if tipo_imovel else "N/A",
                        "endereco": address.get_text(separator=" ", strip=True) if address else "N/A",
                        "Link": link if link else "N/A",
                        "Preços": prices if prices else [{"Tipo": "N/A", "Valor": "N/A", "Data": "N/A"}] # Garante ao menos um preço
                    }
                    properties.append(property_data)
                
                except Exception as e:
                    print(f"[ERRO] Erro ao processar card na página principal: {str(e)}")
                    # Não impede o processamento dos demais cards
                    continue

        except Exception as e:
            print(f"[ERRO] scrapMainPage: {str(e)}")
            traceback.print_exc()

        return properties

    def enrich_with_details(self, properties):
        """
        Enriquece a lista de propriedades com dados detalhados usando paralelismo.
        Não descarta propriedades mesmo que a extração de detalhes falhe.
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
                        # Não há necessidade de adicionar uma chave de erro se 'details' já retornou vazio.
                        # Os dados originais do imóvel já estão em enriched_properties[original_index]
                except Exception as e:
                    print(f"[ERRO] Falha ao enriquecer propriedade no índice {original_index} ({current_property_link}): {str(e)}")
                    traceback.print_exc()
                    enriched_properties[original_index]["ErroEnriquecimentoDetalhes"] = str(e)
                
                if processed_count % 50 == 0 or processed_count == len(future_to_index):
                    print(f"Progresso de enriquecimento de detalhes: {processed_count}/{len(future_to_index)} propriedades processadas.")
        
        return enriched_properties 

    def enrich_with_process_details(self, properties):
        """
        Para cada propriedade, se houver um link de processo (judicial ou geral),
        scrapeia o conteúdo dessa URL e adiciona aos detalhes da propriedade.
        Aplica paralelismo para maior eficiência e garante que nenhum dado seja perdido.
        """
        print(f"Buscando detalhes de páginas de processo para {len(properties)} propriedades usando paralelismo...")
        
        tasks_for_process_pages = []
        for prop in properties:
            process_link = prop.get("Link do Processo Judicial") or prop.get("Link do Processo")
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
                
                if processed_count % 50 == 0 or processed_count == len(tasks_for_process_pages):
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
            "tipo_imovel", "endereco", "Link", "rotulo", "Valor (R$)", "Data", 
            "ocupado", "Matrícula", "Link do Processo", 
            "leiloeiro", "Visitação", "Formas de Pagamento",
            "Direito de Preferência", "Descrição do imóvel", 
            "Observações Gerais", "Observações Adicionais", 
            "Direitos do Compromissário"
        ]
        
        photo_fields = sorted([f for f in fieldnames if f.startswith("Foto_")])
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
            print("Iniciando scraping...")
            
            start_time = time.time()
            
            target_url = start_url if start_url else self.base_url
            
            html = self.load_all_properties(target_url)
            
            properties = self.scrapMainPage(html)
            print(f"Propriedades encontradas na página principal: {len(properties)}")
            
            if not properties:
                print("Nenhuma propriedade encontrada na página principal para enriquecer. Encerrando.")
                return

            enriched_properties = self.enrich_with_details(properties)
            print(f"Propriedades enriquecidas com detalhes da página do imóvel: {len(enriched_properties)}")

            final_properties = self.enrich_with_process_details(enriched_properties)
            
            parsed_url = urlparse(target_url)
            filename_suffix = parsed_url.path.replace('/', '_').replace('-', '_').strip('_')
            if not filename_suffix or filename_suffix == 'leilao_de_imoveis':
                output_filename = "portalzuk_default1.csv"
            else:
                output_filename = f"portalzuk_{filename_suffix.replace(':', '_').replace('?', '_').replace('=', '_').replace('&', '_').replace('%', '_')}.csv"

            self.export_to_csv(final_properties, filename=output_filename)
            
            end_time = time.time()
            print(f"Processo concluído com sucesso em {end_time - start_time:.2f} segundos!")
            
        except Exception as e:
            print(f"Erro durante a execução: {str(e)}")
            traceback.print_exc()
        finally:
            if self.driver:
                self.driver.quit()

scraper = PortalzukScraper()
scraper.run()