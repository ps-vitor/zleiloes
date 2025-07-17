from bs4 import BeautifulSoup
from urllib.parse import urlparse
from cachetools import TTLCache
import  requests,traceback,time
from    .requestManager  import  RequestManager
from    .circuitBreaker import  CircuitBreaker

class DataExtractor:
    def __init__(self, base_url=None, request_manager=None):
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.base_url = base_url
        
        if request_manager is None:
            raise ValueError("RequestManager deve ser fornecido")
        
        self.request_manager = request_manager
        if not hasattr(request_manager, 'session'):
            raise ValueError("O RequestManager fornecido deve ter um atributo 'session'")
        
        if not hasattr(request_manager, 'random_delay'):
            raise ValueError("O RequestManager fornecido deve ter um método 'random_delay'")

        self.circuit_breaker=CircuitBreaker()

    def is_valid_url(self, url):
        """Verifica se uma URL é válida."""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def extract_image_urls(self, html_content):
        """Extrai URLs de imagens da página do imóvel."""
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
            print(f"Erro ao extrair URLs de imagem: {e}")
            return []

    def scrap_nested_page(self, url):
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
            self.request_manager.random_delay()
            response = self.request_manager.session.get(url, timeout=15)

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
        """Agora usa self.request_manager internamente"""
        extra_data = {} 
        if not self.is_valid_url(url):
            print(f"[AVISO] URL inválida para scrapItensPages: {url}")
            return extra_data

        try:
            self.request_manager.random_delay()  # Usa o request_manager da instância
            
            response = self.request_manager.session.get(url, timeout=20)

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
                        # Usa self.base_url que deve ser definido no __init__
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
                    traceback.print_exc()
                    continue

        except Exception as e:
            print(f"[ERRO] scrapMainPage: {str(e)}")
            traceback.print_exc()

        return properties


    def _clean_price(self, price_text):
        """Limpa e formata o valor do preço."""
        return price_text.replace("R$", "").replace(".", "").replace(",", ".").strip()