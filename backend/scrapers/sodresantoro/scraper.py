import requests, time, traceback, csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from urllib.parse import urljoin
import random
from lib.req_rules import ReqRules
from lib.circuit_breaker import CircuitBreaker
from selenium.common.exceptions import TimeoutException

class SodreSantoroScraper:
    def __init__(self, delay=2.0, max_workers=None):
        self.delay = delay
        self.max_workers = max_workers or min(10, cpu_count() * 2)
        self.base_url = "https://www.sodresantoro.com.br"
        self.session = ReqRules.create_requests_session()
        self.circuit_breaker = CircuitBreaker(max_failures=3, reset_timeout=120)
        self.last_request_time = time.time()

        options = Options()
        options.headless = True
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, 30)

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()

    def _random_delay(self):
        time_since_last = time.time() - self.last_request_time
        sleep_time = random.uniform(self.delay, self.delay + 5.0)
        if time_since_last < sleep_time:
            time.sleep(sleep_time - time_since_last)
        time.sleep(random.uniform(0.5, 2.0))
        self.last_request_time = time.time()

    def _get_soup(self, url=None, source=None):
        if source:
            return BeautifulSoup(source, 'html.parser')
        try:
            self._random_delay()
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"403 Forbidden error encountered. Rotating session and retrying...")
                self.circuit_breaker.record_failure()
                self.session = ReqRules.create_requests_session()
                try:
                    self._random_delay()
                    response = self.session.get(url, timeout=20)
                    response.raise_for_status()
                    return BeautifulSoup(response.text, 'html.parser')
                except Exception as retry_error:
                    print(f"Retry failed for {url}: {retry_error}")
                    return None
            else:
                print(f"HTTP error {e.response.status_code} for {url}")
                return None
        except Exception as e:
            print(f"Error getting {url}: {e}")
            return None

    def _clean_url(self, url):
        return url if url.startswith('http') else urljoin(self.base_url, url)

    def scrap_item_page(self, url):
        extra_data = {
            "descricao": "n/a",
            "forma_pagamento": "n/a",
            "metragem": "n/a",
            "caracteristicas": "n/a",
            "cidade": "n/a",
            "bairro": "n/a",
            "endereco": "n/a",
            "tipo_imovel": "n/a",
            "processo_link": "n/a",
            "leiloeiro": "n/a"
        }
        try:
            soup = self._get_soup(url)
            if not soup:
                return extra_data
            
            swiper_wrapper = soup.find("div", {"data-swiper-target": "mainSlider"})
            if swiper_wrapper:
                img_tags = swiper_wrapper.find_all("img", src=True)
                for i, img in enumerate(img_tags[:3]):
                    extra_data[f"imagem{i+1}"] = img["src"]

            descricao = soup.find("div", id="detail_info_lot_description")
            if descricao:
                extra_data["descricao"] = descricao.get_text(strip=True)

            pagamento = soup.find("div", id="payments_options")
            if pagamento:
                extra_data["forma_pagamento"] = pagamento.get_text(" | ", strip=True)

            features_container = soup.find("div", class_="grid grid-cols-2 gap-4")
            if features_container:
                features = [f.get_text(strip=True) for f in features_container.find_all("div", recursive=False)]
                if features:
                    extra_data["metragem"] = features[0]
                    extra_data["caracteristicas"] = " | ".join(features[1:]) if len(features) > 1 else "n/a"

            if (cidade := soup.find("div", id="detail_info_property_city_state")):
                cidade_texto = cidade.get_text(strip=True)
                extra_data["cidade"] = cidade_texto.split(":")[1].strip()

            if (bairro := soup.find("div", id="detail_info_property_neighborhood")):
                bairro_texto=bairro.get_text(strip=True)
                extra_data["bairro"] = bairro_texto.split(":")[1].strip()

            if (endereco := soup.find("div", id="detail_info_property_address")):
                endereco_texto=endereco.get_text(strip=True)
                extra_data["endereco"] = endereco_texto.split(":")[1].strip()

            if (tipo_imovel := soup.find("div", id="detail_info_property_category")):
                tipo_imovel_texto=tipo_imovel.get_text(strip=True)
                extra_data["tipo_imovel"] = tipo_imovel_texto.split(":")[1].strip()

            if (processo := soup.find("div", id="aditionalInfoLot_tj_number_process")):
                if (processo_link := processo.find("a", href=True)):
                    extra_data["processo_link"] = processo_link['href']

            if (leiloeiro := soup.find("div", id="aditionalInfoLot_leiloeiro")):
                leiloeiro_texto=leiloeiro.get_text(strip=True)
                extra_data["leiloeiro"] = leiloeiro_texto.split(":")[1].strip()

            if ocupado := soup.find("div", id="extraLabelLot"):
                # Procura especificamente pela tag que contém "Desocupado" ou "Ocupado"
                status_tag = ocupado.find("span", string=lambda text: text and ("Desocupado" in text or "Ocupado" in text))

                if status_tag:
                    status_text = status_tag.get_text(strip=True)
                    if "DESOCUPADO" in status_text.upper():
                        extra_data["ocupado"] = "Desocupado"
                    elif "OCUPADO" in status_text.upper():
                        extra_data["ocupado"] = "Ocupado"
                    else:
                        extra_data["ocupado"] = "n/a"
                else:
                    extra_data["ocupado"] = "n/a"


        except Exception as e:
            print(f"Error scraping item page {url}: {e}")
            traceback.print_exc()

        return extra_data

    def scrap_main_page(self):
        results = []
        links = []
        page = 1

        while True:
            try:
                url = f"{self.base_url}/imoveis/lotes?page={page}"
                print(f"Scraping page {page}: {url}")
                self.driver.get(url)
                time.sleep(3)  # Increased wait time

                # Debug: Save page source
                with open(f"debug_page_{page}.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)

                # Wait for cards using the exact structure from HTML
                try:
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex > a.wrapper"))
                    )
                except TimeoutException:
                    print("No property cards found, ending pagination")
                    break
                
                # Get all property cards using the exact parent-child relationship
                cards = self.driver.find_elements(By.CSS_SELECTOR, "div.flex > a.wrapper")
                print(f"Found {len(cards)} property cards on page {page}")

                if not cards:
                    break

                for card in cards:
                    try:
                        # Extract data using the exact class names from HTML
                        link = card.get_attribute("href")

                        # Title - from the exact structure
                        title = card.find_element(
                            By.CSS_SELECTOR, "div.text-body-medium span").text

                        # Price - from the exact structure
                        price = card.find_element(
                            By.CSS_SELECTOR, "div.p-2 p.text-headline-small").text

                        # Address - from the exact structure
                        address = card.find_element(
                            By.CSS_SELECTOR, "div.text-body-small.uppercase").text

                        # Auction date - from the exact structure
                        date_element = card.find_element(
                            By.CSS_SELECTOR, "div.flex div.inline-flex span.text-body-small")
                        date = date_element.text if date_element else "n/a"

                        # Images - from the exact structure
                        images = []
                        img_elements = card.find_elements(By.CSS_SELECTOR, "picture img")
                        for img in img_elements[:3]:  # Get first 3 images only
                            if img.get_attribute("src"):
                                images.append(img.get_attribute("src"))

                        # Occupancy status - from the exact structure
                        ocupado = "n/a"
                        try:
                            status_btn = card.find_element(
                                By.CSS_SELECTOR, "button.wrapper span.label")
                            if status_btn:
                                ocupado = status_btn.text
                        except:
                            pass
                        
                        results.append({
                            'url': url,
                            'link': link,
                            'preco': price,
                            'titulo': title,
                            'endereco': address,
                            'data_leilao': date,
                            'ocupado': ocupado,
                            'imagens': images
                        })
                        links.append(link)

                    except Exception as e:
                        print(f"Error processing card: {str(e)[:100]}...")
                        continue
                    
                # Pagination - more robust approach
                try:
                    # Try next page button
                    next_buttons = self.driver.find_elements(
                        By.XPATH, "//button[contains(., 'Próxima') or contains(., 'Next') or contains(., '›')]")
                    if next_buttons:
                        next_buttons[0].click()
                        page += 1
                        time.sleep(3)
                        continue
                    
                    # Try numbered pagination
                    next_page = self.driver.find_element(
                        By.XPATH, f"//button[text()='{page + 1}']")
                    next_page.click()
                    page += 1
                    time.sleep(3)
                except:
                    print("No more pages available")
                    break

            except Exception as e:
                print(f"Error processing page {page}: {str(e)[:100]}...")
                traceback.print_exc()
                break

        return results, links

    def run(self):
        try:
            print("Starting main page scraping...")
            dados, links = self.scrap_main_page()

            if not dados:
                print("No properties found.")
                return {"error": "No properties found."}

            print(f"Found {len(dados)} properties. Scraping details...")

            # Primeiro passamos por todos os itens para descobrir o número máximo de imagens
            max_images = 0
            for d, link in zip(dados, links):
                if link:
                    extra_info = self.scrap_item_page(link)
                    # Verifica quantas imagens tem este item
                    img_count = sum(1 for key in extra_info if key.startswith('imagem'))
                    max_images = max(max_images, img_count)
                    d.update(extra_info)

            # Define as colunas baseadas no máximo encontrado
            fieldnames = [
                "link", "preco", "descricao", "forma_pagamento",
                "cidade", "bairro", "endereco", "tipo_imovel", 
                "processo_link", "leiloeiro","ocupado"
            ]

            # Adiciona apenas as colunas de imagem que existem
            for i in range(1, max_images + 1):
                fieldnames.append(f"imagem{i}")

            filename = f"sodresantoro.csv"
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for d in dados:
                    # d.pop("link", None)
                    # d["url"] = d.pop("link")
                    row = {k: d.get(k, "n/a") for k in fieldnames}
                    writer.writerow(row)

            return {
                "properties": dados,
                "metadata": {
                    "source": "sodresantoro",
                    "scraped_at": datetime.now().isoformat(),
                    "count": len(dados),
                    "pages_scraped": dados[-1]["pagina"] if dados else 0,
                    "output_file": filename,
                    "max_images_found": max_images
                }
            }

        except Exception as e:
            return {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        finally:
            if hasattr(self, 'driver'):
                self.driver.quit()
            print("\nScraping completed.\n")

if __name__ == "__main__":
    scraper = SodreSantoroScraper(delay=2.0)
    result = scraper.run()
    print(result.get("metadata", {}))
