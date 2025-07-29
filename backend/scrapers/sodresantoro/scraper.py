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
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import random
from    ..lib.req_rules import  ReqRules
from    ..lib.circuit_breaker   import  CircuitBreaker

class SodreSantoroScraper:
    def __init__(self, delay=2.0, max_workers=None):
        self.delay = delay
        self.max_workers = max_workers or min(10, cpu_count() * 2)
        self.base_url = "https://www.sodresantoro.com.br"
        self.session = ReqRules.create_requests_session()
        self.circuit_breaker = CircuitBreaker(max_failures=3, reset_timeout=120)
        self.last_request_time = time.time()
        
        # Configure Chrome options
        options = Options()
        options.headless = True
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize WebDriver
        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, 20)

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()

    def _random_delay(self):
        """Adiciona atraso aleatório entre requisições"""
        time_since_last = time.time() - self.last_request_time
        sleep_time = random.uniform(self.delay, self.delay + 5.0)
        
        if time_since_last < sleep_time:
            time.sleep(sleep_time - time_since_last)
        
        additional_delay = random.uniform(0.5, 2.0)
        time.sleep(additional_delay)
        
        self.last_request_time = time.time()

    def _get_soup(self, url=None, source=None):
        """Helper method to get BeautifulSoup object from URL or page source."""
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
        """Clean and normalize the URL."""
        if url.startswith('http'):
            return url
        return urljoin(self.base_url, url)

    def scrap_item_page(self, url):
        """Scrape detailed information from an individual property page."""
        extra_data = {
            "descricao": "n/a",
            "forma_pagamento": "n/a",
            "metragem": "n/a",
            "localizacao": "n/a",
            "caracteristicas": "n/a"
        }
        
        try:
            soup = self._get_soup(url)
            if not soup:
                return extra_data

            # Description
            descricao = soup.find("div", id="detail_info_lot_description")
            if descricao:
                extra_data["descricao"] = descricao.get_text(strip=True)

            # Payment options
            pagamento = soup.find("div", id="payments_options")
            if pagamento:
                extra_data["forma_pagamento"] = pagamento.get_text(" | ", strip=True)

            # Dimensions and features
            features_container = soup.find("div", class_="grid grid-cols-2 gap-4")
            if features_container:
                features = [f.get_text(strip=True) for f in features_container.find_all("div", recursive=False)]
                extra_data["metragem"] = features[0] if len(features) > 0 else "n/a"
                extra_data["caracteristicas"] = " | ".join(features[1:]) if len(features) > 1 else "n/a"

            # Location
            location = soup.find("div", class_="text-sm text-gray-500")
            if location:
                extra_data["localizacao"] = location.get_text(strip=True)

        except Exception as e:
            print(f"Error scraping item page {url}: {e}")
            traceback.print_exc()

        return extra_data

    def scrap_main_page(self):
        """Scrape the main listing page and collect property cards."""
        results = []
        links = []
        page = 1
        
        while True:
            try:
                url = f"{self.base_url}/imoveis/lotes?page={page}"
                print(f"Scraping page {page}: {url}")
                
                self.driver.get(url)
                time.sleep(self.delay)
                
                # Wait for cards to load
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.relative.flex-none.h-fit.rounded-xl"))
                )
                
                # Get the current page source and parse
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                cards = soup.find_all('div', class_="relative flex-none h-fit rounded-xl overflow-hidden bg-white border")
                
                if not cards:
                    print(f"No cards found on page {page}, stopping...")
                    break
                
                for card in cards:
                    try:
                        data = card.find("span", class_="text-sm line-clamp-1 font-semibold")
                        title = card.find('h2', class_="text-lg font-bold")
                        price = card.find('p', class_="text-2xl text-blue-700 font-medium")
                        link_tag = card.find("a", href=True)
                        
                        # Clean and normalize the URL
                        link = self._clean_url(link_tag['href']) if link_tag else None
                        
                        item = {
                            'data': data.get_text(strip=True) if data else "n/a",
                            'titulo': title.get_text(strip=True) if title else "n/a",
                            'preco': price.get_text(strip=True) if price else "n/a",
                            'link': link,
                            'pagina': page
                        }
                        results.append(item)
                        if link:
                            links.append(link)
                    except Exception as e:
                        print(f"Error processing card on page {page}: {e}")
                        continue
                    
                # Check for next page button
                pagination = soup.find('nav', {'aria-label': 'Navegação de Paginação'})
                if pagination:
                    page_buttons = pagination.find_all('button')
                    next_page_buttons = [btn for btn in page_buttons 
                                        if btn.get_text(strip=True).isdigit() 
                                        and int(btn.get_text(strip=True)) == page + 1]
                    
                    if next_page_buttons:
                        # Try to click the next page button using Selenium
                        try:
                            next_page_btn = self.driver.find_element(
                                By.XPATH,
                                f"//nav[@aria-label='Navegação de Paginação']//button[contains(., '{page + 1}')]"
                            )
                            next_page_btn.click()
                            time.sleep(self.delay + 2)  # Extra delay for page load
                            page += 1
                        except Exception as e:
                            print(f"Failed to click page {page + 1} button: {e}")
                            break
                    else:
                        print("No next page button found.")
                        break
                # else: 
                    # print("No pagination found, stopping...")
                    # break
                
                if page > 50:  # Safety limit
                    print("Reached page limit (50), stopping...")
                    break
                
            except Exception as e:
                print(f"Error processing page {page}: {e}")
                traceback.print_exc()
                break
            
        return results, links

    def run(self):
        """Main method to run the scraper."""
        try:
            print("Starting main page scraping...")
            dados, links = self.scrap_main_page()
            
            if not dados:
                print("No properties found.")
                return {"error": "No properties found."}

            print(f"Found {len(dados)} properties. Scraping details...")
            
            # Parallel processing of detail pages
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.scrap_item_page, link): idx 
                          for idx, link in enumerate(links) if link}
                
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        extra_info = future.result()
                        dados[idx].update(extra_info)
                    except Exception as e:
                        print(f"Error getting details for property {idx}: {e}")
                        dados[idx].update({k: "error" for k in [
                            "descricao", "forma_pagamento", "metragem", 
                            "localizacao", "caracteristicas"
                        ]})

            # Prepare CSV data
            fieldnames = [
                "data", "titulo", "preco", "localizacao", 
                "descricao", "forma_pagamento", "metragem", 
                "caracteristicas", "pagina"
            ]

            # Export to CSV
            filename = f"sodresantoro.csv"
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for d in dados:
                    d.pop("link", None)  # Remove link from final output
                    writer.writerow(d)

            return {
                "properties": dados,
                "metadata": {
                    "source": "sodresantoro",
                    "scraped_at": datetime.now().isoformat(),
                    "count": len(dados),
                    "pages_scraped": dados[-1]["pagina"] if dados else 0,
                    "output_file": filename
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