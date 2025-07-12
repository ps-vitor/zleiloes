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

    def get_property_details(self, url):
        return self.scrapItensPages(url)

    def scrapItensPages(self, url):
        extra_data = {}
        try:
            if url is None:
                print("[ERRO] URL está None!")
                return extra_data

            html = self.session.get(url, timeout=20)
            soup = BeautifulSoup(html.text, "html.parser")

            itens_div = soup.find_all("div", class_="property-featured-items")
            for itens in itens_div:
                all_itens = itens.find_all("div", class_="property-featured-item")
                for iten in all_itens:
                    label = iten.find("span", class_="property-featured-item-label")
                    value = iten.find("span", class_="property-featured-item-value")
                    if label and value:
                        extra_data[label.get_text(strip=True)] = value.get_text(strip=True)

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
        print("Acessando a página e carregando todos os imóveis...")
        self.driver.get(self.base_url)
        time.sleep(3)
        self.close_popups()

        current_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'div.card-property'))
        max_attempts = 35
        print(f"Imóveis iniciais carregados: {current_count}")

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
                print(f"Total de imóveis carregados: {current_count}")
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"Erro ao carregar mais imóveis: {str(e)}")
                break

        return self.driver.page_source

    def extract_property_data(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        properties = {}
        cards = soup.find_all(class_="card-property")
        print(f"Total de imóveis encontrados: {len(cards)}")

        for card in cards:
            try:
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

    def enrich_with_details(self, properties):
        print("Enriquecendo imóveis com dados individuais...")
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            future_to_url = {executor.submit(self.get_property_details, url): url for url in properties.keys()}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    details = future.result()
                    properties[url].update(details)
                except Exception as e:
                    print(f"[ERRO] Falha ao processar {url}: {e}")
        return properties

    def export_to_csv(self, properties, filename="portalzuk.csv"):
        try:
            if not properties:
                print("Nenhum dado para exportar")
                return False

            rows = []
            for link, data in properties.items():
                base_data = {
                    'Lote': data.get('Lote'),
                    'Endereco': data.get('Endereco'),
                    'link': link,
                    **{k: v for k, v in data.items() if k not in ['Lote', 'Endereco', 'link', 'Precos']}
                }

                if not data['Precos']:
                    rows.append(base_data)
                else:
                    for price in data['Precos']:
                        rows.append({**base_data, **price})

            fieldnames = sorted(set(k for row in rows for k in row))
            with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            print(f"Exportado para {filename}")
            return True

        except Exception as e:
            print(f"[ERRO] Falha ao exportar CSV: {str(e)}")
            return False

    def run(self):
        html = self.load_all_properties()
        base_data = self.extract_property_data(html)
        enriched_data = self.enrich_with_details(base_data)
        self.export_to_csv(enriched_data)
        self.driver.quit()


if __name__ == "__main__":
    scraper = PortalzukScraper()
    scraper.run()
