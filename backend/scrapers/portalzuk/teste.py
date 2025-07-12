import csv
import re
import time
import traceback
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import requests

class PortalzukScraper:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=options)
        self.base_url = "https://www.portalzuk.com.br/imoveis"

    def load_all_properties(self):
        self.driver.get(self.base_url)
        while True:
            try:
                ver_mais = self.driver.find_element(By.CLASS_NAME, "btn-carregar")
                self.driver.execute_script("arguments[0].click();", ver_mais)
                time.sleep(2)
            except Exception:
                break

    def scrapMainPage(self, url):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        cards = soup.select("div#content-imoveis .card-imovel")
        dados = []

        for card in cards:
            rotulo = card.select_one(".title-imovel").get_text(strip=True) if card.select_one(".title-imovel") else ""
            valor = card.select_one(".valor-imovel").get_text(strip=True) if card.select_one(".valor-imovel") else ""
            endereco = card.select_one(".local-imovel").get_text(strip=True) if card.select_one(".local-imovel") else ""
            lote = card.select_one(".lote-imovel").get_text(strip=True) if card.select_one(".lote-imovel") else ""
            link = card.find("a")["href"] if card.find("a") else ""
            if link and not link.startswith("http"):
                link = f"https://www.portalzuk.com.br{link}"

            dados.append({
                "Rotulo": rotulo,
                "Valor (R$)": valor,
                "Endereco": endereco,
                "Lote": lote,
                "link": link,
                "Data": datetime.now().strftime("%Y-%m-%d")
            })

        return dados

    def scrapItensPages(self, url):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                return {}
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.select_one("div.info-imovel table")
            extras = {}

            if table:
                rows = table.find_all("tr")
                for row in rows:
                    th = row.find("th")
                    td = row.find("td")
                    if th and td:
                        key = th.get_text(strip=True).replace(":", "")
                        value = td.get_text(strip=True)
                        extras[key] = value

            return extras
        except Exception:
            return {}

    def run(self):
        try:
            self.load_all_properties()
            dados = self.scrapMainPage(self.base_url)
            links = [d["link"] for d in dados if d.get("link")]

            if not links:
                print("Nenhum link válido encontrado.")
                return

            with ThreadPoolExecutor(max_workers=min(len(links), cpu_count() * 2)) as executor:
                futures = {executor.submit(self.scrapItensPages, link): link for link in links}
                extra_infos = []
                for future in as_completed(futures):
                    try:
                        extra_info = future.result()
                        extra_infos.append(extra_info)
                    except Exception as e:
                        print(f"Erro ao obter dados de {futures[future]}: {e}")
                        extra_infos.append({})

            for data, extra_info in zip(dados, extra_infos):
                if extra_info:
                    data.update(extra_info)

            all_keys = set()
            for d in dados:
                all_keys.update(d.keys())
            all_keys.discard("link")

            fieldnames = [
                "Rotulo", "Valor (R$)", "Data", "Lote", "Endereco",
            ] + sorted(k for k in all_keys if k not in [
                "Rotulo", "Valor (R$)", "Data", "Lote", "Endereco",
            ])

            with open("portalzuk.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for d in dados:
                    d.pop("link", None)
                writer.writerows(dados)

            return {
                "properties": dados,
                "metadata": {
                    "source": "portalzuk",
                    "scraped_at": datetime.now().isoformat(),
                    "count": len(dados)
                }
            }

        except Exception as e:
            return {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        finally:
            self.driver.quit()
            print("\nDados exportados para 'portalzuk.csv'\n")
if __name__ == "__main__":
    scraper = PortalzukScraper()
    resultado = scraper.run()
    print(resultado)
