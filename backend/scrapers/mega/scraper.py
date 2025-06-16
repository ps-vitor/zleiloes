# ./backend/scrapers/mega/scraper.py

from bs4 import BeautifulSoup
import requests, traceback, csv
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin

class MegaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        })

    def scrapItensPages(self, url):
        try:
            if not url:
                print("[ERRO] URL está vazia!")
                return {}

            resp = self.session.get(url, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")

            def get_text(selector, cls):
                el = soup.find(selector, class_=cls)
                return el.get_text(strip=True) if el else ""

            valor = get_text("div", "value")
            batch = get_text("div", "batch-type")
            leilao = get_text("div", "auction-id")
            local = get_text("div", "locality item")
            processoDiv = soup.find("div", class_="process-number item")
            processo = processoDiv.find("div", class_="value").get_text(strip=True) if processoDiv else ""
            processoLink = processoDiv.find("a")["href"] if processoDiv and processoDiv.find("a") else None
            data = get_text("span", "card-second-instance-date")

            return {
                "Valor (R$)": valor.replace("R$", "").replace(".", "").replace(",", ".").strip(),
                "Lote": batch,
                "Endereco": local,
                "Data": data,
                "Processo": processo,
                "ProcessoLink": processoLink,
                "Leilao": leilao,
            }

        except Exception:
            traceback.print_exc()
            return {}

    def scrapMainPage(self, url):
        results = []
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                print(f"[ERRO] Falha ao acessar {url} - Status {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all("div", class_="col-sm-6 col-md-4 col-lg-3")

            for card in cards:
                price_el = card.find("div", class_="card-price")
                link_el = card.find("a")
                if price_el and link_el and link_el.get("href"):
                    results.append({
                        "Rotulo": price_el.get_text(strip=True),
                        "link": urljoin(url, link_el["href"])
                    })
        except Exception:
            traceback.print_exc()

        return results

    def run(self):
        try:
            base_url = "https://www.megaleiloes.com.br/imoveis"
            dados = self.scrapMainPage(base_url)

            links = [d["link"] for d in dados if d.get("link")]
            if not links:
                print("Nenhum link válido encontrado.")
                return

            extra_infos = []
            with ThreadPoolExecutor(max_workers=min(len(links), cpu_count() * 2)) as executor:
                futures = {executor.submit(self.scrapItensPages, link): link for link in links}
                for future in as_completed(futures):
                    try:
                        extra_infos.append(future.result())
                    except Exception:
                        traceback.print_exc()
                        extra_infos.append({})

            for base, extra in zip(dados, extra_infos):
                base.update(extra)

            all_keys = set(k for d in dados for k in d if k != "link")
            fieldnames = ["Rotulo", "Valor (R$)", "Data", "Lote", "Endereco"] + sorted(all_keys - {"Rotulo", "Valor (R$)", "Data", "Lote", "Endereco"})

            with open("mega.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for d in dados:
                    d.pop("link", None)
                    writer.writerow(d)

            print("\nDados exportados para 'mega.csv'\n")
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
