import requests, time, traceback, csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

class SodreSantoroScraper:
    def __init__(self, delay=2.0):
        options = Options()
        options.headless = False
        options.add_argument("--disable-gpu")
        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=options)
        self.delay = delay
        self.session = requests.Session()

    def scrapItensPages(self, url):
        extra_data = {}
        try:
            if url is None:
                print("[ERRO] URL está None!")
                return extra_data

            html = self.session.get(url, timeout=20)
            soup = BeautifulSoup(html.text, "html.parser")

            itens_div = soup.find_all("div", class_="ss-main-content-body")
            for itens in itens_div:
                leilao = itens.find("div", id="aditionalInfoLot_auction")
                lote = itens.find("div", id="aditionalInfoLot_lot_number")
                local = itens.find("div", id="aditionalInfoLot_lot_address")
                codigo = itens.find("div", id="aditionalInfoLot_internal_code")

                processo = itens.find("div", id="aditionalInfoLot_tj_number_process")
                processoLink = processo.find("a")["href"] if processo and processo.find("a") else None

                vara = itens.find("div", id="aditionalInfoLot_tj_vara")
                tipoDeAcao = itens.find("div", id="aditionalInfoLot_tj_action")
                exequente = itens.find("div", id="aditionalInfoLot_tj_n_exequente")
                executada = itens.find("div", id="aditionalInfoLot_tj_n_executed")

                matricula = None
                for div in itens.find_all("div", class_="is-flex-widescreen is-flex-tablet is-align-items-center"):
                    for a in div.find_all("a"):
                        if "Matrícula" in a.text:
                            matricula = a["href"]
                            break

                extra_data.update({
                    "Leilao": leilao.get_text(strip=True) if leilao else None,
                    "Lote": lote.get_text(strip=True) if lote else None,
                    "Local": local.get_text(strip=True) if local else None,
                    "Codigo": codigo.get_text(strip=True) if codigo else None,
                    "ProcessoLink": processoLink,
                    "Vara": vara.get_text(strip=True) if vara else None,
                    "TipoDeAcao": tipoDeAcao.get_text(strip=True) if tipoDeAcao else None,
                    "Exequente": exequente.get_text(strip=True) if exequente else None,
                    "Executada": executada.get_text(strip=True) if executada else None,
                    "Matricula": matricula,
                })

            return extra_data

        except Exception as e:
            print(e)
            traceback.print_exc()
            return extra_data

    def scrapMainPage(self, url):
        try:
            self.driver.get(url)
            time.sleep(self.delay)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            cards = soup.find_all('div', class_="relative flex-none h-fit rounded-xl overflow-hidden bg-white border")
            results = []
            links = []
            for card in cards:
                data = card.find("span", class_="text-sm line-clamp-1 font-semibold")
                title = card.find('h2')  # Ajustar para seletor correto
                price = card.find('p', class_="text-2xl text-blue-700 font-medium")
                link_tag = card.find("a", href=True)
                link = link_tag["href"] if link_tag else None
                links.append(link)

                item = {
                    'data': data.get_text(strip=True) if data else None,
                    'preco': price.get_text(strip=True) if price else None,
                    'titulo': title.get_text(strip=True) if title else None,
                    'link': link
                }
                results.append(item)

            return results, links

        except Exception:
            traceback.print_exc()
            return [], []

    def run(self):
        try:
            url = "https://www.sodresantoro.com.br/imoveis/lotes?page=1"
            dados, links = self.scrapMainPage(url)
            self.driver.quit()
            if not dados:
                print("Nenhum imóvel encontrado.")
                return {"error": "Nenhum imóvel encontrado."}

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
                "data", "preco", "titulo", "Lote", "Local", "Codigo", "Leilao", "ProcessoLink",
                "Vara", "TipoDeAcao", "Exequente", "Executada", "Matricula"
            ]

            with open("sodresantoro.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for d in dados:
                    d.pop("link", None)
                writer.writerows(dados)

            return {
                "properties": dados,
                "metadata": {
                    "source": "sodresantoro",
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
            print("\nDados exportados para 'sodresantoro.csv'\n")
