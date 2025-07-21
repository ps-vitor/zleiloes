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

    """
    Requeridos:
    Rotulo,Valor (R$),Data,Lote,Endereco,Andar,
    Descrição do imóvel,Direito de Preferencia,
    Direitos do Compromissario Comprador,Link do processo,
    Matricula do imovel,Metragem construída,Metragem terreno,
    Metragem total,Metragem útil,Observacoes,Quartos,Situacao,
    Vagas,Visitacao

    Lidos (até então):
    data,preco,titulo,Lote,Local,Codigo,Leilao,ProcessoLink,
    Vara,TipoDeAcao,Exequente,Executada,Matricula
    """

    def scrapItensPages(self, url):
        extra_data = {}
        try:
            if url is None:
                print("[ERRO] URL está None!")
                return extra_data

            html = self.session.get(url, timeout=20)
            soup = BeautifulSoup(html.text, "html.parser")

            descricao=soup.find("div",id="detail_info_lot_description")
            pagamento=soup.find("div",id="payments_options")

            extra_data.update({
                "descricao":descricao.get_text(strip=True)  if  descricao   else    "n/a",
                "forma de pagamento":pagamento.get_text(strip=True) if  pagamento   else    "n/a"
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
                    'preco': price.get_text(strip=True) if price else None,
                    'data': data.get_text(strip=True) if data else None,
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
            urls = ["https://www.sodresantoro.com.br/imoveis/lotes?page=1","https://www.sodresantoro.com.br/imoveis/lotes?page=2"]
            for url in  urls:
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
                "data", "preco", "titulo", "Lote"
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
