# ./backend/scrapers/mega/scraper.py

from    bs4 import  BeautifulSoup   
import  requests,traceback,csv,time
from multiprocessing import Pool,cpu_count
from    concurrent.futures  import  ThreadPoolExecutor,as_completed
from    playwright.sync_api import  sync_playwright
from    datetime    import  datetime

class   MegaScraper:
    def __init__(self):
        self.session=requests.Session()
        self.session.headers.update({
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'  
        })

    def scrapItensPages(self,url):
        extra_data={}
        try:
            if url is None:
                print("[ERRO] URL está None! Não é possível fazer a requisição.")
                return  extra_data

            html = self.session.get(url,timeout=20)
            soup = BeautifulSoup(html.text, "html.parser")

            return  extra_data

        except Exception as e:
            print(e)
            traceback.print_exc()


    def scrapMainPage(self,url):
        results=[]

        try:
            html = self.session.get(url,timeout=20)
            if html.status_code != 200:
                print(f"Failed to fetch page: HTTP {html.status_code}")
                return results

            soup = BeautifulSoup(html.text, "html.parser")
            imoveis = soup.find_all("div", class_="col-sm-6 col-md-4 col-lg-3")

            print(f"{len(imoveis)} imoveis encontrados")

        except Exception as e:
            print(e)
            traceback.print_exc()

        return results


    def run(self):
        try:
            url="https://www.megaleiloes.com.br/imoveis"
            dados = self.scrapMainPage(url) 

            # First get all the links from the dados
            links = [d["link"] for d in dados if d["link"]]

            # Check if there are any valid links
            if not links:
                print("No valid links found")
                return

            # Now use ThreadPoolExecutor with the links
            with    ThreadPoolExecutor(max_workers=min(len(links),cpu_count()*2))as executor:
                futures={executor.submit(self.scrapItensPages,link):link for link    in  links}
                extra_infos=[]
                for future  in  as_completed(futures):
                    try:
                        extra_info=future.result()
                        extra_infos.append(extra_info)
                    except  Exception   as  e:
                        print(f"Erro ao obter dados de {futures[future]}:   {e}")
                        extra_infos.append({})

            # Combine the data
            for data, extra_info in zip(dados, extra_infos):
                if extra_info:
                    data.update(extra_info)

            # Prepare for CSV export
            all_keys = set()
            for d in dados:
                all_keys.update(d.keys())
            all_keys.discard("link")

            fieldnames = [
                "Rotulo", "Valor (R$)", "Data", "Lote", "Endereco",
            ] + sorted(k for k in all_keys if k not in [
                "Rotulo", "Valor (R$)", "Data", "Lote", "Endereco",
            ])

            # with open("portalzuk.csv", "w", newline="", encoding="utf-8") as csvfile:
                # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                # writer.writeheader()
                # for d in dados:
                    # d.pop("link", None)  # Safely remove 'link' if it exists
                # writer.writerows(dados)
            return {
                "properties": dados,
                "metadata": {
                "source": "portalzuk",
                "scraped_at": datetime.now().isoformat(),
                "count": len(dados)
            }
        }
        except Exception as e:
            # print(e)
            # traceback.print_exc()
            return  {
                "error":str(e),
                "traceback":traceback.format_exc()
            }
        # finally:
            # print("\nDados exportados para 'portalzuk.csv'\n")
            # print(json.dumps(dados))
