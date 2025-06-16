# ./backend/scrapers/sodresantoro/scraper.py

from    bs4 import  BeautifulSoup   
import  requests,traceback,csv,time
from multiprocessing import Pool,cpu_count
from    concurrent.futures  import  ThreadPoolExecutor,as_completed

class   SodresantoroScraper:
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
            html  =   self.session.get(url,timeout=20)
            soup    =   BeautifulSoup(html.text, "html.parser")

            ul_imoveis = soup.find("ul", class_=lambda x: x and "grid" in x and "gap-4" in x) if soup else None
            
            if not ul_imoveis:
                print("Lista de imóveis não encontrada.")
                return results

            articles = ul_imoveis.find_all("article")

            for article    in  articles:
                try:
                    imovel=article.find("div",class_="relative flex-none h-fit rounded-xl overflow-hidden bg-white border")
                    link_tag = article.find("div", class_="relative rounded-xl overflow-hidden bg-neutral-200")
                    link = link_tag.find("a")["href"]
                except Exception   as  e:
                    print(f"Erro ao processar imoveis: {e}")


                print(imovel)
                        
                # data = {
                    
                #     "Rotulo":valor_label.get_text(strip=True),
                #     "Valor (R$)":valor_value.get_text(strip=True).replace("R$", "").replace(".","").replace(",",".").strip(),
                #     "Data":valor_data.get_text(strip=True),
                #     "Lote":lote_label.get_text(strip=True) if lote_label else None,
                #     "Endereco":address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                #     "link":link,
                # }

            print(html.status_code)
            print(html.url)
            print(soup.prettify()[:1000])

        except  Exception   as  e:
            print(e)
            traceback.print_exc()

        return  results


    def run(self):
        try:
            dados = self.scrapMainPage("https://www.sodresantoro.com.br/imoveis/lotes?page=1") 

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
