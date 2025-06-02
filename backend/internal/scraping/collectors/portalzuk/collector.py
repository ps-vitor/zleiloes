from    bs4 import  BeautifulSoup   
import  requests,traceback,csv
# from multiprocessing import Pool,cpu_count
from    concurrent.futures  import  ThreadPoolExecutor,as_completed

session=requests.Session()
session.headers.update({
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'  
})

def scrapItensPages(url):
    extra_data={}
    try:
        if url is None:
            print("[ERRO] URL está None! Não é possível fazer a requisição.")
            return  extra_data

        html = session.get(url,timeout=20)
        soup = BeautifulSoup(html.text, "html.parser")

        itens_div = soup.find_all("div",class_="property-featured-items")
        for itens in itens_div:
            all_itens=itens.find_all("div",class_="property-featured-item")
            for iten    in  all_itens:
                iten_label_tag = iten.find("span", class_="property-featured-item-label")
                iten_value_tag = iten.find("span", class_="property-featured-item-value")
                if  iten_label_tag  and iten_value_tag:
                    try:
                        iten_label=iten_label_tag.get_text(strip=True)
                        iten_value=iten_value_tag.get_text(strip=True)
                        extra_data[iten_label]=iten_value
                    except  Exception   as  e:
                        print(e)
                        traceback.print_exc()

        content=soup.find_all("div",class_="content")
        for c   in  content:
            extra_data["Situacao"]=c.find("span",class_="property-status-title").get_text(strip=True)
            extra_data["Matricula do imovel"]=c.find("p",{"class":"text_subtitle","id":"itens_matricula"}).get_text(strip=True)
            extra_data["Observacoes"]=c.find("div",{"class":"property-info-text div-text-observacoes"}).get_text(strip=True)
            extra_data["Link do processo"]= c.find("a", class_="glossary-link")["href"]
            extra_data["Visitacao"]=c.find("div",class_="property-info-text").get_text(strip=True)

            f_pagamento_h3_tag=c.find("h3",class_="property-info-title")
            f_pagamento_h3=f_pagamento_h3_tag.get_text(strip=True)
            f_pagamento_ul=c.find_all(class_="property-payments-items")
            for f   in  f_pagamento_ul:
                extra_data[f_pagamento_h3]=f.find("p",class_="property-payments-item-text").get_text(strip=True)

            extra_data["Direitos do Compromissario Comprador"]=c.find("p",class_="property-status-text").get_text(strip=True)

            extra_data["Direito de Preferencia"]=c.find("p",class_="text_subtitle").get_text(strip=True)

        return  extra_data

    except Exception as e:
        print(e)
        traceback.print_exc()


def scrapMainPage(url):
    results=[]

    try:
        html  =   session.get(url,timeout=20)
        soup    =   BeautifulSoup(html.text, "html.parser")
        
        section=soup.find(class_="s-list-properties")
        cards=section.find_all(class_="card-property")
        for card in cards:
            card_content = card.find(class_="card-property-content") 
            prices_uls = card.find_all("ul", class_="card-property-prices")
            valor_label = valor_value = valor_data = None 
            lote_label = card.find(class_="card-property-price-lote")
            address_tag = card.find(class_="card-property-address")
            
            link_tag = card.find("div", class_="card-property-image-wrapper")
            link = link_tag.find("a")["href"] if link_tag and link_tag.find("a") else None
            if link and not link.startswith("http"):
                link = "https://www.portalzuk.com.br" + link

            for prices_ul in prices_uls:
                prices_li=prices_ul.find_all(class_="card-property-price")
                for prices   in  prices_li:
                    valor_label=prices.find(class_="card-property-price-label")
                    valor_value=prices.find(class_="card-property-price-value")
                    valor_data=prices.find(class_="card-property-price-data")

                    if not (valor_label and valor_value and valor_data):
                        continue
                    
                    data = {
                        "Rotulo":valor_label.get_text(strip=True),
                        "Valor (R$)":valor_value.get_text(strip=True).replace("R$", "").replace(".","").replace(",",".").strip(),
                        "Data":valor_data.get_text(strip=True),
                        "Lote":lote_label.get_text(strip=True) if lote_label else None,
                        "Endereco":address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                        "link":link,
                    }
                    results.append(data)

    except  Exception   as  e:
        print(e)
        traceback.print_exc()

    return  results


def main():
    try:
        dados=scrapMainPage("https://www.portalzuk.com.br/leilao-de-imoveis/") 
                
        # for data    in  dados:
            # if  data["link"]:
                # extra_info=scrapItensPages(data["link"])
                # data.update(extra_info)

        with    ThreadPoolExecutor(max_workers=min(len(links),cpu_count()*2))as executor:
            futures={executor.submit(scrapItensPages,link):link for link    in  links}
            extra_infos=[]
            for future  in  as_completed(futures):
                try:
                    extra_info=future.result()
                    extra_infos.append(extra_info)
                except  Exception   as  e:
                    print(f"Erro ao obter dados de {futures[future]}:   {e}")
                    extra_infos.append({})

        links = [d["link"] for d in dados if d["link"]]
        
        for data, extra_info in zip(dados, extra_infos):
            if extra_info:
                data.update(extra_info)
        
        all_keys=set()
        for d   in  dados:
            all_keys.update(d.keys())
        all_keys.discard("link")

        fieldnames=[
                "Rotulo","Valor (R$)","Data","Lote","Endereco",
            ]+sorted(k  for k   in  all_keys    if  k   not in[
                "Rotulo","Valor (R$)","Data","Lote","Endereco",
        ])

        with    open("portalzuk.csv","w",newline="",encoding="utf-8")   as  csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=fieldnames)
            writer.writeheader()
            for d in dados:
                d.pop("link")
                
            writer.writerows(dados)

    except  Exception   as  e:
        print(e)
    finally:
        print("\nDados exportados para 'portalzuk.csv'\n")
