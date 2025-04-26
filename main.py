from    bs4 import  BeautifulSoup   
import  requests,   traceback,  csv
from    concurrent.futures  import  ThreadPoolExecutor,as_completed

def scrapItensPages(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
    }
    extra_data={}
    try:
        if url is None:
            print("[ERRO] URL está None! Não é possível fazer a requisição.")
            return  extra_data

        html = requests.get(url, headers=headers,timeout=20)
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
            situation_tag=c.find("span",class_="property-status-title")
            situation=situation_tag.get_text(strip=True)
            extra_data["Situacao"]=situation

            matricula_tag=c.find("p",{"class":"text_subtitle","id":"itens_matricula"})
            matricula=matricula_tag.get_text(strip=True)
            extra_data["Matricula do imovel"]=matricula

            observacoes_tag=c.find("div",{"class":"property-info-text div-text-observacoes"})
            observacoes = observacoes_tag.get_text(strip=True)
            extra_data["Observacoes"]=observacoes

        
            link_tag = c.find("a", class_="glossary-link")
            link = link_tag["href"]
            extra_data["Link do processo"]=link

            visitacao_tag=c.find("div",class_="property-info-text")
            visitacao=visitacao_tag.get_text(strip=True)
            extra_data["Visitacao"]=visitacao

            f_pagamento_h3_tag=c.find("h3",class_="property-info-title")
            f_pagamento_h3=f_pagamento_h3_tag.get_text(strip=True)
            f_pagamento_ul=c.find_all(class_="property-payments-items")
            for f   in  f_pagamento_ul:
                f_tag=f.find("p",class_="property-payments-item-text")
                f_text=f_tag.get_text(strip=True)
                extra_data[f_pagamento_h3]=f_text

            d_compromissario_c_tag=c.find("p",class_="property-status-text")
            d_compromissario_c=d_compromissario_c_tag.get_text(strip=True)
            extra_data["Direitos do Compromissario Comprador"]=d_compromissario_c

        return  extra_data

    except Exception as e:
        print(e)
        traceback.print_exc()


def scrapMainPage(url):
    headers =   {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        }

    results=[]

    try:
        html  =   requests.get(url,    headers=headers,timeout=20)
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

        with    ThreadPoolExecutor(max_workers=5)as    executor:
            future_to_data={
                executor.submit(scrapItensPages,d["link"]):d   for d   in  dados   if  d["link"]
            }

            for future  in  as_completed(future_to_data):
                data=future_to_data[future]
                try:
                    extra_info=future.result()
                    data.update(extra_info)
                except  Exception   as  e:
                    print(f"Erro no procesamento paralelo: {e}")
        
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

if  __name__   ==   "__main__":
    main()