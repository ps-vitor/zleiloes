from    bs4 import  BeautifulSoup   
import  requests,   traceback,  csv


def scrapItensPages(url, headers):
    try:
        if url is None:
            print("[ERRO] URL está None! Não é possível fazer a requisição.")
            return
        html = requests.get(url, headers=headers)
        soup = BeautifulSoup(html.text, "html.parser")

        itens_div = soup.find_all(class_="property-featured-items")
        for iten in itens_div:
            iten_label_tag = iten.find("span", class_="property-featured-item-label")
            iten_value_tag = iten.find("span", class_="property-featured-item-value")
            if  iten_label_tag  and iten_value_tag:
                try:
                    iten_label=iten_label_tag.get_text(strip=True)
                    iten_value=iten_value_tag.get_text(strip=True)
                except  Exception   as  e:
                    print(e)
                    traceback.print_exc()


    except Exception as e:
        print(e)
        traceback.print_exc()


def scrapMainPage(url):
    headers =   {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        }

    results=[]

    try:
        html  =   requests.get(url,    headers=headers)
        soup    =   BeautifulSoup(html.text, "html.parser")
        
        section=soup.find(class_="s-list-properties")
        cards=section.find_all(class_="card-property")
        # print(section)
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

            print(link)

            for prices_ul in prices_uls:
                prices_li=prices_ul.find_all(class_="card-property-price")
                for prices   in  prices_li:
                    valor_label=prices.find(class_="card-property-price-label")
                    valor_value=prices.find(class_="card-property-price-value")
                    valor_data=prices.find(class_="card-property-price-data")

                    if not (valor_label and valor_value and valor_data):
                        continue
                    
                    scrapItensPages(link,headers)

                    data = {
                        "rotulo":valor_label.get_text(strip=True),
                        "valor (R$)":valor_value.get_text(strip=True).replace("R$", ""),
                        "data":valor_data.get_text(strip=True),
                        "lote":lote_label.get_text(strip=True) if lote_label else None,
                        "endereco":address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                    }
                    results.append(data)

    except  Exception   as  e:
        print(e)
        traceback.print_exc()

    return  results



dados=scrapMainPage("https://www.portalzuk.com.br/leilao-de-imoveis/")    
# for d in dados:
    # print(f"--> {d}")
    # print("-" * 40)

with    open("portalzuk.csv","w",newline="",encoding="utf-8")   as  csvfile:
    fieldnames=["rotulo","valor (R$)","data","lote","endereco"]
    writer=csv.DictWriter(csvfile,fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(dados)
    print("\nDados exportados para 'portalzuk.csv'\n")