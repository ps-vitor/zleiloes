from    bs4 import  BeautifulSoup   
import  requests,   traceback,  csv

def scrap(url):
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
            valor_label = valor_value = valor_data = None  # Evita erro
            lote_label = card.find(class_="card-property-price-lote")
            address_tag = card.find(class_="card-property-address")
            
            for prices_ul in prices_uls:
                prices_li=prices_ul.find_all(class_="card-property-price")
                for prices   in  prices_li:
                    valor_label=prices.find(class_="card-property-price-label")
                    valor_value=prices.find(class_="card-property-price-value")
                    valor_data=prices.find(class_="card-property-price-data")

                    if not (valor_label and valor_value and valor_data):
                        continue

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

dados=scrap("https://www.portalzuk.com.br/leilao-de-imoveis/")    
for d in dados:
    print(f"--> {d}")
    print("-" * 40)

with    open("portalzuk.csv","w",newline="",encoding="utf-8")   as  csvfile:
    fieldnames=["rotulo","valor (R$)","data","lote","endereco"]
    writer=csv.DictWriter(csvfile,fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(dados)
    print("\nDados exportados para 'portalzuk.csv'\n")