from    bs4 import  BeautifulSoup   
import  requests,   traceback

def scrap(url):
    headers =   {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        }

    try:
        html  =   requests.get(url,    headers=headers)
        soup    =   BeautifulSoup(html.text, "html.parser")
        
        section=soup.find("section",class_="s-list-properties")
        cards=section.find_all("div",class_="card-property")
        # print(section)
        for card   in  cards:
            card_content=soup.find("div",class_="card-property-content") 

            valor_label=soup.find("span",class_="card-property-price-label")
            valor_value=soup.find("span",class_="card-property-price-value")
            
            lote_label=soup.find("span",class_="card-property-price-lote")

            address_tag=soup.find("address",class_="card-property-address")

            data=[
                valor_label.get_text(strip=True)if  valor_label else    "Sem label",
                valor_value.get_text(strip=True)if  valor_value else    "Sem valor",
                lote_label.get_text(strip=True)if  lote_label else    "Sem lote",
                address_tag.get_text(separator=" ",strip=True)if address_tag else    "Sem endereço",
            ]

            for d   in  data:
                print(f"--> {d}")
            print("-"*40)

    except  Exception   as  e:
        print(e)
        traceback.print_exc()


scrap("https://www.portalzuk.com.br/leilao-de-imoveis/")    
