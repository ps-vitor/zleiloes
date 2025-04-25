from    bs4 import  BeautifulSoup   
import  requests

def scrap(url):
    headers =   {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        }
    html  =   requests.get(url,    headers=headers)
    soup    =   BeautifulSoup(html.text, "html.parser")

    try:
        descricao=soup.find("div",class_="card-property-content").get_text().strip()
        
    except  Exception   as  e:
        print(e)


scrap("https://www.portalzuk.com.br/leilao-de-imoveis/v/extrajudicial/1750")    