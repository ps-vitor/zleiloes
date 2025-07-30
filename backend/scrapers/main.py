# ./backend/scrapers/main.py

import	time
from    portalzuk.scraper import  PortalzukScraper 
from    sodresantoro.scraper    import  SodreSantoroScraper
from    mega.scraper    import  MegaScraper
from    portalbayit.scraper import  PortalBayitScraper
from    superbid.scraper    import  SuperbidScraper

if  __name__   ==   "__main__":
    start=time.time()
    
    print("\nScrap do Portalzuk:\n")
    portalzukscraper =  PortalzukScraper()
    portalzukscraper.run()
    print("\nFim.\n")
    
    print("\nScrap do Sodre Santoro:\n")
    sodresantoroscraper = SodreSantoroScraper(delay=4.5)
    sodresantoroscraper.run()
    print("\nFim.\n")
    
    print("\nScrap do Mega:\n")
    megascraper=MegaScraper()
    megascraper.get_homelinks()
    print("\nFim\n")

    print("\nScrap do Portalbayit:\n")
    portalbayitscraper=PortalBayitScraper()
    portalbayitscraper.run()
    print("\nFim.\n")

    print("\nScrap do Superbid:\n")
    superbidscraper=SuperbidScraper()
    superbidscraper.run()
    print("\nFim.\n")
    
    end=time.time()
    tempo_total=end-start
    if  tempo_total>60:
        tempo=tempo_total/60
        print(f"Tempo total de execução: {tempo:.2f} minutos")
    else:
        print(f"Tempo total de execução: {tempo_total:.2f} segundos")
