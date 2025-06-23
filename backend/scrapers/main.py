# ./backend/scrapers/main.py

import	time
from    portalzuk.scraper import  PortalzukScraper 
from    sodresantoro.scraper    import  SodreSantoroScraper
from    mega.scraper    import  MegaScraper

if  __name__   ==   "__main__":
    start=time.time()
    print("\nScrap do Portalzuk:\n")
    portalzukscraper =  PortalzukScraper()
    portalzukscraper.run()
    print("\nFim.\n")
    print("\nScrap do Sodre Santoro:\n")
    sodresantoroscraper = SodreSantoroScraper(delay=1.5)
    sodresantoroscraper.run()
    print("\nFim.\n")
    print("\nScrap do Mega:\n")
    megascraper=MegaScraper()
    megascraper.run()
    print("\nFim\n")
    end=time.time()
    print(f"Tempo total de execução: {end-start:.2f} segundos")
