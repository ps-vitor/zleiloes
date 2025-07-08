# ./backend/scrapers/main.py

import	time
from    portalzuk.scraper import  PortalzukScraper 
from    sodresantoro.scraper    import  SodreSantoroScraper
from    mega.scraper    import  MegaScraper

if  __name__   ==   "__main__":
    start=time.time()
    
    print("\nScrap do Portalzuk:\n")
    start0=time.time()
    portalzukscraper =  PortalzukScraper()
    portalzukscraper.run()
    end0=time.time()
    print(f"Tempo total de execução: {end0-start0:.2f} segundos")
    print("\nFim.\n")
    """
    print("\nScrap do Sodre Santoro:\n")
    start1=time.time()
    sodresantoroscraper = SodreSantoroScraper(delay=4.5)
    sodresantoroscraper.run()
    end1=time.time()
    print(f"Tempo total de execução: {end1-start1:.2f} segundos")
    print("\nFim.\n")
    
    print("\nScrap do Mega:\n")
    start2=time.time()
    megascraper=MegaScraper()
    megascraper.run()
    end2=time.time()
    print(f"Tempo total de execução: {end2-start2:.2f} segundos")
    print("\nFim\n")
    """
    end=time.time()
    print(f"Tempo total de execução: {end-start:.2f} segundos")
