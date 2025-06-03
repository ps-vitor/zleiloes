# backend/cmd/portalzuk/main.py

import	time
from    portalzuk.scraper import  PortalzukScraper 

if  __name__   ==   "__main__":
    start=time.time()
    portalzukscraper =  PortalzukScraper()
    portalzukscraper.run()
    end=time.time()
    print(f"Tempo total de execução: {end-start:.2f} segundos")