from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import csv
import unicodedata
import  traceback,re
from cachetools import TTLCache

class PortalBayitScraper:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/50 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:109.0) Gecko/20100101 Firefox/110.0",
        ]

        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless=new')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=self.options)
        self.all_properties_data = [] # Inicialize aqui
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    def get_pages(self, url):
        self.driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        # Scroll to ensure all elements are loaded
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        nav_el = soup.find("nav", class_="dg-paginacao")
        
        if not nav_el:
            return 1
            
        # Find all pagination links
        page_links = nav_el.find_all("a", onclick=lambda x: x and "BuscaPaginacao" in x)
        
        if not page_links:
            return 1
            
        # Extract all page numbers from onclick attributes
        pages = set()
        for link in page_links:
            onclick = link.get("onclick", "")
            try:
                page_num = int(onclick.split("BuscaPaginacao(")[1].split(")")[0])
                pages.add(page_num)
            except (IndexError, ValueError):
                continue
        
        # Also check the current page which might be a span instead of a link
        current_page = nav_el.find("span")
        if current_page:
            try:
                pages.add(int(current_page.get_text(strip=True)))
            except ValueError:
                pass
        
        return max(pages) if pages else 1

    def get_links(self, url):
        self.driver.get(url)
        time.sleep(3)

        # Scroll to load all content
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # Get all property links
        links_propriedades = set()
        div_col_opor = soup.find_all("div", class_="col-xs-12 col-sm-6 col-md-4 col-lg-3 dg-leiloes-item-col")

        for opor in div_col_opor:
            categoria = opor.find("div", class_="dg-leiloes-label-cat")
            if categoria:
                if  categoria.get_text(strip=True) != "Veículos":
                    if  categoria.get_text(strip=True) != "Diversos":
                        row_leiloes = opor.find_all("a", class_="dg-leiloes-img")
                        for item in row_leiloes:
                            href = item.get('href')
                            if href:
                                links_propriedades.add(href)

        return list(links_propriedades)

    def retorna_links(self, max_properties=None):
        base_url = "https://www.portalbayit.com.br/busca/?Engine=Start&Pagina=1&Busca=&Mapa=&Ordem=10&PaginaIndex=1"
        qtd_paginas = self.get_pages(base_url)
        print(f"Total de páginas encontradas: {qtd_paginas}")

        all_links = set()
        for pagina in range(1, qtd_paginas + 1):
            print(f"Processando página {pagina} de {qtd_paginas}")
            url = f"https://www.portalbayit.com.br/busca/?Engine=Start&Pagina={pagina}&Busca=&Mapa=&Ordem=10&PaginaIndex=1"
            try:
                links = self.get_links(url)
                if links:  # Only process if links were found
                    print(f"Links encontrados na página {pagina}: {len(links)}")
                    for link in links:
                        full_link = f"https://www.portalbayit.com.br{link}" if not link.startswith('http') else link
                        all_links.add(full_link)
                        print(full_link)
                    print(f"Total acumulado: {len(all_links)}")
                else:
                    print(f"Nenhum link encontrado na página {pagina}")
            except Exception as e:
                print(f"Erro ao processar página {pagina}: {str(e)}")
                traceback.print_exc()
                continue

        print(f"\nTotal de links únicos encontrados: {len(all_links)}")
        return list(all_links)

    def is_valid_url(self, url):
        return url and (url.startswith("http://") or url.startswith("https://"))

    def extract_portalbayit_image_urls(self, html_content):
        """Extrai URLs de imagens do conteúdo HTML específico do PortalBayit com cache."""
        cache_key = f"portalbayit_image_urls_{hash(html_content)}"

        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            soup = BeautifulSoup(html_content, "html.parser")
            image_urls = []

            # Find all image containers (using the specific class from PortalBayit)
            image_containers = soup.select('div.slick-track a.dg-lote-img-item')

            for container in image_containers:
                # Get the high-resolution image URL (1300x1300)
                hi_res_url = container.get('href')
                if hi_res_url and hi_res_url not in image_urls:
                    image_urls.append(hi_res_url)

                # Get the full-size image URL (770x620)
                full_size_url = container.get('imgfull')
                if full_size_url and full_size_url not in image_urls:
                    image_urls.append(full_size_url)

                # Get the thumbnail URL (605x487)
                img_tag = container.find('img')
                if img_tag:
                    thumb_url = img_tag.get('src')
                    if thumb_url and thumb_url not in image_urls:
                        image_urls.append(thumb_url)

            self.cache[cache_key] = image_urls
            return image_urls

        except Exception as e:
            print(f"Erro ao extrair URLs de imagem do PortalBayit: {e}")
            traceback.print_exc()
            return []

    def get_property_info(self, url):
        data = {
            "url": url,
            "avaliacao": None,
            "lance_minimo": None,
            "endereco": None,
            "metragem_util": None,
            "metragem_total": None,
            "descricao": None,          # Novo campo
            "matricula": None,    # Novo campo
            "observacoes": None,  # Novo campo
            "imagens":[]
        }

        if not self.is_valid_url(url):
            print("url invalida")
            return data

        try:
            self.driver.get(url)
            time.sleep(3)

            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            html_content = self.driver.page_source
            image_urls = self.extract_portalbayit_image_urls(html_content)
            #define o maximo de imagens
            MAX_IMAGES=3
            limited_image_urls=image_urls[:MAX_IMAGES]
            data["imagens"]=limited_image_urls

            for idx,img_url in  enumerate(limited_image_urls,start=1):
                data[f"imagem_{idx}"]=img_url
            data["total_imagens"]=len(limited_image_urls)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Dados básicos
            if (avaliacao := soup.find("strong", class_="ValorAvaliacao")):
                data["avaliacao"] = avaliacao.get_text(strip=True).replace("R$", "").replace(".", "").replace(",", ".").strip()

            if (lance_min := soup.find("strong", class_="BoxLanceValor")):
                data["lance_minimo"] = lance_min.get_text(strip=True).replace("R$", "").replace(".", "").replace(",", ".").strip(),

            if (endereco := soup.find("div", class_="dg-lote-local-endereco")):
                data["endereco"] = endereco.get_text(strip=True)

            # Metragens
            metragens_container = soup.find("div", class_="dg-lote-cfgs-box")
            if metragens_container:
                for item in metragens_container.find_all("div", class_="dg-lote-cfgs-item"):
                    title = item.get("title", "")
                    value = item.find("span", class_="dg-lote-cfgs-txt")

                    if value:
                        value_text = value.get_text(strip=True)
                        if "ÁREA DO ÚTIL" in title:
                            data["metragem_util"] = value_text
                        elif "ÁREA TOTAL" in title:
                            data["metragem_total"] = value_text

            # Extração de BEM, MATRÍCULA e OBSERVAÇÕES
            descricao_div = soup.find("div", class_="dg-lote-descricao-txt")
            if descricao_div:
                descricao_text = descricao_div.get_text(separator="\n", strip=True)
                data["descricao_imovel"] = descricao_text

                # Extração inteligente dos campos variáveis
                lines = [line.strip() for line in descricao_text.split('\n') if line.strip()]

                # Padrões flexíveis para identificar os campos
                patterns = {
                    'descricao': [
                        r'BEM:\s*(.*)', 
                        r'IMOVEL:\s*(.*)', 
                        r'LOTE \d+\)\s*(.*)',
                        r'DESCRIÇÃO DO IMÓVEL:\s*(.*)'
                    ],
                    'matricula': [
                        r'MATRÍCULA\s*(n?[°ºo]?\s*[\d\.\-]+\s*.*?(?:\n|$))',  # Modificado
                        r'Matrícula\s*(n?[°ºo]?\s*[\d\.\-]+\s*.*?(?:\n|$))',  # Modificado
                        r'Registro\s*(n?[°ºo]?\s*[\d\.\-]+\s*.*?(?:\n|$))'    # Modificado
                    ],
                    'observacoes': [
                        r'OBS\s*\d+:\s*(.*)', 
                        r'Obs\s*\d+:\s*(.*)',
                        r'OBSERVAÇÃO:\s*(.*)'
                    ],
                    'onus': [
                        r'Ônus:\s*(.*)', 
                        r'ÔNUS:\s*(.*)',
                        r'OCUPAÇÃO:\s*(.*)'
                    ]
                }

                for field, regex_list in patterns.items():
                    for line in lines:
                        for regex in regex_list:
                            match = re.search(regex, line, re.IGNORECASE)
                            if match:
                                if field == 'observacoes':
                                    if data[field] is None:
                                        data[field] = []
                                    data[field].append(match.group(1).strip())
                                else:
                                    data[field] = match.group(1).strip()
                                break
                        else:
                            continue
                        break
                    
                # Formata observações como texto separado por quebras de linha
                if isinstance(data.get('observacoes'), list):
                    data['observacoes'] = '\n'.join(data['observacoes'])


            # Processo link (mantido da versão anterior)
            info_div = soup.find_all("div", class_="dg-lote-descricao-info")
            for info in info_div:
                processo_link = info.find("a", href=True)
                if processo_link:
                    data["processo_link"] = processo_link["href"]

            docs_div = soup.find_all("div", class_="dg-lote-documentos-wrapper")
            for div in docs_div:
                items = div.find_all("li", class_="dg-lote-documentos-downloads__item")
                for item in items:
                    item_text = item.get_text(strip=True)
                    # Pegar o link de download (segundo link)
                    download_link = item.find_all("a", href=True)[1]["href"]

                    if "Dívida Ativa" in item_text:
                        data["divida_ativa"] = download_link
                    if "Edital do Leilão" in item_text:
                        data["edital_do_leilao"] = download_link

        except Exception as e:
            print("Erro: {str(e)}")
            traceback.print_exc()

        return data

    def save_to_csv(self, filename="portalbayit.csv"):
        """Versão corrigida que identifica todos os campos dinâmicos antes de exportar"""
        if not hasattr(self, 'all_properties_data') or not self.all_properties_data:
            print("Nenhum dado disponível para exportar!")
            return False
    
        try:
            # Primeiro identificamos todos os campos possíveis
            all_fieldnames = set()
            
            # Campos fixos que sabemos que existem
            base_fields = [
                'url', 'avaliacao', 'lance_minimo', 'endereco', 
                'metragem_util', 'metragem_total', 'descricao',
                'matricula', 'observacoes', 'total_imagens',
                'processo_link', 'imagens', 'imagens_full'
            ]
            
            # Adiciona os campos base
            all_fieldnames.update(base_fields)
            
            # Adiciona todos os campos dinâmicos de imagem encontrados
            for prop in self.all_properties_data:
                all_fieldnames.update(prop.keys())
            
            # Ordena os campos para melhor organização no CSV
            # Primeiro os campos fixos, depois os dinâmicos de imagem ordenados numericamente
            fixed_fields = [f for f in base_fields if f in all_fieldnames]
            
            # Extrai e ordena os campos de imagem (imagem_1, imagem_2, etc.)
            image_fields = sorted(
                [f for f in all_fieldnames if re.match(r'imagem_\d+', f)],
                key=lambda x: int(x.split('_')[1])
            )
            
            # Campos adicionais que não são de imagem
            other_fields = sorted(
                [f for f in all_fieldnames if f not in fixed_fields and f not in image_fields]
            )
            
            # Junta todos os campos na ordem desejada
            ordered_fieldnames = fixed_fields + other_fields + image_fields
            
            with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=ordered_fieldnames)
                writer.writeheader()
                
                for prop in self.all_properties_data:
                    # Se existir a lista 'imagens', podemos removê-la pois já temos os campos individuais
                    prop.pop('imagens', None)
                    prop.pop('imagens_full', None)
                    writer.writerow(prop)
            
            print(f"Dados exportados com sucesso para {filename}")
            return True
    
        except Exception as e:
            print(f"Erro ao exportar para CSV: {str(e)}")
            traceback.print_exc()
            return False

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
    
    def run(self):
        links = self.retorna_links(max_properties=12) 

        # Coletar os dados de cada propriedade
        for i, link in enumerate(links):
            print(f"Processando link {i+1}/{len(links)}: {link}")
            property_data = self.get_property_info(link)
            self.all_properties_data.append(property_data)

        # Salvar os dados no CSV
        self.save_to_csv()

if __name__ == "__main__":
    scraper = PortalBayitScraper()
    links = scraper.retorna_links(max_properties=12) 

    # Coletar os dados de cada propriedade
    for i, link in enumerate(links):
        print(f"Processando link {i+1}/{len(links)}: {link}")
        property_data = scraper.get_property_info(link)
        scraper.all_properties_data.append(property_data)
    
    # Salvar os dados no CSV
    scraper.save_to_csv()