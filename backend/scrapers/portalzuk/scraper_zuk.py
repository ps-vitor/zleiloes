from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from bs4 import BeautifulSoup
import csv
import os
import time
import random
import traceback
from datetime import datetime
from urllib.parse import urljoin

# --- Configuração do WebDriver ---
# DEFINA ISTO COMO FALSE PARA USAR O CHROMEDRIVER MANUALMENTE BAIXADO.
# Se você quiser tentar o webdriver-manager novamente, defina como True.
USE_WEBDRIVER_MANAGER = False 

# Se USE_WEBDRIVER_MANAGER for True, tente importar (requer 'pip install webdriver-manager')
if USE_WEBDRIVER_MANAGER:
    try:
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("WebDriver-manager não encontrado. Por favor, instale-o ou defina USE_WEBDRIVER_MANAGER = False.")
        USE_WEBDRIVER_MANAGER = False # Força para False se a importação falhar

class PortalzukScraperSelenium:
    def __init__(self):
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis/"
        self.driver = self._initialize_driver()
        if self.driver:
            self.wait = WebDriverWait(self.driver, 30) # Aumentei o tempo de espera para elementos
        else:
            self.wait = None # Para evitar erros se o driver não inicializar

    def _initialize_driver(self):
        """Inicializa o WebDriver do Chrome."""
        try:
            options = webdriver.ChromeOptions()
            
            # --- Opções Headless (Descomente para rodar sem interface gráfica) ---
            # options.add_argument('--headless')
            # options.add_argument('--disable-gpu')
            # options.add_argument('--no-sandbox') # Necessário em alguns ambientes Linux
            # options.add_argument('--window-size=1920x1080') # Garante uma boa resolução em headless
            # options.add_argument('--disable-dev-shm-usage') # Para sistemas Linux
            
            # User-Agent para simular um navegador comum (versão atualizada para Chrome 126)
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')
            
            print("Inicializando WebDriver...")
            if USE_WEBDRIVER_MANAGER:
                # Usa webdriver-manager para baixar e gerenciar o ChromeDriver
                print("Usando webdriver-manager para instalar o ChromeDriver.")
                service = Service(ChromeDriverManager().install())
            else:
                # Caminho manual para o chromedriver se webdriver-manager não for usado
                print("Usando ChromeDriver baixado manualmente.")
                # Certifique-se de que o 'chromedriver' está no mesmo diretório do script
                # ou especifique o caminho completo, ex: '/usr/local/bin/chromedriver'
                chrome_driver_path = './chromedriver' 
                if not os.path.exists(chrome_driver_path):
                    print(f"❌ Erro: chromedriver não encontrado em '{chrome_driver_path}'.")
                    print("Por favor, baixe o chromedriver compatível com seu Chrome/Chromium (v137) e coloque-o neste diretório.")
                    return None
                service = Service(executable_path=chrome_driver_path)
            
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(60) # Tempo limite para carregamento da página completa
            return driver
        except WebDriverException as e:
            print(f"❌ Erro ao inicializar o WebDriver: {e}")
            print("Verifique se o Chrome/Chromium está instalado e se o chromedriver é compatível com sua versão do navegador.")
            return None
        except Exception as e:
            print(f"❌ Erro inesperado na inicialização do WebDriver: {e}")
            return None

    def close(self):
        """Fecha o navegador."""
        if self.driver:
            print("Fechando navegador...")
            self.driver.quit()

    def fechar_modal_virada(self):
        """Tenta fechar o modal de virada de página se ele aparecer."""
        try:
            print("Tentando fechar modal de virada (se presente)...")
            # Espera até que o modal esteja visível (ou que o tempo limite expire)
            modal = self.wait.until(EC.visibility_of_element_located((By.ID, 'modalVirada')))
            # Encontra o botão de fechar dentro do modal
            close_button = modal.find_element(By.CSS_SELECTOR, '.modal-footer button.btn-close-modal')
            
            if close_button and close_button.is_displayed():
                close_button.click()
                print("Modal de virada fechado com sucesso.")
                # Espera o modal desaparecer
                self.wait.until(EC.invisibility_of_element_located((By.ID, 'modalVirada')))
                time.sleep(1) # Pequeno atraso adicional para garantir
                return True
            return False
        except TimeoutException:
            print("Modal de virada não apareceu ou não foi encontrado dentro do tempo limite.")
            return False
        except NoSuchElementException:
            print("Botão de fechar modal não encontrado no elemento modal detectado.")
            return False
        except Exception as e:
            print(f"Erro ao tentar fechar modal: {str(e)}")
            traceback.print_exc()
            return False

    def scrap_main_page(self):
        """Coleta os dados principais de todas as páginas usando Selenium para rolar e clicar."""
        properties = {}
        processed_links = set() # Para evitar duplicatas e saber quando parar

        if not self.driver or not self.wait:
            print("WebDriver não inicializado. Não é possível raspar a página principal.")
            return properties

        print(f"Navegando para a URL base: {self.base_url}")
        self.driver.get(self.base_url)

        # Tenta fechar o modal inicial, se houver
        self.fechar_modal_virada()

        # Rola a página para garantir que o conteúdo inicial seja carregado e o botão apareça
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 4)) # Espera os primeiros itens carregarem

        click_count = 0
        # O total de imóveis é cerca de 989. Cada clique carrega 30.
        # 989 / 30 = ~33 cliques. Definimos um limite um pouco maior para segurança.
        max_clicks = 40 

        while click_count < max_clicks:
            load_more_button = None
            try:
                # Espera o botão "Carregar mais 30 imóveis" ser clicável
                load_more_button = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Carregar mais 30 imóveis')]"))
                )
                
            except TimeoutException:
                print("Botão 'Carregar mais 30 imóveis' não encontrado ou não clicável após esperar. Assumindo que todos os imóveis foram carregados.")
                break # Sai do loop se o botão não estiver mais visível/clicável
            except NoSuchElementException:
                print("Botão 'Carregar mais 30 imóveis' não encontrado. Assumindo que todos os imóveis foram carregados.")
                break # Sai do loop se o botão não for encontrado
            
            # Scroll até o botão para garantir que ele esteja visível na viewport e clicável
            self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
            time.sleep(random.uniform(0.5, 1.5)) # Pequeno atraso após rolar

            try:
                # Clica no botão
                print(f"Clicando no botão 'Carregar mais 30 imóveis' (clique {click_count + 1})...")
                load_more_button.click()
                click_count += 1
                
                # Espera que novos elementos apareçam.
                # Aumentei o tempo de espera após o clique para dar mais chance ao site carregar.
                time.sleep(random.uniform(4, 8)) 
                # Rola para garantir que novos botões/conteúdo apareçam e sejam visíveis
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
                time.sleep(random.uniform(2, 4)) # Espera a rolagem e o carregamento final

            except Exception as e:
                print(f"Erro ao clicar no botão 'Carregar mais': {str(e)}")
                traceback.print_exc()
                break # Se não conseguir clicar, algo deu errado, sai do loop
            
            # Raspar os cards da página atual após cada clique
            current_page_source = self.driver.page_source
            soup = BeautifulSoup(current_page_source, 'html.parser')
            section = soup.find(class_="s-list-properties")
            cards = section.find_all(class_="card-property") if section else []
            
            new_cards_found_in_iteration = 0
            for card in cards:
                try:
                    image_wrapper = card.find("div", class_="card-property-image-wrapper")
                    link_tag = image_wrapper.find("a") if image_wrapper else None
                    link = link_tag["href"] if link_tag else None
                    
                    if link and not link.startswith("http"):
                        link = urljoin("https://www.portalzuk.com.br", link)
                    
                    if not link or link in processed_links:
                        continue # Pula links sem URL ou já processados
                    
                    processed_links.add(link)
                    new_cards_found_in_iteration += 1

                    address_tag = card.find(class_="card-property-address")
                    lote_tag = card.find(class_="card-property-price-lote")
                    
                    common_data = {
                        "Lote": lote_tag.get_text(strip=True) if lote_tag else None,
                        "Endereco": address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                        "link": link,
                        "Precos": [],
                    }
                    
                    prices_uls = card.find_all("ul", class_="card-property-prices")
                    for ul in prices_uls:
                        for li in ul.find_all("li", class_="card-property-price"):
                            label = li.find(class_="card-property-price-label")
                            value = li.find(class_="card-property-price-value")
                            date = li.find(class_="card-property-price-data")
                            
                            if label and value and date:
                                preco = {
                                    "Rotulo": label.get_text(strip=True),
                                    "Valor (R$)": value.get_text(strip=True)
                                        .replace("R$", "").replace(".", "").replace(",", ".").strip(),
                                    "Data": date.get_text(strip=True)
                                }
                                common_data["Precos"].append(preco)
                    
                    properties[link] = common_data
                
                except Exception as e:
                    print(f"Erro ao processar card: {link if link else 'N/A'} - {str(e)}")
                    traceback.print_exc()
                    continue
            
            print(f"Imóveis únicos coletados até agora na listagem: {len(properties)}. Novos encontrados nesta iteração: {new_cards_found_in_iteration}")
            
            # Critério de parada: Se não houver novos cards encontrados em uma iteração após um clique, assumimos que chegamos ao fim.
            # Ou se o número de cards não aumentar significativamente.
            # Um limite de cliques também é uma segurança.
            if new_cards_found_in_iteration == 0 and click_count > 0: 
                print("Nenhuma nova propriedade encontrada nesta iteração. Assumindo fim da lista.")
                break

        print(f"Finalizada a coleta da listagem principal. Total de {len(properties)} imóveis únicos.")
        return properties

    def scrap_item_page(self, url):
        """Coleta informações detalhadas de uma página específica de imóvel usando Selenium para navegação."""
        try:
            # Reutiliza o mesmo driver para navegar para as páginas de detalhe
            self.driver.get(url)
            time.sleep(random.uniform(2.5, 5.0)) # Espera a página de detalhes carregar completamente

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            details = {}

            # Informações básicas
            title = soup.find('h1', class_='property-title')
            if title:
                details['Titulo'] = title.get_text(strip=True)

            # Características principais
            features = {}
            features_container = soup.find('div', class_='property-features-container')
            if features_container:
                for item in features_container.find_all('div', class_='property-feature-item'):
                    icon = item.find('i')
                    text = item.get_text(strip=True)
                    if icon:
                        text = text.replace(icon.get_text(strip=True), '', 1).strip()
                    if ':' in text:
                        key, val = text.split(':', 1)
                        features[key.strip()] = val.strip()
                    else:
                        features[text] = True
            details['Caracteristicas'] = features

            # Descrição do imóvel
            description = soup.find('div', class_='property-description')
            if description:
                details['Descricao'] = description.get_text(separator='\n', strip=True)

            # Informações de localização
            location = {}
            address = soup.find('div', class_='property-address')
            if address:
                location['Endereco_Completo'] = address.get_text(separator=' ', strip=True)
                
            neighborhood = soup.find('span', class_='property-neighborhood')
            if neighborhood:
                location['Bairro'] = neighborhood.get_text(strip=True)
            details['Localizacao'] = location

            # Informações do leilão
            auction = {}
            auction_info = soup.find('div', class_='property-auction-info')
            if auction_info:
                for row in auction_info.find_all('div', class_='info-row'):
                    label = row.find('div', class_='info-label')
                    value = row.find('div', class_='info-value')
                    if label and value:
                        auction[label.get_text(strip=True)] = value.get_text(strip=True)
            details['Leilao'] = auction

            # Fotos do imóvel (apenas contagem)
            gallery = soup.find('div', class_='property-gallery')
            if gallery:
                photos = gallery.find_all('img')
                details['Total_Fotos'] = len(photos)

            return details

        except Exception as e:
            print(f"[ERRO detalhes] {url} - {str(e)}")
            traceback.print_exc()
            return None
    
    def export_to_csv(self, properties, filename="portalzuk_imoveis_selenium.csv"):
        """Exporta os dados coletados para um arquivo CSV"""
        try:
            rows = []
            fieldnames = set()

            for link, data in properties.items():
                precos = data.pop('Precos', [])
                
                caracteristicas = data.pop('Caracteristicas', {})
                leilao = data.pop('Leilao', {})
                localizacao = data.pop('Localizacao', {})

                base_row_data = {
                    'Lote': data.get('Lote'),
                    'Endereco': data.get('Endereco'),
                    'link': link,
                    'Titulo': data.get('Titulo'),
                    'Descricao': data.get('Descricao'),
                    'Bairro': localizacao.get('Bairro'),
                    'Endereco_Completo': localizacao.get('Endereco_Completo'),
                    'Total_Fotos': data.get('Total_Fotos'),
                    **caracteristicas,
                    **leilao,
                }

                if not precos:
                    row = {**base_row_data, 'Rotulo': '', 'Valor (R$)': '', 'Data': ''}
                    rows.append(row)
                    fieldnames.update(row.keys())
                else:
                    for preco in precos:
                        row = {
                            **base_row_data,
                            'Rotulo': preco.get('Rotulo'),
                            'Valor (R$)': preco.get('Valor (R$)'),
                            'Data': preco.get('Data')
                        }
                        rows.append(row)
                        fieldnames.update(row.keys())

            preferred_order = [
                'Lote', 'Titulo', 'Endereco', 'Bairro', 'Endereco_Completo', 'link',
                'Descricao', 'Rotulo', 'Valor (R$)', 'Data',
                'Total_Fotos'
            ]
            
            final_fieldnames = []
            for field in preferred_order:
                if field in fieldnames:
                    final_fieldnames.append(field)
            
            for field in sorted(list(fieldnames)):
                if field not in final_fieldnames:
                    final_fieldnames.append(field)

            dirname = os.path.dirname(filename)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            print(f"\n✅ Dados exportados com sucesso para {filename}")
            print(f"Total de registros: {len(rows)}")
            return True

        except Exception as e:
            print(f"\n❌ Erro ao exportar dados: {str(e)}")
            traceback.print_exc()
            return False

    def run(self, output_file="portalzuk_imoveis_selenium.csv"):
        """Método principal para executar todo o processo de scraping com Selenium."""
        start_time = datetime.now()
        print("\n=== INICIANDO SCRAPING PORTALZUK COM SELENIUM ===\n")
        
        if not self.driver:
            print("Não foi possível iniciar o navegador. Abortando.")
            return {
                "status": "error",
                "message": "Falha na inicialização do WebDriver",
                "duration": str(datetime.now() - start_time)
            }

        # 1. Coleta os dados principais (todas as páginas clicando no botão)
        print("\n⏳ Coletando lista de imóveis através do navegador...")
        properties = self.scrap_main_page()
        
        if not properties:
            print("\n❌ Nenhum imóvel encontrado na listagem principal. Verifique o processo de clique no botão.")
            self.close()
            return {
                "status": "error",
                "message": "Nenhum imóvel encontrado",
                "duration": str(datetime.now() - start_time)
            }

        print(f"\n✅ {len(properties)} imóveis encontrados na listagem. Coletando detalhes...")

        # 2. Coleta detalhes sequencialmente (usando a mesma instância do driver)
        # Optamos por coletar os detalhes sequencialmente para evitar a complexidade e o alto consumo de recursos de múltiplos WebDrivers.
        total_properties = len(properties)
        for i, link in enumerate(list(properties.keys())): # Converte para lista para iterar com segurança
            # Opcional: Se já houver detalhes pré-existentes, podemos pular para economizar tempo em re-runs parciais.
            # if 'Titulo' in properties[link] and properties[link]['Titulo']:
            #     print(f"✓ Detalhes para {link} já parecem estar presentes. Pulando.")
            #     continue

            details = self.scrap_item_page(link)
            if details:
                properties[link].update(details)
                print(f"✓ Detalhes coletados para {i+1}/{total_properties} imóveis: {link}")
            else:
                print(f"✗ Falha ao coletar detalhes para {i+1}/{total_properties} imóveis: {link}")
        
        # 3. Exporta os dados para CSV
        print("\n⏳ Exportando dados para CSV...")
        export_result = self.export_to_csv(properties, output_file)

        duration = datetime.now() - start_time
        print(f"\n⏱ Tempo total de execução: {duration}")
        
        self.close() # Garante que o navegador seja fechado no final

        if export_result:
            return {
                "status": "success",
                "total_imoveis": len(properties),
                "arquivo": output_file,
                "duration": str(duration)
            }
        else:
            return {
                "status": "partial_success",
                "total_imoveis": len(properties),
                "message": "Dados coletados mas não foram exportados",
                "duration": str(duration)
            }

if __name__ == "__main__":
    scraper = PortalzukScraperSelenium()
    result = scraper.run()
    print(result)