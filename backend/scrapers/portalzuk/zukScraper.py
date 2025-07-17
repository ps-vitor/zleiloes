import time
import traceback
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from .browserManager import BrowserManager
from .requestManager import RequestManager
from .dataExtractor import DataExtractor
from .dataProcessor import DataProcessor
from .fileExporter import FileExporter
from .circuitBreaker import CircuitBreaker

class PortalzukScraper:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:109.0) Gecko/20100101 Firefox/110.0",
        ]
        
        self.base_url = "https://www.portalzuk.com.br/leilao-de-imoveis"
        
        self.circuit_breaker = CircuitBreaker(max_failures=3, reset_timeout=120)
        
        # 1. Primeiro crie o request_manager
        self.request_manager = RequestManager(
            user_agents=self.user_agents,
            circuit_breaker=self.circuit_breaker
        )
        
        # 2. Depois crie o data_extractor com as dependências
        self.data_extractor = DataExtractor(
            base_url=self.base_url,
            request_manager=self.request_manager  # Passando o request_manager criado
        )
        
        # 3. Depois inicialize os outros componentes
        self.browser = BrowserManager(self.user_agents)
        self.data_processor = DataProcessor(max_workers=4)
        self.file_exporter = FileExporter()
        self.driver = self.browser.driver

    def run(self, start_url=None):
        """Executa o processo de scraping completo."""
        try:
            print("Iniciando scraping com proteções contra bloqueio...")
            
            start_time = time.time()
            
            target_url = start_url if start_url else self.base_url
            
            # todas as propriedades
            # html = self.browser.load_all_properties(self.base_url)
            # properties = self.data_extractor.extract_main_page_properties(html, self.base_url)

            # Carrega propriedades
            self.driver.get(target_url)
            html = self.driver.page_source
            properties = self.data_extractor.scrapMainPage(html)

            print(f"Propriedades encontradas na página principal: {len(properties)}")
            
            if not properties:
                print("Nenhuma propriedade encontrada na página principal para enriquecer. Encerrando.")
                return

            # Enriquecimento com detalhes
            enriched_properties = self.data_processor.enrich_with_details(
                properties,
                self.data_extractor.scrapItensPages,
                self.request_manager
            )
            
            print(f"Propriedades enriquecidas com detalhes: {len(enriched_properties)}")

            # Enriquecimento com processos
            final_properties = self.data_processor.enrich_with_details(
                enriched_properties,
                self.data_extractor.scrap_nested_page,
                self.request_manager
            )
            
            # Exportação
            output_filename = "portalzuk.csv"
            if self.file_exporter.export_to_csv(final_properties, output_filename):
                print(f"Dados exportados com sucesso para {output_filename}")
            
            end_time = time.time()
            print(f"Processo concluído em {end_time - start_time:.2f} segundos!")
            
        except Exception as e:
            print(f"Erro durante a execução: {str(e)}")
            traceback.print_exc()
        finally:
            self.browser.close()  # Fecha o browser corretamente