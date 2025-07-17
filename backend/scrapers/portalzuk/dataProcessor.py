# dataProcessor.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import  traceback

class DataProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers

    def enrich_with_details(self, properties, extract_function, request_manager):
        """
        Enriquece propriedades com detalhes extraídos.
        
        Args:
            properties: Lista de propriedades básicas
            extract_function: Função para extrair detalhes (de DataExtractor)
            request_manager: Gerenciador de requisições HTTP
        """
        print(f"Enriquecendo {len(properties)} propriedades com detalhes...")
        
        enriched_properties = [dict(prop) for prop in properties]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(
                    self._fetch_property_details, 
                    prop["Link"], 
                    extract_function,
                    request_manager
                ): i 
                for i, prop in enumerate(enriched_properties) 
                if prop.get("Link")
            }
            
            for future in as_completed(future_to_index):
                i = future_to_index[future]
                try:
                    details = future.result()
                    if details:
                        enriched_properties[i].update(details)
                except Exception as e:
                    print(f"Erro ao enriquecer propriedade: {str(e)}")
        
        return enriched_properties

    def _fetch_property_details(self, url, extract_function, request_manager):
        """Agora extract_function precisa apenas da URL"""
        try:
            return extract_function(url)  # O request_manager já está na instância
        except Exception as e:
            print(f"Erro ao processar {url}: {str(e)}")
            traceback.print_exc()
            return {}