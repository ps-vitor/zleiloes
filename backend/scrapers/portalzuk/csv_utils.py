import csv
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class   CSV_Utils:
    def prepare_for_export(self, properties):
        """Prepara os dados para exportação CSV, achatando os preços em linhas separadas."""
        flat_properties = []
        for prop in properties:
            if "Preços" not in prop or not prop["Preços"]:
                base_prop = {
                    "Data": "", 
                    "endereco": prop.get("endereco", ""),
                    "Link": prop.get("Link", ""),
                    "tipo_imovel": prop.get("tipo_imovel", ""),
                    "rotulo": "N/A", 
                    "Valor (R$)": "N/A" 
                }
                for key, value in prop.items():
                    if key not in ["Preços", "Data", "endereco", "Link", "tipo_imovel", "rotulo", "Valor (R$)"]:
                        base_prop[key] = value
                flat_properties.append(base_prop)
            else:
                for price in prop["Preços"]:
                    flat_prop = {
                        "Data": price.get("Data", ""),
                        "endereco": prop.get("endereco", ""),
                        "Link": prop.get("Link", ""),
                        "tipo_imovel": prop.get("tipo_imovel", ""),
                        "rotulo": price.get("Tipo", ""),
                        "Valor (R$)": price.get("Valor", "")
                    }
                    
                    for key, value in prop.items():
                        if key not in ["Preços", "Data", "endereco", "Link", "tipo_imovel", "rotulo", "Valor (R$)"]:
                            flat_prop[key] = value
                    
                    flat_properties.append(flat_prop)
        
        return flat_properties

    def export_to_csv(self, properties, filename="portalzuk.csv"):
        """Exporta os dados das propriedades para um arquivo CSV."""
        if not properties:
            print("Nenhum dado para exportar (lista de propriedades vazia).")
            return False

        flat_data = self.prepare_for_export(properties)
        
        if not flat_data:
            print("Nenhum dado válido para exportação após preparo (lista achatada vazia).")
            return False

        fieldnames = set()
        for row in flat_data:
            fieldnames.update(row.keys())
        
        preferred_order = [
            "tipo_imovel", "rotulo", "endereco", "Link", "Valor (R$)", 
            "Data", "Matrícula", "ocupado", "leiloeiro", "Descrição do imóvel",
            "Formas de Pagamento", "Direito de Preferência", 
            "Observações Gerais", "Direitos do Compromissário"
        ]
        
        photo_fields = sorted([f for f in fieldnames if f.startswith("Foto_")])
        if photo_fields:
            preferred_order.extend(photo_fields)
            preferred_order.append("Total_Fotos")

        other_fieldnames = sorted([f for f in fieldnames if f not in preferred_order])
        final_fieldnames = preferred_order + other_fieldnames
        
        try:
            with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
                writer.writeheader()
                writer.writerows(flat_data)
            
            print(f"Dados exportados com sucesso para {filename}")
            print(f"Total de registros no CSV: {len(flat_data)}")
            return True
        except Exception as e:
            print(f"Erro ao exportar CSV: {str(e)}")
            traceback.print_exc()
            return False