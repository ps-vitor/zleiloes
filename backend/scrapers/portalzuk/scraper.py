from bs4 import BeautifulSoup
import requests, traceback, csv
from datetime import datetime
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed


class PortalzukScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36'
        })

    def scrap_item_page(self, url):
        """Raspa dados da página de detalhes de um imóvel"""
        try:
            if not url:
                print("[ERRO] URL está None!")
                return {}

            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            items = soup.find_all("div", class_="property-featured-item")
            item_data = {}
            for item in items:
                label = item.find("span", class_="property-featured-item-label")
                value = item.find("span", class_="property-featured-item-value")
                if label and value:
                    item_data[label.get_text(strip=True)] = value.get_text(strip=True)

            return item_data

        except Exception as e:
            print(f"[ERRO scraping detalhes] {url} -> {e}")
            traceback.print_exc()
            return {}

    def scrap_main_page(self, url):
        """Raspa a página principal e retorna um dicionário indexado por link"""
        properties = {}

        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            section = soup.find(class_="s-list-properties")
            cards = section.find_all(class_="card-property") if section else []

            for card in cards:
                address_tag = card.find(class_="card-property-address")
                lote_tag = card.find(class_="card-property-price-lote")

                image_wrapper = card.find("div", class_="card-property-image-wrapper")
                link_tag = image_wrapper.find("a") if image_wrapper else None
                link = link_tag["href"] if link_tag else None
                if link and not link.startswith("http"):
                    link = "https://www.portalzuk.com.br" + link

                if not link:
                    continue

                # Dados comuns
                common_data = {
                    "Lote": lote_tag.get_text(strip=True) if lote_tag else None,
                    "Endereco": address_tag.get_text(separator=" ", strip=True) if address_tag else None,
                    "link": link,
                    "Precos": [],
                }

                # Lista de preços associados
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

                # Armazena pelo link como chave única
                properties[link] = common_data

        except Exception as e:
            print(f"[ERRO scraping principal] {e}")
            traceback.print_exc()

        return properties

    def run(self):
        try:
            base_url = "https://www.portalzuk.com.br/leilao-de-imoveis/"
            propriedades = self.scrap_main_page(base_url)

            if not propriedades:
                print("Nenhuma propriedade encontrada.")
                return {"error": "Nenhum dado coletado", "properties": []}

            # Coleta informações adicionais de forma paralela
            with ThreadPoolExecutor(max_workers=min(cpu_count() * 2, len(propriedades))) as executor:
                futures = {
                    executor.submit(self.scrap_item_page, link): link
                    for link in propriedades
                }

                for future in as_completed(futures):
                    link = futures[future]
                    try:
                        detalhes = future.result()
                        if detalhes:
                            propriedades[link].update(detalhes)
                    except Exception as e:
                        print(f"Erro ao processar detalhes de {link}: {e}")

            # Preparar dados para exportação
            export_rows = []
            all_keys = set()

            for prop in propriedades.values():
                for preco in prop.pop("Precos", []):
                    row = {**prop, **preco}
                    export_rows.append(row)
                    all_keys.update(row.keys())

            # Campos fixos + outros ordenados
            base_fields = ["Rotulo", "Valor (R$)", "Data", "Lote", "Endereco", "link"]
            fieldnames = base_fields + sorted(k for k in all_keys if k not in base_fields)

            # Exportar CSV
            with open("portalzuk.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in export_rows:
                    writer.writerow(row)

            print(f"\n✅ Dados exportados para 'portalzuk.csv' ({len(export_rows)} registros)\n")
            return {
                "properties": export_rows,
                "metadata": {
                    "source": "portalzuk",
                    "scraped_at": datetime.now().isoformat(),
                    "count": len(export_rows)
                }
            }

        except Exception as e:
            return {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
