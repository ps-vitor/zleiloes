import csv

class FileExporter:
    def export_to_csv(self, data, filename):
        # Coletar todos os fieldnames possíveis
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        
        fieldnames = sorted(fieldnames)
        
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            return True
        except Exception as e:
            print(f"Erro ao exportar CSV: {str(e)}")
            return False