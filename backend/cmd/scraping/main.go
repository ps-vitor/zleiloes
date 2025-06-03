// backend/cmd/scraping-service/main.go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os/exec"

	"github.com/ps-vitor/leiloes-sys/backend/internal/services"
)

func main() {
	log.Println("Executando scraper...")

	cmd := exec.Command("/venv/bin/python3", "./scrapers/main.py")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Erro ao executar o scraper: %v\nOutput:%s", err, string(output))
	}
	log.Printf("Scraper finalizado:\n%s\n", string(output))
	log.Println("Iniciando servidor 8081...")

	http.HandleFunc("/scrape/portalzuk", func(w http.ResponseWriter, r *http.Request) {
		data, err := services.ScrapePortalzuk()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	})

	log.Println("Scraping service on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
