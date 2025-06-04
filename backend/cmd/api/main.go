// backend/cmd/api/main.go
package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ps-vitor/leiloes-sys/backend/config"
	"github.com/ps-vitor/leiloes-sys/backend/internal/api/handlers"
	services "github.com/ps-vitor/leiloes-sys/backend/internal/services/scraping"
)

func main() {
	cfg := config.Load()

	// Setup dependencies
	repo := repositories.PostgresPropertyRepository(cfg.Database)
	scraperSvc := services.NewScraperService(repo)
	scrapingHandler := handlers.NewScrapingHandler(scraperSvc)

	r := mux.NewRouter()
	r.HandleFunc("/api/scrape", scrapingHandler.HandleScrape).Methods("GET")
	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
