// backend/cmd/api/main.go
package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ps-vitor/leiloes-sys/backend/internal/scrapers/portalzuk"
)

func handleScrape(w http.ResponseWriter, r *http.Request) {
	properties, err := portalzuk.Scrape()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(properties)
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/api/scrape", handleScrape).Methods("GET")

	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
