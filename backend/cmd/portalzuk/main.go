// ./backend/cmd/portalzuk/main.go

package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/ps-vitor/leiloes_sys/tree/goscraper/backend/internal/scraping/collectors/portalzuk"
)

func main() {
	collector := portalzuk.NewPortalZukCollector()
	properties, err := collector.Run()
	if err != nil {
		log.Fatalf("Error running scraper: %v", err)
	}

	jsonData, err := json.MarshalIndent(properties, "", "  ")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %v", err)
	}

	fmt.Println(string(jsonData))
}
