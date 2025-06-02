package services

import (
	"bytes"
	"net/http"
	"os"
)

func TriggerScraper() error {
	scraperURL := os.Getenv("SCRAPER_URL")
	_, err := http.Post(scraperURL+"/run", "application/json", bytes.NewBuffer([]byte(`{}`)))
	return err
}
