// backend/internal/scrapers/portalzuk/client.go
package portalzuk

import (
	"encoding/json"
	"log"
	"os/exec"
)

type Property struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Price float64 `json:"price"`
}

func Scrape() ([]Property, error) {
	cmd := exec.Command("python3", "../scrapers/portalzuk/scraper.py")

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Python error: %s\n", output)
		return nil, err
	}

	var properties []Property
	if err := json.Unmarshal(output, &properties); err != nil {
		return nil, err
	}

	return properties, nil
}
