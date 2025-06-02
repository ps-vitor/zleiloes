// backend/internal/scraping/services/portalzuk/service.go
package portalzuk

import (
	"encoding/json"
	"log"
	"os/exec"
)

type Property struct {
	// Defina a estrutura baseada no seu retorno Python
}

func Scrape() ([]Property, error) {
	cmd := exec.Command("python3", "-m", "backend.internal.scraping.collectors.portalzuk.collector")

	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var properties []Property
	if err := json.Unmarshal(output, &properties); err != nil {
		log.Printf("Error decoding Python output: %v", err)
		return nil, err
	}

	return properties, nil
}
