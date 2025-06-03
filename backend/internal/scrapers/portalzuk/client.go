package portalzuk

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/ps-vitor/leiloes-sys/backend/internal/domain"
)

func Scrape(ctx context.Context) ([]domain.Property, error) {
	cmd := exec.CommandContext(ctx, "python3", "-c", `
        from portalzuk.scraper import PortalzukScraper
        import json
        scraper = PortalzukScraper()
        data = scraper.run()
        print(json.dumps(data))
    `)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("python error: %v\nOutput: %s", err, string(output))
	}

	var properties []domain.Property
	if err := json.Unmarshal(output, &properties); err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}

	return properties, nil
}
