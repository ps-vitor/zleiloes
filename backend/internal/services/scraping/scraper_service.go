package services

import (
	"context"

	"github.com/ps-vitor/leiloes-sys/backend/internal/repositories"
	"github.com/ps-vitor/leiloes-sys/backend/internal/scrapers/portalzuk"
)

type ScraperService struct {
	repo repositories.PropertyRepository
}

func NewScraperService(repo repositories.PropertyRepository) *ScraperService {
	return &ScraperService{repo: repo}
}

func (s *ScraperService) ScrapeAndStore(ctx context.Context) error {
	properties, err := portalzuk.Scrape(ctx) // Modificar a função Scrape
	if err != nil {
		return err
	}
	return s.repo.Save(ctx, properties)
}
