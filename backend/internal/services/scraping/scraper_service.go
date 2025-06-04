package services

import (
	"context"

	"github.com/ps-vitor/leiloes-sys/backend/internal/domain"
	"github.com/ps-vitor/leiloes-sys/backend/internal/repositories"
	"github.com/ps-vitor/leiloes-sys/backend/internal/scrapers/portalzuk"
)

type ScraperService struct {
	repo repositories.PropertyRepository
}

func ScrapePortalzuk() ([]domain.Property, error) {
	ctx := context.Background()
	return portalzuk.Scrape(ctx)
}

func NewScraperService(repo repositories.PropertyRepository) *ScraperService {
	return &ScraperService{repo: repo}
}

func (s *ScraperService) ScrapeAndStore(ctx context.Context) error {
	properties, err := portalzuk.Scrape(ctx) // Modificar a função Scrape
	if err != nil {
		return err
	}

	for _, p := range properties {
		if err := s.repo.Save(&p); err != nil {
			return err
		}
	}

	return nil
}
