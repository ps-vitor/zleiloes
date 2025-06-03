// backend/internal/domain/scraper.go

type Property struct {
	ID    string
	Title string
	Price float64
	// other fields from Python version
}

// internal/repositories/
type PropertyRepository interface {
	Save(properties []domain.Property) error
	FindAll() ([]domain.Property, error)
}

// internal/services/
type ScraperService struct {
	repo PropertyRepository
}

func (s *ScraperService) ScrapeAndStore() error {
	// call Python scraper
	// process results
	// store in repository
}