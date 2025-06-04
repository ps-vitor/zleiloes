package services

import (
	"github.com/ps-vitor/leiloes-sys/backend/internal/domain"
	"github.com/ps-vitor/leiloes-sys/backend/internal/repositories"
)

type PropertyService struct {
	repo repositories.PropertyRepository
}

func NewPropertyService(repo repositories.PropertyRepository) *PropertyService {
	return &PropertyService{repo: repo}
}

// Exemplo de método
func (s *PropertyService) FindAll() ([]*domain.Property, error) {
	return s.repo.FindAll()
}
