package repositories

import (
	"context"

	"github.com/ps-vitor/leiloes-sys/backend/internal/domain"
)

type PropertyRepository interface {
	Save(ctx context.Context, properties []domain.Property) error
	FindAll(ctx context.Context) ([]domain.Property, error)
}

type PostgresPropertyRepository struct {
	// Adicione dependências (DB connection)
}

// Implemente os métodos da interface
