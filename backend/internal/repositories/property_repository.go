package repositories

import (
	"errors"

	"github.com/ps-vitor/leiloes-sys/backend/internal/domain"
)

// PropertyRepository define as operações de acesso a dados para Property.
type PropertyRepository interface {
	Save(property *domain.Property) error
	FindByID(id int) (*domain.Property, error)
	FindAll() ([]*domain.Property, error)
	Delete(id int) error
}

// Em memória (mock). Substitua por implementação real (ex: banco de dados) depois.
type propertyRepositoryInMemory struct {
	data map[int]*domain.Property
}

func NewPropertyRepositoryInMemory() PropertyRepository {
	return &propertyRepositoryInMemory{
		data: make(map[int]*domain.Property),
	}
}

func (r *propertyRepositoryInMemory) Save(property *domain.Property) error {
	if property == nil {
		return errors.New("property is nil")
	}
	r.data[property.ID] = property
	return nil
}

func (r *propertyRepositoryInMemory) FindByID(id int) (*domain.Property, error) {
	prop, ok := r.data[id]
	if !ok {
		return nil, errors.New("property not found")
	}
	return prop, nil
}

func (r *propertyRepositoryInMemory) FindAll() ([]*domain.Property, error) {
	properties := make([]*domain.Property, 0, len(r.data))
	for _, p := range r.data {
		properties = append(properties, p)
	}
	return properties, nil
}

func (r *propertyRepositoryInMemory) Delete(id int) error {
	if _, ok := r.data[id]; !ok {
		return errors.New("property not found")
	}
	delete(r.data, id)
	return nil
}
