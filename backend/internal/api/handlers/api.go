package handlers

import (
	"net/http"

	"github.com/ps-vitor/leiloes-sys/backend/internal/services/property"
)

type APIHandler struct {
	propertyService *services.PropertyService
}

func NewAPIHandler(propertyService *services.PropertyService) *APIHandler {
	return &APIHandler{propertyService: propertyService}
}

func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/properties", h.handleProperties)
}

// Exemplo de handler
func (h *APIHandler) handleProperties(w http.ResponseWriter, r *http.Request) {
	properties, err := h.propertyService.FindAll()
	if err != nil {
		http.Error(w, "Erro ao buscar propriedades", http.StatusInternalServerError)
		return
	}
	// Aqui você pode serializar para JSON, por simplicidade, só mostrando o número:
	w.Write([]byte("Total de propriedades: "))
	w.Write([]byte(string(len(properties))))
}
