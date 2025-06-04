package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/ps-vitor/leiloes-sys/backend/internal/api/handlers"
	config "github.com/ps-vitor/leiloes-sys/backend/internal/configs"
	"github.com/ps-vitor/leiloes-sys/backend/internal/repositories"
	services "github.com/ps-vitor/leiloes-sys/backend/internal/services/property"
)

func main() {
	// Carregar configurações
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Erro ao carregar configurações: %v", err)
	}

	// Inicializar repositórios
	propertyRepo := repositories.NewPropertyRepositoryInMemory() // Troque por implementação real se necessário

	// Inicializar serviços
	propertyService := services.NewPropertyService(propertyRepo)

	// Inicializar handlers
	apiHandler := handlers.NewAPIHandler(propertyService)

	// Configurar rotas
	mux := http.NewServeMux()
	apiHandler.RegisterRoutes(mux)

	addr := ":" + strconv.Itoa(cfg.App.Port)
	log.Printf("Servidor iniciado em %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}
