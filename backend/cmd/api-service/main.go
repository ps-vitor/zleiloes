package main

import (
	"log"
	"net/http"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Erro ao carregar .env")
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("API funcionando"))
	})

	log.Println("API rodando na porta 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
