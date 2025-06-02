// ./backend/internal/api/services/listing_service.go

package services

import (
	"context"
	"log"
	"os"
	"time"

	"leiloes-sys/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchListings() []models.Listing {
	mongoURI := os.Getenv("MONGO_URI")
	dbName := os.Getenv("MONGO_DB_NAME")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Println("Erro ao conectar ao MongoDB:", err)
		return nil
	}
	defer client.Disconnect(ctx)

	collection := client.Database(dbName).Collection("listings")

	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		log.Println("Erro ao buscar documentos:", err)
		return nil
	}
	defer cursor.Close(ctx)

	var listings []models.Listing
	if err = cursor.All(ctx, &listings); err != nil {
		log.Println("Erro ao decodificar documentos:", err)
		return nil
	}

	return listings
}
