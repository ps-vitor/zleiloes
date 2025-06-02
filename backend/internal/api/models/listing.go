// ./backend/api/models/listing.go

package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type Listing struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Title        string             `bson:"title" json:"title"`
	Location     string             `bson:"location" json:"location"`
	Price        float64            `bson:"price" json:"price"`
	Link         string             `bson:"link" json:"link"`
	Description  string             `bson:"description" json:"description"`
	AuctionDate  string             `bson:"auction_date" json:"auction_date"`
	JudicialType string             `bson:"judicial_type" json:"judicial_type"`
}
