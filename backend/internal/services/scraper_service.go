// internal/services/scraper_service.go
package services

import (
	"fmt"
	"net/http"

	"github.com/PuerkitoBio/goquery"
)

type Auction struct {
	Title string `json:"title"`
	Price string `json:"price"`
	Link  string `json:"link"`
}

func ScrapePortalzuk() ([]Auction, error) {
	res, err := http.Get("https://www.portalzuk.com.br/leiloes")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d", res.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
	}

	var auctions []Auction

	doc.Find(".auction-card").Each(func(i int, s *goquery.Selection) {
		title := s.Find(".title").Text()
		price := s.Find(".price").Text()
		link, _ := s.Find("a").Attr("href")

		auctions = append(auctions, Auction{
			Title: title,
			Price: price,
			Link:  link,
		})
	})

	return auctions, nil
}
