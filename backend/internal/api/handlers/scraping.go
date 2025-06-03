// backend/internal/api/handlers/scraping.go

package handlers

import (
	"net/http"

	"github.com/ps-vitor/leiloes-sys/backend/internal/services"
)

type ScrapingHandler struct {
	scraperService *services.ScraperService
}

func NewScrapingHandler(svc *services.ScraperService) *ScrapingHandler {
	return &ScrapingHandler{scraperService: svc}
}

func (h *ScrapingHandler) HandleScrape(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := h.scraperService.ScrapeAndStore(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Scraping completed successfully"))
}
