// backend/internal/scraping/collectors/portalzuk/collector.go
package portalzuk

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/gocolly/colly"
)

type Property struct {
	Label           string `json:"rotulo"`
	Value           string `json:"valor_r"`
	Date            string `json:"data"`
	Lot             string `json:"lote"`
	Address         string `json:"endereco"`
	Status          string `json:"situacao"`
	Registration    string `json:"matricula_imovel"`
	Notes           string `json:"observacoes"`
	ProcessLink     string `json:"link_processo"`
	VisitInfo       string `json:"visitacao"`
	BuyerRights     string `json:"direitos_comprador"`
	PreferenceRight string `json:"direito_preferencia"`
	PaymentInfo     string `json:"info_pagamento"`
	Link            string `json:"link"`
}

type PortalZukCollector struct {
	collector *colly.Collector
	baseURL   string
}

func NewPortalZukCollector() *PortalZukCollector {
	c := colly.NewCollector(
		colly.AllowedDomains("www.portalzuk.com.br"),
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36"),
	)

	// Limitar paralelismo e aleatorizar delays
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2,
		RandomDelay: 2 * 1000,
	})

	return &PortalZukCollector{
		collector: c,
		baseURL:   "https://www.portalzuk.com.br",
	}
}

func (p *PortalZukCollector) scrapePropertyDetails(url string) (map[string]string, error) {
	details := make(map[string]string)
	var err error

	c := p.collector.Clone()

	// Configurar manipuladores para os detalhes da propriedade
	c.OnHTML("div.property-featured-items", func(e *colly.HTMLElement) {
		e.ForEach("div.property-featured-item", func(_ int, el *colly.HTMLElement) {
			label := el.ChildText("span.property-featured-item-label")
			value := el.ChildText("span.property-featured-item-value")
			if label != "" && value != "" {
				details[label] = strings.TrimSpace(value)
			}
		})
	})

	c.OnHTML("div.content", func(e *colly.HTMLElement) {
		// Situação
		if status := e.ChildText("span.property-status-title"); status != "" {
			details["situacao"] = strings.TrimSpace(status)
		}

		// Matrícula
		if registration := e.ChildText("p#itens_matricula.text_subtitle"); registration != "" {
			details["matricula_imovel"] = strings.TrimSpace(registration)
		}

		// Observações
		if notes := e.ChildText("div.property-info-text.div-text-observacoes"); notes != "" {
			details["observacoes"] = strings.TrimSpace(notes)
		}

		// Link do processo
		if processLink := e.ChildAttr("a.glossary-link", "href"); processLink != "" {
			details["link_processo"] = strings.TrimSpace(processLink)
		}

		// Visitação
		if visitInfo := e.ChildText("div.property-info-text"); visitInfo != "" {
			details["visitacao"] = strings.TrimSpace(visitInfo)
		}

		// Pagamento
		paymentTitle := e.ChildText("h3.property-info-title")
		if paymentTitle != "" {
			paymentInfo := e.ChildText("p.property-payments-item-text")
			if paymentInfo != "" {
				details["info_pagamento"] = strings.TrimSpace(paymentInfo)
			}
		}

		// Direitos do Comprador
		if buyerRights := e.ChildText("p.property-status-text"); buyerRights != "" {
			details["direitos_comprador"] = strings.TrimSpace(buyerRights)
		}

		// Direito de Preferência
		if preferenceRight := e.ChildText("p.text_subtitle"); preferenceRight != "" {
			details["direito_preferencia"] = strings.TrimSpace(preferenceRight)
		}
	})

	c.OnError(func(r *colly.Response, e error) {
		err = fmt.Errorf("request URL %v failed with response %v: %v", r.Request.URL, r, e)
	})

	// Visitar a URL
	if err := c.Visit(url); err != nil {
		return nil, err
	}

	c.Wait()

	if err != nil {
		return nil, err
	}

	return details, nil
}

func (p *PortalZukCollector) scrapeMainPage(url string) ([]Property, error) {
	var properties []Property
	var err error

	c := p.collector.Clone()

	c.OnHTML("section.s-list-properties", func(e *colly.HTMLElement) {
		e.ForEach("div.card-property", func(_ int, card *colly.HTMLElement) {
			prop := Property{}

			// Link da propriedade
			link := card.ChildAttr("div.card-property-image-wrapper a", "href")
			if link != "" && !strings.HasPrefix(link, "http") {
				link = p.baseURL + link
			}
			prop.Link = link

			// Lote
			prop.Lot = strings.TrimSpace(card.ChildText("span.card-property-price-lote"))

			// Endereço
			prop.Address = strings.TrimSpace(card.ChildText("div.card-property-address"))

			// Preços
			card.ForEach("ul.card-property-prices", func(_ int, ul *colly.HTMLElement) {
				ul.ForEach("li.card-property-price", func(_ int, li *colly.HTMLElement) {
					label := strings.TrimSpace(li.ChildText("span.card-property-price-label"))
					value := strings.TrimSpace(li.ChildText("span.card-property-price-value"))
					date := strings.TrimSpace(li.ChildText("span.card-property-price-data"))

					if label != "" && value != "" && date != "" {
						// Limpar valor monetário
						value = cleanCurrency(value)

						prop.Label = label
						prop.Value = value
						prop.Date = date
						properties = append(properties, prop)
					}
				})
			})
		})
	})

	c.OnError(func(r *colly.Response, e error) {
		err = fmt.Errorf("request URL %v failed with response %v: %v", r.Request.URL, r, e)
	})

	// Visitar a URL principal
	if err := c.Visit(url); err != nil {
		return nil, err
	}

	c.Wait()

	if err != nil {
		return nil, err
	}

	return properties, nil
}

func cleanCurrency(value string) string {
	// Remove R$, pontos e substitui vírgula por ponto
	re := regexp.MustCompile(`[R\$\s.]`)
	cleaned := re.ReplaceAllString(value, "")
	return strings.Replace(cleaned, ",", ".", 1)
}

func (p *PortalZukCollector) Run() ([]Property, error) {
	mainURL := p.baseURL + "/leilao-de-imoveis/"

	// Coletar lista de propriedades
	properties, err := p.scrapeMainPage(mainURL)
	if err != nil {
		return nil, fmt.Errorf("error scraping main page: %v", err)
	}

	// Coletar detalhes em paralelo
	var wg sync.WaitGroup
	type result struct {
		index int
		data  map[string]string
	}
	results := make(chan result, len(properties))
	errChan := make(chan error, len(properties))

	for i, prop := range properties {
		if prop.Link == "" {
			continue
		}

		wg.Add(1)
		go func(idx int, url string) {
			defer wg.Done()

			details, err := p.scrapePropertyDetails(url)
			if err != nil {
				errChan <- fmt.Errorf("error scraping %s: %v", url, err)
				return
			}

			results <- result{idx, details}
		}(i, prop.Link)
	}

	go func() {
		wg.Wait()
		close(results)
		close(errChan)
	}()

	// Processar resultados
	for res := range results {
		idx := res.index
		details := res.data

		// Mapear detalhes para a struct Property
		if status, ok := details["situacao"]; ok {
			properties[idx].Status = status
		}
		if registration, ok := details["matricula_imovel"]; ok {
			properties[idx].Registration = registration
		}
		if notes, ok := details["observacoes"]; ok {
			properties[idx].Notes = notes
		}
		if processLink, ok := details["link_processo"]; ok {
			properties[idx].ProcessLink = processLink
		}
		if visitInfo, ok := details["visitacao"]; ok {
			properties[idx].VisitInfo = visitInfo
		}
		if paymentInfo, ok := details["info_pagamento"]; ok {
			properties[idx].PaymentInfo = paymentInfo
		}
		if buyerRights, ok := details["direitos_comprador"]; ok {
			properties[idx].BuyerRights = buyerRights
		}
		if preferenceRight, ok := details["direito_preferencia"]; ok {
			properties[idx].PreferenceRight = preferenceRight
		}
	}

	// Verificar erros
	var errs []error
	for e := range errChan {
		errs = append(errs, e)
	}

	if len(errs) > 0 {
		return properties, fmt.Errorf("encountered %d errors during scraping", len(errs))
	}

	return properties, nil
}
