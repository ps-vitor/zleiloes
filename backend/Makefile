# Makefile (na raiz de backend)

BINARY_NAME=scraping-service

.PHONY: build run docker-build docker-run clean

build:
	go build -o $(BINARY_NAME) ./cmd/scraping-service

run:
	go run ./cmd/scraping-service/main.go

docker-build:
	docker build -t $(BINARY_NAME) .

docker-run:
	docker run --rm -p 8001:8001 $(BINARY_NAME)

clean:
	rm -f $(BINARY_NAME)
