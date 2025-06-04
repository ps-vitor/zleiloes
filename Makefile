# Makefile (na raiz)

BINARY_NAME=scraping-service

.PHONY: build run docker-build docker-run clean

build:
	go	build	-o	api	./cmd/api/
	go build -o scraping-service ./cmd/scraping-service

run:
	go	run	./cmd/api/main.go
	go run ./cmd/scraping-service/main.go

docker-build-api:
	docker build	-f	infrastructure/docker/api.dockerfile -t api .
docker-run-api:
	docker run --rm	-p	8080:8080	api

docker-build-scraping:
	docker build	-f	infrastructure/docker/scraping.dockerfile -t scraping-service .
docker-run-scraping:
	docker	run	--rm	--dns	8.8.8.8	-p	8081:8081	scraping-service

clean:
	rm -f scraping-service
	rm -f api