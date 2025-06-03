# infrastructure/docker/scraping.dockerfile
# Python base
FROM python:3.9-slim as python-base
WORKDIR /app
COPY backend/scrapers/portalzuk/requirements.txt .
RUN python -m venv /venv && \
    . /venv/bin/activate && \
    pip install --no-cache-dir -r requirements.txt

# Go build
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ .
RUN CGO_ENABLED=0 GOOS=linux go build -o /scraping-service ./cmd/scraping-service

# Runtime
FROM alpine:3.18
WORKDIR /app

# Copia Python
COPY --from=python-base /venv /venv
COPY backend/scrapers /app/scrapers

# Copia Go
COPY --from=builder /scraping-service /app/
COPY backend/config/config.yaml /app/config/

ENV PATH="/venv/bin:$PATH"
EXPOSE 8081
CMD ["/app/scraping-service"]