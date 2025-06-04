# infrastructure/docker/scraping.dockerfile
# Python base
FROM python:3.9-slim as python-base
WORKDIR /app
COPY backend/scrapers/portalzuk/requirements.txt .
RUN python -m venv /venv && \
    . /venv/bin/activate && \
    pip install --no-cache-dir -r requirements.txt

# Go build
FROM golang:1.24.3-alpine AS builder
WORKDIR /app

# Copie go.mod e go.sum da raiz do projeto
COPY go.mod go.sum ./
RUN go mod download

# Copie todo o projeto (incluindo o backend, go.mod e go.sum)
COPY . .

# Agora, o caminho relativo funciona como no seu dev local
RUN CGO_ENABLED=0 GOOS=linux go build -o /scraping-service ./backend/cmd/scraping-service

# Runtime
FROM python:3.9-slim
WORKDIR /app

COPY --from=python-base /venv /venv
COPY backend/scrapers /app/scrapers
COPY --from=builder /scraping-service /app/
COPY backend/configs/config.yaml /app/configs/
ENV PATH="/venv/bin:$PATH"
EXPOSE 8081
CMD ["/app/scraping-service"]