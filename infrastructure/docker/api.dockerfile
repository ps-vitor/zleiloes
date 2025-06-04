# infrastructure/docker/api.dockerfile
# Build stage
FROM golang:1.24.3-alpine AS builder

WORKDIR /app

# Copie go.mod e go.sum da raiz do projeto
COPY go.mod go.sum ./
RUN go mod download

# Copie todo o código fonte do projeto (ajustado para contexto correto)
COPY . .

# Build do binário da API
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /backend ./backend/cmd/api

# Runtime stage
FROM alpine:3.18

WORKDIR /app
COPY --from=builder /backend /app/backend
COPY backend/configs/config.yaml /app/configs/app.yaml

# Python para scrapers
RUN apk add --no-cache python3 py3-pip
COPY backend/scrapers /app/scrapers
RUN pip install --no-cache-dir -r /app/scrapers/portalzuk/requirements.txt

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:8080/health || exit 1
CMD ["/app/backend"]