# infrastructure/docker/api.dockerfile
# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY backend/go.mod backend/go.sum ./
RUN go mod download

COPY backend/ .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /backend ./cmd/api

# Runtime stage
FROM alpine:3.18

WORKDIR /app
COPY --from=builder /backend /app/backend
COPY backend/config/config.yaml /app/config/

# Python para scrapers
RUN apk add --no-cache python3 py3-pip
COPY backend/scrapers /app/scrapers
RUN pip install --no-cache-dir -r /app/scrapers/portalzuk/requirements.txt

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:8080/health || exit 1
CMD ["/app/backend"]