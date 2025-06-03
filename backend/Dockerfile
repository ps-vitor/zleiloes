FROM golang:1.24.3-alpine

# Instalar dependências necessárias
RUN apk update && apk add --no-cache git python3 py3-pip

# Diretório de trabalho
WORKDIR /leiloes-sys

# Copiar arquivos do projeto
COPY . .

# Instalar dependências do Python (se necessário)
RUN python3 -m venv /venv && \
    . /venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r scrapers/portalzuk/requirements.txt

# Baixar dependências Go
RUN go mod download

# Compilar o serviço Go
RUN go build -o scraping-service ./cmd/scraping-service

# Expor a porta do servidor Go
EXPOSE 8001


# Rodar o binário
CMD ["./scraping-service"]
