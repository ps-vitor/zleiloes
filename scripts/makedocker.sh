#!/bin/bash
# scripts/makedocker.sh

# Navega para o diretório raiz do projeto
cd "$(dirname "$0")/../" || exit 1

DOCKER_COMPOSE_FILE="infrastructure/docker/docker-compose.yml"

# Verifica se o docker-compose.yml existe
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    echo "Erro: docker-compose.yml não encontrado em $DOCKER_COMPOSE_FILE!"
    exit 1
fi

# Remove containers antigos
echo "Removendo containers antigos..."
sudo docker-compose -f "$DOCKER_COMPOSE_FILE" down

# Constrói e sobe os containers
echo "Construindo e iniciando os containers..."
sudo docker-compose -f "$DOCKER_COMPOSE_FILE" up -d --build

# Verifica o status
echo -e "\nStatus dos containers:"
sudo docker-compose -f "$DOCKER_COMPOSE_FILE" ps

echo -e "\nServiços disponíveis:"
echo "API: http://localhost:8080"
echo "Scraping Service: http://localhost:8081"