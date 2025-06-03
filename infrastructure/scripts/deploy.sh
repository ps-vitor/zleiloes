#!/bin/bash

# Build and push images
docker-compose -f docker-compose.build.yml build
docker push your-registry/backend:latest
docker push your-registry/scraping-service:latest

# Kubernetes deployment
kubectl apply -f k8s/