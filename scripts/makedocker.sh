#!/bin/bash
# scripts/makedocker.sh

cd "$(dirname "$0")/.."
sudo    make docker-build-api
sudo    make docker-run-api
sudo    make docker-build-scraping
sudo    make docker-run-scraping

