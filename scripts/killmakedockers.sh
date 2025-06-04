#!/bin/bash
sudo    docker kill $(sudo  docker ps -q)
pkill   -f  make