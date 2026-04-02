#!/bin/bash
docker volume create --driver local \
 -o o=bind \
 -o type=none \
 -o device="/home/ubuntu/Ignite/flashpoint-docker/telemetry" \
 flashpoint-telemetry

docker build -t flashpoint-ingest .

