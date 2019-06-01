#!/bin/bash
set -e
set -x

echo "Starting Schellar..."
schellar \
    --conductor-api-url="$CONDUCTOR_API_URL" \
    --mongo-address="$MONGO_ADDRESS" \
    --mongo-username=$MONGO_USERNAME \
    --mongo-password=$MONGO_PASSWORD \
    --loglevel=$LOG_LEVEL

