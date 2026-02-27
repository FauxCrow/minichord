#!/bin/bash
set -euo pipefail

# Configure setup variables
REGISTRY_HOST=127.0.0.1
REGISTRY_PORT=3000
NODES=9

# Setup messagers based on NODES number
echo "Starting $NODES Messenger Nodes..."
cd messenger
for i in $(seq 1 $NODES); do
    go run messenger.go $REGISTRY_HOST:$REGISTRY_PORT &
    sleep 0.5
done

echo "Setup complete!"

wait
