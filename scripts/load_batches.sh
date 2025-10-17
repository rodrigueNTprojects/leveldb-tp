#!/bin/bash
# Script pour charger des lots de données avec offset

for i in {1..10}; do
    offset=$((i * 500))
    ./bin/loader -csv ./data -limit 500 -offset $offset
    echo "Lot $i chargé"
done