#!/bin/bash
# Camino feliz + fallo: crea 2 activaciones y muestra el estado final y el historial.
API=${API:-http://localhost:3000}
for mode in none provisioning; do
  id=$(curl -s -X POST $API/activations -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"C-1234\",\"planId\":\"Plan Digital\",\"simulateFailure\":\"$mode\"}" | sed 's/.*"activationId":"\([^"]*\)".*/\1/')
  echo "== modo=$mode id=$id"; sleep 6; curl -s $API/activations/$id; echo
done
