#!/bin/bash
# Criterio de aceptación: un mensaje inválido termina en la DLQ y el flujo sigue.
echo 'C-1234:esto-no-es-json' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic activation.requested \
  --property parse.key=true --property key.separator=:
echo "Revisá activation.requested.dlq en Kafka UI (http://localhost:8080)"
