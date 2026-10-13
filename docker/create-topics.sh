#!/bin/bash
# Crea los 4 topics de negocio y sus DLQ: 3 particiones, retención de 7 días.
set -e
KT=/opt/kafka/bin/kafka-topics.sh
for t in activation.requested billing.events provisioning.events activation.events; do
  for name in "$t" "$t.dlq"; do
    $KT --bootstrap-server kafka:9092 --create --if-not-exists --topic "$name" \
        --partitions 3 --replication-factor 1 --config retention.ms=604800000
  done
done
$KT --bootstrap-server kafka:9092 --list
