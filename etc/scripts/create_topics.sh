#!/usr/bin/env bash

set -euo pipefail

topics=(notes.raw notes.normalized notes.extracted notes.diagnosed notes.planned)

for t in "${topics[@]}"; do
  docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$t" --replication-factor 1 --partitions 1 || true
done

echo "Topics ensured: ${topics[*]}"
