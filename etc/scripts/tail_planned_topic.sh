#!/usr/bin/env bash

set -euo pipefail

TOPIC="${1:-notes.planned}"

docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$TOPIC" --from-beginning "$@"
