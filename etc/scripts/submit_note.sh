#!/usr/bin/env bash

set -euo pipefail

NOTE_TEXT="${1:-}"

if [ -z "$NOTE_TEXT" ]; then
  echo "Usage: $0 "Clinical note text...""
  exit 1
fi

NOTE_ID=$(uuidgen || cat /proc/sys/kernel/random/uuid)

PAYLOAD=$(cat <<JSON
{"noteId":"$NOTE_ID","patientId":"demo-patient","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","noteText":"$NOTE_TEXT","source":"cli"}
JSON
)

echo "$PAYLOAD" | docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic notes.raw >/dev/null

echo "OK: published noteId=$NOTE_ID to topic notes.raw"
