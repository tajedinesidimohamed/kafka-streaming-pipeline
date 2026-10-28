#!/usr/bin/env bash
# Crée le topic "network-events" : 3 partitions, réplication 3, min.insync.replicas=2.
# Idempotent : ne fait rien si le topic existe déjà, puis affiche sa description
# (leader, réplicas et ISR de chaque partition).
set -euo pipefail

TOPIC="${TOPIC:-network-events}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-3}"
BROKER_CONTAINER="${BROKER_CONTAINER:-kafka-1}"
BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# MSYS_NO_PATHCONV évite que Git Bash (Windows) réécrive /opt/... en C:/Program Files/Git/opt/...
kafka() {
  MSYS_NO_PATHCONV=1 docker exec "$BROKER_CONTAINER" "/opt/kafka/bin/$@"
}

echo ">> Création du topic '$TOPIC' ($PARTITIONS partitions, réplication $REPLICATION, min.insync.replicas=2)"
kafka kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --config min.insync.replicas=2

echo
echo ">> Description du topic"
kafka kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
