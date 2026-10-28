#!/usr/bin/env bash
# Démonstration de la tolérance aux pannes.
#
# Partie 1 — panne d'un broker :
#   un producteur et l'archiveur tournent ; on arrête le broker leader de la
#   partition 0, on montre l'élection d'un nouveau leader, l'ISR réduit à 2
#   et le flux qui continue ; puis on redémarre le broker et il se resynchronise.
#
# Partie 2 — rééquilibrage d'un consumer group :
#   2 consommateurs se partagent les partitions ; on en arrête un, l'autre
#   reprend toutes les partitions.
#
# Usage : bash scripts/test-broker-failure.sh [all|broker|consumer]   (défaut : all)
set -euo pipefail

cd "$(dirname "$0")/.."

PART="${1:-all}"
TOPIC="${TOPIC:-network-events}"
RATE="${RATE:-10}"
OUTAGE_SECONDS="${OUTAGE_SECONDS:-20}"
INTERNAL_BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
LOG_DIR="$(mktemp -d)"
STOPPED_BROKER=""
PIDS=()

if [ -z "${PYTHON:-}" ]; then
  if [ -x .venv/Scripts/python.exe ]; then PYTHON=.venv/Scripts/python.exe
  elif [ -x .venv/bin/python ]; then PYTHON=.venv/bin/python
  else PYTHON=python
  fi
fi

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }

# Exécute un outil Kafka dans un broker qui tourne (pas celui qu'on a arrêté).
# MSYS_NO_PATHCONV empêche Git Bash de réécrire les chemins /opt/...
kafka() {
  local container
  for container in kafka-1 kafka-2 kafka-3; do
    if [ "$container" != "$STOPPED_BROKER" ]; then break; fi
  done
  MSYS_NO_PATHCONV=1 docker exec "$container" "/opt/kafka/bin/$@"
}

describe_topic() {
  kafka kafka-topics.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --topic "$TOPIC" \
    | grep 'Partition:' | awk '{print "    " $3, $4, $5, $6, $7, $8, $9, $10}'
}

end_offsets() {
  kafka kafka-get-offsets.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --topic "$TOPIC" \
    | awk -F: '{printf "p%s=%s  ", $2, $3; total += $3} END {printf "| total=%d\n", total}'
}

wait_healthy() {
  local container="$1"
  for _ in $(seq 1 60); do
    if [ "$(docker inspect -f '{{.State.Health.Status}}' "$container")" = "healthy" ]; then return 0; fi
    sleep 2
  done
  echo "Le conteneur $container n'est pas redevenu healthy" >&2
  return 1
}

# Attend que chaque partition ait de nouveau 3 répliques dans l'ISR
wait_full_isr() {
  for _ in $(seq 1 60); do
    if kafka kafka-topics.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --topic "$TOPIC" \
        | grep 'Partition:' | awk '{for (i = 1; i <= NF; i++) if ($i == "Isr:") print $(i+1)}' \
        | awk -F, 'NF != 3 {bad = 1} END {exit bad}'; then
      return 0
    fi
    sleep 2
  done
  echo "L'ISR n'est pas revenu à 3 répliques" >&2
  return 1
}

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  if [ -n "$STOPPED_BROKER" ]; then
    echo "Redémarrage de $STOPPED_BROKER (nettoyage)…"
    docker start "$STOPPED_BROKER" > /dev/null || true
  fi
  rm -rf "$LOG_DIR"
}
trap cleanup EXIT

broker_failure() {
  local leader broker producer_pid archiver_pid group lag

  step "Partie 1 — Panne d'un broker"
  info "État initial du topic (leader, réplicas, ISR) :"
  describe_topic

  leader=$(kafka kafka-topics.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --topic "$TOPIC" \
    | awk '/Partition: 0/ {for (i = 1; i <= NF; i++) if ($i == "Leader:") print $(i+1)}')
  broker="kafka-$leader"
  group="archiver"

  step "Démarrage du producteur ($RATE évts/s) et de l'archiveur"
  "$PYTHON" producer/producer.py --rate "$RATE" --count $((RATE * 75)) > "$LOG_DIR/producer.log" 2>&1 &
  producer_pid=$!; PIDS+=("$producer_pid")
  "$PYTHON" consumer/archiver.py --group "$group" > "$LOG_DIR/archiver.log" 2>&1 &
  archiver_pid=$!; PIDS+=("$archiver_pid")
  sleep 10
  info "Offsets de fin avant la panne : $(end_offsets)"

  step "Arrêt de $broker (leader de la partition 0)"
  docker stop "$broker" > /dev/null
  STOPPED_BROKER="$broker"
  # Les outils Kafka ne contactent plus le broker arrêté (évite des warnings DNS)
  INTERNAL_BOOTSTRAP=$(echo "kafka-1:9092,kafka-2:9092,kafka-3:9092" | tr ',' '\n' | grep -v "^$broker:" | paste -sd, -)
  sleep 5
  info "Nouveaux leaders et ISR réduit à 2 répliques (>= min.insync.replicas=2, donc les écritures restent acceptées) :"
  describe_topic

  step "Le flux continue pendant ${OUTAGE_SECONDS}s avec un broker en moins"
  for _ in 1 2 3; do
    sleep $((OUTAGE_SECONDS / 3))
    info "Offsets de fin : $(end_offsets)"
  done
  info "Derniers messages confirmés par le producteur :"
  grep 'partition=' "$LOG_DIR/producer.log" | tail -n 3 | sed 's/^/    /'

  step "Redémarrage de $broker"
  docker start "$broker" > /dev/null
  wait_healthy "$broker"
  STOPPED_BROKER=""
  INTERNAL_BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
  info "$broker est healthy, attente de la resynchronisation (ISR = 3)…"
  wait_full_isr
  info "Retour des leaders préférés (répartition initiale) :"
  kafka kafka-leader-election.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --election-type PREFERRED --all-topic-partitions > /dev/null 2>&1 || true
  describe_topic

  step "Fin du producteur"
  wait "$producer_pid" || true
  grep -A 5 '^Bilan' "$LOG_DIR/producer.log" | sed 's/^/    /'

  step "Vérification : l'archiveur a tout consommé (LAG = 0)"
  for _ in $(seq 1 30); do
    lag=$(kafka kafka-consumer-groups.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --group "$group" 2>/dev/null \
      | awk -v t="$TOPIC" '$2 == t {sum += $6} END {print sum + 0}')
    [ "$lag" = "0" ] && break
    sleep 2
  done
  kafka kafka-consumer-groups.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --group "$group" 2>/dev/null \
    | awk 'NF {printf "    %-16s %-10s %-15s %-15s %s\n", $2, $3, $4, $5, $6}'
  kill "$archiver_pid" 2>/dev/null || true

  if [ "$lag" = "0" ] && grep -q ' 0 échecs, 0 sans réponse' "$LOG_DIR/producer.log"; then
    printf '\n\033[1;32m    OK : aucun message perdu malgré la panne de %s\033[0m\n' "$broker"
  else
    printf '\n\033[1;31m    ATTENTION : lag=%s, voir le bilan du producteur ci-dessus\033[0m\n' "$lag"
  fi
}

consumer_rebalance() {
  local group="monitoring-demo-$$" producer_pid m1 m2

  step "Partie 2 — Rééquilibrage du consumer group '$group'"
  "$PYTHON" producer/producer.py --rate "$RATE" --count $((RATE * 40)) --quiet > "$LOG_DIR/producer2.log" 2>&1 &
  producer_pid=$!; PIDS+=("$producer_pid")
  "$PYTHON" consumer/monitor.py --name monitor-1 --group "$group" --no-color > "$LOG_DIR/monitor-1.log" 2>&1 &
  m1=$!; PIDS+=("$m1")
  "$PYTHON" consumer/monitor.py --name monitor-2 --group "$group" --no-color > "$LOG_DIR/monitor-2.log" 2>&1 &
  m2=$!; PIDS+=("$m2")
  sleep 12

  info "Répartition des partitions entre les 2 consommateurs :"
  kafka kafka-consumer-groups.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --group "$group" --members --verbose 2>/dev/null \
    | awk 'NF && $4 != "CLIENT-ID" {print "    " $4 " -> partitions " $NF}'

  step "Arrêt brutal de monitor-2 (pas de départ propre : Kafka le détecte après session.timeout.ms = 10 s)"
  kill "$m2"
  sleep 15

  info "monitor-1 a repris toutes les partitions :"
  kafka kafka-consumer-groups.sh --bootstrap-server "$INTERNAL_BOOTSTRAP" --describe --group "$group" --members --verbose 2>/dev/null \
    | awk 'NF && $4 != "CLIENT-ID" {print "    " $4 " -> partitions " $NF}'
  info "Journal de monitor-1 :"
  grep 'REEQUILIBRAGE' "$LOG_DIR/monitor-1.log" | sed 's/^/    /'

  kill "$m1" "$producer_pid" 2>/dev/null || true
}

case "$PART" in
  all)      broker_failure; consumer_rebalance ;;
  broker)   broker_failure ;;
  consumer) consumer_rebalance ;;
  *)        echo "Usage : $0 [all|broker|consumer]" >&2; exit 1 ;;
esac

step "Démonstration terminée"
