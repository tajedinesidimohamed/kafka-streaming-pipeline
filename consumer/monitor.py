"""Consommateur de supervision : affiche des alertes en temps réel.

Toutes les instances lancées avec le même --group forment un "consumer group" :
Kafka répartit les 3 partitions du topic entre elles (ex. 2 + 1 avec 2
instances). Si une instance s'arrête, ses partitions sont redonnées aux autres
(rééquilibrage), ce que ce script affiche.

Exemples (dans deux terminaux) :
    python consumer/monitor.py --name monitor-1
    python consumer/monitor.py --name monitor-2
"""

import argparse
import json
import os
import signal
import socket
import sys
import time

from confluent_kafka import Consumer, KafkaError

DEFAULT_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092"
)
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "network-events")

COLORS = {"critical": "\033[1;31m", "warning": "\033[33m", "info": "\033[36m", "reset": "\033[0m"}

ALERT_TEMPLATES = {
    "link_down":    "{value} interface(s) DOWN",
    "high_latency": "latence de {value} ms",
    "packet_loss":  "perte de paquets {value} %",
    "cpu_high":     "CPU à {value} %",
    "memory_high":  "mémoire à {value} %",
    "disk_full":    "disque plein à {value} %",
    "login_failed": "{value} échecs de connexion",
    "port_scan":    "scan de {value} ports détecté",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Consommateur de supervision (alertes temps réel)")
    parser.add_argument("--name", default=f"monitor-{socket.gethostname()}-{os.getpid()}",
                        help="nom de cette instance dans le groupe")
    parser.add_argument("--group", default="monitoring", help="consumer group (défaut : monitoring)")
    parser.add_argument("--show-warnings", action="store_true", help="affiche aussi les warnings")
    parser.add_argument("--from-beginning", action="store_true",
                        help="relit le topic depuis le début si le groupe n'a pas encore d'offset")
    parser.add_argument("--no-color", action="store_true", help="désactive les couleurs")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"brokers Kafka (défaut : {DEFAULT_BOOTSTRAP})")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"topic lu (défaut : {DEFAULT_TOPIC})")
    return parser.parse_args()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    if args.no_color:
        for key in COLORS:
            COLORS[key] = ""
    else:
        os.system("")  # active les codes couleur ANSI dans l'ancienne console Windows

    def log(text):
        print(f"[{time.strftime('%H:%M:%S')}] [{args.name}] {text}", flush=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "client.id": args.name,
        # Sans offset déjà enregistré pour le groupe : "latest" = seulement les
        # nouveaux événements (temps réel), "earliest" = tout l'historique
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        # Offsets enregistrés automatiquement toutes les 5 s : suffisant pour des alertes
        "enable.auto.commit": True,
        # Une instance qui ne donne plus signe de vie est exclue du groupe après 10 s
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })

    assigned = set()

    def on_assign(_consumer, partitions):
        assigned.update(p.partition for p in partitions)
        log(f"{COLORS['info']}REEQUILIBRAGE -> partitions attribuées : {sorted(assigned)}{COLORS['reset']}")

    def on_revoke(_consumer, partitions):
        assigned.difference_update(p.partition for p in partitions)
        log(f"{COLORS['info']}REEQUILIBRAGE -> partitions retirées : "
            f"{sorted(p.partition for p in partitions)}{COLORS['reset']}")

    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log(f"démarré dans le groupe '{args.group}', en attente des partitions…")
    total, alerts = 0, 0
    per_partition = {}
    last_report = time.monotonic()

    while running:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log(f"erreur Kafka : {msg.error()}")
                continue
            try:
                event = json.loads(msg.value())
            except (TypeError, ValueError):
                log(f"message illisible ignoré (partition {msg.partition()}, offset {msg.offset()})")
                continue

            total += 1
            per_partition[msg.partition()] = per_partition.get(msg.partition(), 0) + 1
            severity = event.get("severity")
            if severity == "critical" or (severity == "warning" and args.show_warnings):
                alerts += 1
                detail = ALERT_TEMPLATES.get(event.get("event_type"), "{value}").format(value=event.get("value"))
                log(f"{COLORS[severity]}ALERTE {severity.upper():<8}{COLORS['reset']} "
                    f"p{msg.partition()}/off{msg.offset():<6} "
                    f"{event.get('device', '?'):<16} {event.get('event_type', '?'):<13} {detail}")

        if time.monotonic() - last_report >= 10:
            last_report = time.monotonic()
            counts = ", ".join(f"p{p}={n}" for p, n in sorted(per_partition.items())) or "aucun"
            log(f"bilan : partitions {sorted(assigned)} | {total} événements lus ({counts}) | {alerts} alertes")

    log("arrêt : je quitte le groupe (mes partitions vont être redistribuées)")
    consumer.close()


if __name__ == "__main__":
    main()
