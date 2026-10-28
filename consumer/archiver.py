"""Archiveur : enregistre tous les événements dans data/archive/*.jsonl.

Il a son propre consumer group ("archiver") : il reçoit donc TOUS les messages,
indépendamment du groupe "monitoring". Un fichier JSON Lines (un événement JSON
par ligne) est créé par heure, d'après l'horodatage de l'événement :
    data/archive/events-20261029-14.jsonl

Garantie "at-least-once" : l'offset n'est enregistré (commit) qu'APRÈS
l'écriture sur disque. En cas de crash, des messages peuvent être réécrits
(doublons possibles), mais aucun n'est perdu.

Exemple :
    python consumer/archiver.py
"""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

DEFAULT_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092"
)
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "network-events")
DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent / "data" / "archive"


def parse_args():
    parser = argparse.ArgumentParser(description="Archive les événements Kafka en fichiers JSON Lines")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT,
                        help=f"dossier des archives (défaut : {DEFAULT_OUTPUT})")
    parser.add_argument("--group", default="archiver", help="consumer group (défaut : archiver)")
    parser.add_argument("--commit-every", type=int, default=100,
                        help="commit des offsets tous les N messages (défaut : 100, et au moins toutes les 5 s)")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"brokers Kafka (défaut : {DEFAULT_BOOTSTRAP})")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"topic lu (défaut : {DEFAULT_TOPIC})")
    return parser.parse_args()


def archive_name(event):
    """events-YYYYMMDD-HH.jsonl à partir de "2026-10-29T14:03:12.123+00:00"."""
    ts = event.get("timestamp", "")
    if len(ts) >= 13 and ts[4] == "-" and ts[10] == "T":
        return f"events-{ts[0:4]}{ts[5:7]}{ts[8:10]}-{ts[11:13]}.jsonl"
    return "events-unknown.jsonl"


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    def log(text):
        print(f"[{time.strftime('%H:%M:%S')}] [archiver] {text}", flush=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        # Première exécution : on archive tout l'historique du topic
        "auto.offset.reset": "earliest",
        # Commit manuel, uniquement après écriture sur disque
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })

    files = {}          # nom de fichier -> fichier ouvert en ajout
    pending = 0         # messages écrits mais pas encore commités
    last_commit = time.monotonic()

    def flush_and_commit():
        """Écrit les données sur disque puis enregistre les offsets dans Kafka."""
        nonlocal pending, last_commit
        last_commit = time.monotonic()
        if pending == 0:
            return
        for handle in files.values():
            handle.flush()
            os.fsync(handle.fileno())
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            log(f"commit impossible ({exc}) : ces messages seront relus (doublons possibles)")
        pending = 0

    def on_assign(_consumer, partitions):
        log(f"partitions attribuées : {sorted(p.partition for p in partitions)}")

    def on_revoke(_consumer, partitions):
        # Avant de perdre des partitions, on sauvegarde ce qui a été écrit
        flush_and_commit()
        log(f"partitions retirées : {sorted(p.partition for p in partitions)}")

    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log(f"archivage de '{args.topic}' vers {args.output_dir}")
    total, skipped = 0, 0
    last_report = time.monotonic()

    try:
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
                    skipped += 1
                    continue

                name = archive_name(event)
                if name not in files:
                    files[name] = open(args.output_dir / name, "a", encoding="utf-8")
                    log(f"nouveau fichier : {name}")
                files[name].write(json.dumps(event, ensure_ascii=False) + "\n")
                total += 1
                pending += 1

            if pending >= args.commit_every or time.monotonic() - last_commit >= 5:
                flush_and_commit()

            if time.monotonic() - last_report >= 10:
                last_report = time.monotonic()
                log(f"{total} événements archivés ({skipped} ignorés) dans {len(files)} fichier(s)")
    finally:
        flush_and_commit()
        for handle in files.values():
            handle.close()
        consumer.close()
        log(f"arrêt : {total} événements archivés au total")


if __name__ == "__main__":
    main()
