"""Producteur d'événements de supervision réseau simulés.

Simule un parc d'équipements (routeurs, serveurs, pare-feu) qui envoient des
événements JSON dans le topic Kafka "network-events".

La clé de chaque message est le nom de l'équipement : Kafka calcule
hash(clé) % nb_partitions, donc tous les événements d'un même équipement vont
dans la même partition et restent dans l'ordre.

Exemples :
    python producer/producer.py                      # 5 évts/s, 12 équipements, sans fin
    python producer/producer.py --rate 20 --devices 30
    python producer/producer.py --count 500          # s'arrête après 500 événements
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

DEFAULT_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092"
)
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "network-events")

# Préfixes des noms d'équipements et sites fictifs
DEVICE_TYPES = {"router": "rtr", "server": "srv", "firewall": "fw"}
SITES = ["paris", "rennes", "lyon", "lille"]

# Catalogue d'événements : types d'équipements concernés, unité, plage de valeurs,
# seuils warning et critical. Plus la valeur est haute, plus c'est grave.
EVENT_CATALOG = {
    "link_down":     {"devices": ["router", "firewall"], "unit": "interfaces", "range": (1, 3),     "warning": 1,    "critical": 2},
    "high_latency":  {"devices": ["router"],             "unit": "ms",         "range": (20, 400),  "warning": 100,  "critical": 250},
    "packet_loss":   {"devices": ["router"],             "unit": "%",          "range": (0.1, 30),  "warning": 2,    "critical": 10},
    "cpu_high":      {"devices": ["router", "server", "firewall"], "unit": "%", "range": (60, 100), "warning": 80,   "critical": 95},
    "memory_high":   {"devices": ["server"],             "unit": "%",          "range": (60, 100),  "warning": 85,   "critical": 95},
    "disk_full":     {"devices": ["server"],             "unit": "%",          "range": (70, 100),  "warning": 90,   "critical": 98},
    "login_failed":  {"devices": ["server", "firewall"], "unit": "attempts",   "range": (1, 50),    "warning": 5,    "critical": 20},
    "port_scan":     {"devices": ["firewall"],           "unit": "ports",      "range": (10, 2000), "warning": 100,  "critical": 1000},
}


def build_devices(count, rng):
    """Crée la liste des équipements, ex. rtr-paris-01, srv-rennes-02, fw-lyon-01.

    Chaque équipement reçoit une "fragilité" : les équipements fragiles émettent
    plus d'événements et des valeurs plus graves, ce qui rend le top 5 des
    équipements critiques (phase MapReduce) intéressant.
    """
    devices = []
    types = list(DEVICE_TYPES)
    for i in range(count):
        dtype = types[i % len(types)]
        site = SITES[(i // len(types)) % len(SITES)]
        number = i // (len(types) * len(SITES)) + 1
        devices.append({
            "name": f"{DEVICE_TYPES[dtype]}-{site}-{number:02d}",
            "type": dtype,
            "fragility": rng.choice([0.1, 0.2, 0.3, 0.5, 1.5]),
        })
    return devices


def severity_for(event_type, value):
    spec = EVENT_CATALOG[event_type]
    if value >= spec["critical"]:
        return "critical"
    if value >= spec["warning"]:
        return "warning"
    return "info"


def generate_event(devices, rng):
    weights = [d["fragility"] for d in devices]
    device = rng.choices(devices, weights=weights, k=1)[0]
    event_type = rng.choice(
        [e for e, spec in EVENT_CATALOG.items() if device["type"] in spec["devices"]]
    )
    low, high = EVENT_CATALOG[event_type]["range"]
    # Valeurs plutôt basses (situation normale) ; plus l'équipement est fragile,
    # plus il atteint souvent les seuils warning/critical (~20 % de critiques au total)
    position = rng.random() ** (4 / (0.5 + device["fragility"]))
    value = low + (high - low) * position
    value = round(value) if isinstance(low, int) else round(value, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "device": device["name"],
        "device_type": device["type"],
        "event_type": event_type,
        "severity": severity_for(event_type, value),
        "value": value,
        "unit": EVENT_CATALOG[event_type]["unit"],
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Producteur d'événements réseau simulés")
    parser.add_argument("--rate", type=float, default=5, help="événements par seconde (défaut : 5)")
    parser.add_argument("--devices", type=int, default=12, help="nombre d'équipements simulés (défaut : 12)")
    parser.add_argument("--count", type=int, default=0, help="nombre d'événements à envoyer, 0 = sans fin (défaut : 0)")
    parser.add_argument("--seed", type=int, default=None, help="graine aléatoire pour rejouer la même simulation")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"brokers Kafka (défaut : {DEFAULT_BOOTSTRAP})")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"topic cible (défaut : {DEFAULT_TOPIC})")
    parser.add_argument("--quiet", action="store_true", help="n'affiche que le bilan final")
    args = parser.parse_args()
    if args.rate <= 0 or args.devices <= 0 or args.count < 0:
        parser.error("--rate et --devices doivent être > 0, --count >= 0")
    return args


def main():
    # Accents corrects même quand la sortie est redirigée (console Windows en cp1252)
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    args = parse_args()
    rng = random.Random(args.seed)
    devices = build_devices(args.devices, rng)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "network-events-producer",
        # acks=all : le leader attend que toutes les répliques de l'ISR aient
        # écrit le message (au moins min.insync.replicas=2) avant de confirmer.
        "acks": "all",
        # Idempotence : pas de doublon ni de désordre en cas de renvoi automatique
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "lz4",
    })

    stats = {"sent": 0, "delivered": 0, "failed": 0}
    per_partition = Counter()

    def on_delivery(err, msg):
        """Appelé par Kafka quand le broker a confirmé (ou refusé) le message."""
        if err is not None:
            stats["failed"] += 1
            print(f"  ECHEC  clé={msg.key().decode()}  erreur={err}", file=sys.stderr)
            return
        stats["delivered"] += 1
        per_partition[msg.partition()] += 1
        if not args.quiet:
            event = json.loads(msg.value())
            print(
                f"  partition={msg.partition()} offset={msg.offset():<6} "
                f"{event['device']:<16} {event['event_type']:<13} "
                f"{event['severity']:<8} {event['value']} {event['unit']}"
            )

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(
        f"Envoi vers '{args.topic}' ({args.bootstrap}) : {args.rate} évts/s, "
        f"{len(devices)} équipements, "
        f"{'sans fin (Ctrl+C pour arrêter)' if args.count == 0 else f'{args.count} événements'}"
    )

    interval = 1 / args.rate
    next_send = time.monotonic()
    while running and (args.count == 0 or stats["sent"] < args.count):
        event = generate_event(devices, rng)
        try:
            producer.produce(
                args.topic,
                key=event["device"],
                value=json.dumps(event),
                on_delivery=on_delivery,
            )
            stats["sent"] += 1
        except BufferError:
            # File d'attente locale pleine (brokers injoignables) : on attend un peu
            producer.poll(1)
            continue
        except KafkaException as exc:
            print(f"Erreur Kafka : {exc}", file=sys.stderr)
            break

        # poll() déclenche les callbacks de livraison des messages déjà confirmés
        producer.poll(0)
        next_send += interval
        time.sleep(max(0.0, next_send - time.monotonic()))

    print("\nVidage de la file d'attente…")
    remaining = producer.flush(15)

    print(f"\nBilan : {stats['sent']} envoyés, {stats['delivered']} confirmés, "
          f"{stats['failed']} échecs, {remaining} sans réponse")
    for partition in sorted(per_partition):
        print(f"  partition {partition} : {per_partition[partition]} messages")
    return 0 if stats["failed"] == 0 and remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
