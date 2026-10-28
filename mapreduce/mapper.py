"""Phase MAP : transforme chaque événement en paires (clé, valeur).

Format Hadoop Streaming : lit des lignes sur stdin, écrit "clé<TAB>valeur" sur stdout.

Pour chaque événement JSON, émet :
    type:<event_type>      1   -> nombre d'événements par type
    device:<équipement>    1   -> nombre d'événements par équipement
    critical:<équipement>  1   -> nombre d'événements critiques (pour le top 5)

Usage :
    cat data/archive/*.jsonl | python mapreduce/mapper.py | sort | python mapreduce/reducer.py
"""

import json
import sys


def map_line(line):
    """Renvoie la liste des paires (clé, 1) d'une ligne JSON, [] si la ligne est invalide."""
    line = line.strip()
    if not line:
        return []
    try:
        event = json.loads(line)
        event_type, device = event["event_type"], event["device"]
    except (ValueError, KeyError, TypeError):
        return []

    pairs = [(f"type:{event_type}", 1), (f"device:{device}", 1)]
    if event.get("severity") == "critical":
        pairs.append((f"critical:{device}", 1))
    return pairs


def main():
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    invalid = 0
    for line in sys.stdin:
        pairs = map_line(line)
        if not pairs and line.strip():
            invalid += 1
        for key, value in pairs:
            sys.stdout.write(f"{key}\t{value}\n")
    if invalid:
        # Compteur au format Hadoop Streaming (lu par Hadoop, simple message sinon)
        print(f"reporter:counter:mapper,invalid_lines,{invalid}", file=sys.stderr)


if __name__ == "__main__":
    main()
