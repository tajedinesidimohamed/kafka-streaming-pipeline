"""Phase REDUCE : additionne les valeurs de chaque clé.

Format Hadoop Streaming : lit "clé<TAB>valeur" sur stdin, TRIÉ par clé (c'est le
rôle de `sort` entre le mapper et le reducer, qui joue la phase SHUFFLE), et
écrit "clé<TAB>total" sur stdout.

Comme l'entrée est triée, toutes les lignes d'une même clé se suivent : il
suffit de garder la clé courante et son total, et d'écrire le résultat quand la
clé change. Le reducer n'a donc jamais besoin de tout garder en mémoire.

Usage :
    cat data/archive/*.jsonl | python mapreduce/mapper.py | sort | python mapreduce/reducer.py
    ... | sort | python mapreduce/reducer.py --pretty     # rapport lisible + top 5
"""

import argparse
import sys


def reduce_key(_key, values):
    """Fonction de réduction : ici une simple somme des compteurs."""
    return sum(values)


def reduce_sorted(lines):
    """Parcourt des lignes "clé<TAB>valeur" triées et renvoie les paires (clé, total)."""
    current_key, current_values = None, []
    for line in lines:
        line = line.rstrip("\r\n")
        if not line:
            continue
        key, _, value = line.rpartition("\t")
        try:
            value = int(value)
        except ValueError:
            continue
        if key != current_key:
            if current_key is not None:
                yield current_key, reduce_key(current_key, current_values)
            current_key, current_values = key, []
        current_values.append(value)
    if current_key is not None:
        yield current_key, reduce_key(current_key, current_values)


def format_report(results, top=5):
    """Met en forme les totaux (clé -> total) : par type, par équipement, top N critiques."""
    groups = {"type": {}, "device": {}, "critical": {}}
    for key, total in results.items():
        prefix, _, name = key.partition(":")
        if prefix in groups:
            groups[prefix][name] = total

    def table(title, counts, limit=None):
        rows = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
        if not rows:
            return [f"\n{title}", "  (aucune donnée)"]
        width = max(len(name) for name, _ in rows)
        biggest = rows[0][1]
        lines = [f"\n{title}"]
        for rank, (name, count) in enumerate(rows, start=1):
            bar = "█" * max(1, round(30 * count / biggest))
            prefix = f"{rank}. " if limit else ""
            lines.append(f"  {prefix}{name:<{width}}  {count:>7}  {bar}")
        return lines

    total_events = sum(groups["type"].values())
    total_critical = sum(groups["critical"].values())
    lines = [f"Événements analysés : {total_events}  (dont {total_critical} critiques)"]
    lines += table("Événements par type", groups["type"])
    lines += table("Événements par équipement", groups["device"])
    lines += table(f"Top {top} des équipements les plus critiques", groups["critical"], limit=top)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Reducer MapReduce (Hadoop Streaming)")
    parser.add_argument("--pretty", action="store_true", help="affiche un rapport lisible au lieu de clé<TAB>total")
    args = parser.parse_args()

    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    results = reduce_sorted(sys.stdin)
    if args.pretty:
        print(format_report(dict(results)))
    else:
        for key, total in results:
            sys.stdout.write(f"{key}\t{total}\n")


if __name__ == "__main__":
    main()
