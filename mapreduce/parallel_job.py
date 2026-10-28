"""Job MapReduce parallèle : MAP -> SHUFFLE -> REDUCE avec multiprocessing.

Reproduit en local ce que fait un framework comme Hadoop, sur plusieurs cœurs :

  1. SPLIT   : les lignes des archives sont découpées en blocs ("input splits")
  2. MAP     : chaque bloc est traité par un processus qui émet des paires (clé, 1)
  3. SHUFFLE : les paires sont regroupées par clé et chaque clé est envoyée à un
               reducer choisi par hash(clé) % nb_reducers (toutes les valeurs
               d'une même clé arrivent donc au même reducer)
  4. REDUCE  : chaque reducer additionne les valeurs de ses clés, en parallèle

Les fonctions map_line et reduce_key sont les mêmes que celles de mapper.py et
reducer.py : seule l'exécution change (parallèle au lieu de stdin/stdout).

Usage :
    python mapreduce/parallel_job.py
    python mapreduce/parallel_job.py --input "data/archive/*.jsonl" --workers 4 --split-size 1000
"""

import argparse
import glob
import os
import sys
import time
import zlib
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from pathlib import Path

from mapper import map_line
from reducer import format_report, reduce_key

DEFAULT_INPUT = str(Path(__file__).resolve().parent.parent / "data" / "archive" / "*.jsonl")


def map_task(task):
    """Tâche MAP exécutée dans un processus : un split -> liste de paires (clé, valeur)."""
    split_id, lines = task
    pairs = []
    for line in lines:
        pairs.extend(map_line(line))
    return split_id, os.getpid(), len(lines), pairs


def reduce_task(task):
    """Tâche REDUCE exécutée dans un processus : {clé: [valeurs]} -> {clé: total}."""
    reducer_id, grouped = task
    results = {key: reduce_key(key, values) for key, values in sorted(grouped.items())}
    return reducer_id, os.getpid(), results


def partition_for(key, reducers):
    # crc32 plutôt que hash() : hash() des chaînes change d'un processus à l'autre
    return zlib.crc32(key.encode("utf-8")) % reducers


def parse_args():
    parser = argparse.ArgumentParser(description="Job MapReduce parallèle (multiprocessing)")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="fichiers JSON Lines (motif glob)")
    parser.add_argument("--workers", type=int, default=min(4, cpu_count()), help="processus en parallèle")
    parser.add_argument("--reducers", type=int, default=None, help="nombre de reducers (défaut : --workers)")
    parser.add_argument("--split-size", type=int, default=500, help="lignes par split (défaut : 500)")
    parser.add_argument("--top", type=int, default=5, help="taille du top des équipements critiques")
    args = parser.parse_args()
    args.reducers = args.reducers or args.workers
    if args.workers < 1 or args.reducers < 1 or args.split_size < 1:
        parser.error("--workers, --reducers et --split-size doivent être >= 1")
    return args


def run_job(files, workers, reducers, split_size, log=print):
    """Exécute SPLIT -> MAP -> SHUFFLE -> REDUCE et renvoie {clé: total}."""
    # 1. SPLIT
    log("=== Phase 0 : SPLIT ===")
    lines = []
    for path in files:
        with open(path, encoding="utf-8") as handle:
            file_lines = handle.readlines()
        lines.extend(file_lines)
        log(f"  {os.path.basename(path)} : {len(file_lines)} lignes")
    splits = [(i, lines[start:start + split_size])
              for i, start in enumerate(range(0, len(lines), split_size))]
    log(f"  -> {len(lines)} lignes découpées en {len(splits)} splits de {split_size} lignes max")

    with Pool(processes=workers) as pool:
        # 2. MAP
        log(f"\n=== Phase 1 : MAP ({workers} processus) ===")
        t0 = time.perf_counter()
        map_outputs = []
        for split_id, pid, line_count, pairs in pool.imap_unordered(map_task, splits):
            log(f"  split {split_id:>3} traité par le processus {pid} : {line_count} lignes -> {len(pairs)} paires")
            map_outputs.append(pairs)
        total_pairs = sum(len(pairs) for pairs in map_outputs)
        log(f"  -> {total_pairs} paires (clé, 1) émises en {time.perf_counter() - t0:.2f}s")

        # 3. SHUFFLE : regroupement par clé et répartition entre les reducers
        log(f"\n=== Phase 2 : SHUFFLE (vers {reducers} reducers) ===")
        t0 = time.perf_counter()
        partitions = [defaultdict(list) for _ in range(reducers)]
        for pairs in map_outputs:
            for key, value in pairs:
                partitions[partition_for(key, reducers)][key].append(value)
        for reducer_id, grouped in enumerate(partitions):
            values = sum(len(v) for v in grouped.values())
            log(f"  reducer {reducer_id} reçoit {len(grouped):>3} clés ({values} valeurs)")
        log(f"  -> {sum(len(p) for p in partitions)} clés distinctes regroupées en {time.perf_counter() - t0:.2f}s")

        # 4. REDUCE
        log(f"\n=== Phase 3 : REDUCE ({reducers} reducers) ===")
        t0 = time.perf_counter()
        results = {}
        for reducer_id, pid, partial in pool.imap_unordered(reduce_task, enumerate(partitions)):
            log(f"  reducer {reducer_id} (processus {pid}) : {len(partial)} résultats")
            results.update(partial)
        log(f"  -> {len(results)} résultats en {time.perf_counter() - t0:.2f}s")
    return results


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    started = time.perf_counter()

    files = sorted(glob.glob(args.input))
    if not files:
        print(f"Aucun fichier trouvé pour '{args.input}'. Lance d'abord l'archiveur (consumer/archiver.py).")
        return 1

    results = run_job(files, args.workers, args.reducers, args.split_size)

    print("\n=== Résultats ===")
    print(format_report(results, top=args.top))
    print(f"\nJob terminé en {time.perf_counter() - started:.2f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
