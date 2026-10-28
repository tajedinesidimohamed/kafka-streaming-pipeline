# Explications : les concepts du projet

Ce document explique simplement les notions utilisées dans le projet, avec des exemples tirés du code. Il se termine par 10 questions d'entretien et leurs réponses.

## Sommaire

1. [Topic](#1-topic)
2. [Partition](#2-partition)
3. [Offset](#3-offset)
4. [Broker](#4-broker)
5. [Réplication](#5-réplication)
6. [ISR (In-Sync Replicas)](#6-isr-in-sync-replicas)
7. [Consumer group](#7-consumer-group)
8. [KRaft](#8-kraft)
9. [Streaming et batch](#9-streaming-et-batch)
10. [Les 3 phases de MapReduce](#10-les-3-phases-de-mapreduce)
11. [10 questions d'entretien](#11-10-questions-dentretien)

---

## 1. Topic

Un **topic** est une catégorie de messages, un peu comme une table dans une base de données ou un dossier de boîte mail. Les producteurs écrivent dans un topic et les consommateurs y lisent.

Dans le projet, il n'y a qu'un topic : `network-events`. Il contient tous les événements de supervision.

Contrairement à une file d'attente classique, **un message lu n'est pas supprimé**. Kafka le conserve pendant une durée configurable (7 jours par défaut), ce qui permet à plusieurs applications de lire les mêmes données, ou à une application de les relire.

## 2. Partition

Un topic est découpé en **partitions**. Chaque partition est un **journal (log) ordonné** dans lequel les messages sont seulement ajoutés à la fin.

```text
network-events
  partition 0 : [m0][m1][m2][m3][m4] ...  ->
  partition 1 : [m0][m1][m2] ...          ->
  partition 2 : [m0][m1][m2][m3] ...      ->
```

Les partitions servent à :

- **répartir la charge** : chaque partition peut être sur un broker différent ;
- **paralléliser la lecture** : chaque partition est lue par un seul consommateur d'un groupe à la fois.

**Le choix de la partition** : si le message a une **clé**, Kafka calcule `hash(clé) % nombre_de_partitions`. Une même clé va donc toujours dans la même partition. Dans le projet, la clé est le nom de l'équipement (`rtr-paris-01`), si bien que **tous les événements d'un équipement restent dans l'ordre**.

> L'ordre est garanti **à l'intérieur d'une partition**, jamais entre partitions.

## 3. Offset

L'**offset** est le numéro de position d'un message dans sa partition (0, 1, 2…). Le couple *(partition, offset)* identifie un message de manière unique.

Chaque consumer group **enregistre (commite) l'offset** jusqu'où il a lu, dans le topic interne `__consumer_offsets`. Après un redémarrage, il reprend là où il s'était arrêté.

Dans le projet :

- `monitor.py` commite automatiquement toutes les 5 secondes (`enable.auto.commit=True`) ;
- `archiver.py` commite **manuellement**, et seulement **après** avoir écrit les événements sur disque. S'il plante, il relit les messages non commités : aucune perte, quelques doublons possibles. C'est le mode **at-least-once**.

Le **lag** est l'écart entre le dernier offset écrit dans la partition et le dernier offset commité par le groupe. Un lag de 0 veut dire que le consommateur est à jour.

## 4. Broker

Un **broker** est un serveur Kafka. Il stocke les partitions sur disque et répond aux producteurs et aux consommateurs. Un ensemble de brokers forme un **cluster**.

Dans le projet, il y a 3 brokers (`kafka-1`, `kafka-2`, `kafka-3`), chacun dans un conteneur Docker. Pour chaque partition, un broker est le **leader** : il reçoit toutes les lectures et écritures de cette partition. Les autres brokers sont des **followers** qui copient les données du leader.

## 5. Réplication

La **réplication** consiste à copier chaque partition sur plusieurs brokers. Avec un **facteur de réplication de 3**, chaque partition existe en 3 exemplaires, un par broker :

```text
Partition: 0  Leader: 3  Replicas: 3,1,2
Partition: 1  Leader: 1  Replicas: 1,2,3
Partition: 2  Leader: 2  Replicas: 2,3,1
```

Si un broker tombe, ses partitions ne sont pas perdues : une autre réplique devient leader. C'est ce que montre `make failover`.

## 6. ISR (In-Sync Replicas)

L'**ISR** est la liste des répliques **à jour** avec le leader. Une réplique qui prend trop de retard, ou dont le broker est arrêté, sort de l'ISR. Elle y revient une fois qu'elle a rattrapé son retard.

Trois réglages travaillent ensemble pour garantir la durabilité :

| Réglage | Où | Valeur | Effet |
|---|---|---|---|
| `replication.factor` | topic | 3 | 3 copies de chaque partition |
| `min.insync.replicas` | topic | 2 | une écriture n'est acceptée que si au moins 2 répliques sont dans l'ISR |
| `acks` | producteur | `all` | le leader attend que toutes les répliques de l'ISR aient écrit avant de confirmer |

Résultat : un message confirmé existe **sur au moins 2 brokers**. Avec un broker arrêté, l'ISR passe à 2 et les écritures continuent. Avec 2 brokers arrêtés, l'ISR passe à 1 et Kafka **refuse les écritures** (`NOT_ENOUGH_REPLICAS`) plutôt que de risquer une perte de données.

## 7. Consumer group

Un **consumer group** est un ensemble de consommateurs qui partagent le même `group.id` et **se répartissent les partitions** d'un topic :

```text
Groupe "monitoring" (2 instances)          Groupe "archiver" (1 instance)
  monitor-1 <- partitions 0, 1               archiver <- partitions 0, 1, 2
  monitor-2 <- partition 2
```

Les règles à retenir :

- une partition est lue par **un seul** consommateur du groupe à la fois ;
- au-delà de 3 consommateurs pour 3 partitions, les consommateurs en trop restent **inactifs** ;
- **des groupes différents sont indépendants** : `monitoring` et `archiver` reçoivent chacun **tous** les messages.

Le **rééquilibrage (rebalance)** est la redistribution des partitions quand un consommateur rejoint ou quitte le groupe. Si un consommateur plante sans prévenir, il est exclu du groupe après `session.timeout.ms` (10 s dans le projet) faute de heartbeat.

## 8. KRaft

Un cluster Kafka a besoin de **métadonnées** : liste des brokers, des topics, leader de chaque partition, ISR… Historiquement, elles étaient stockées dans **ZooKeeper**, un système distribué séparé à installer et surveiller.

**KRaft** (*Kafka Raft*) remplace ZooKeeper. Il est disponible en production depuis Kafka 3.3, et ZooKeeper est supprimé dans Kafka 4.0. Certains nœuds jouent le rôle de **controller** et forment un **quorum** qui se met d'accord sur les métadonnées grâce à l'algorithme de consensus **Raft** :

- un controller est élu **leader du quorum** ;
- chaque changement est écrit dans un journal répliqué (`__cluster_metadata`) ;
- une décision est validée quand une **majorité** de controllers l'a enregistrée.

Dans le projet, les 3 nœuds sont à la fois broker et controller (`KAFKA_PROCESS_ROLES=broker,controller`). Avec 3 controllers, la majorité est de 2 : **le cluster tolère la perte d'un nœud**.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
```

## 9. Streaming et batch

| | Streaming (temps réel) | Batch (par lots) |
|---|---|---|
| Données | flux continu, sans fin | jeu de données fini, déjà stocké |
| Latence | millisecondes à secondes | minutes à heures |
| Traitement | événement par événement, au fil de l'eau | tout le lot d'un coup |
| Usage | alertes, détection de fraude, supervision | rapports, statistiques, entraînement de modèles |
| Dans le projet | `monitor.py` : alerte dès qu'un `link_down` arrive | `parallel_job.py` : statistiques sur toutes les archives |

Les deux approches sont complémentaires. Cette combinaison d'une couche temps réel et d'une couche batch sur les mêmes données correspond à l'**architecture Lambda**.

## 10. Les 3 phases de MapReduce

MapReduce est un modèle de calcul distribué, popularisé par Google puis Hadoop. Il découpe un gros calcul en tâches indépendantes exécutables sur de nombreuses machines.

Exemple avec 3 événements :

```text
Entrée                                   MAP                        SHUFFLE (tri + regroupement)       REDUCE
{"device":"rtr-1","event_type":"cpu_high"}  -> type:cpu_high 1        type:cpu_high  -> [1, 1]      ->  type:cpu_high  2
                                            -> device:rtr-1  1        device:rtr-1   -> [1, 1]      ->  device:rtr-1   2
{"device":"rtr-1","event_type":"link_down"} -> type:link_down 1       device:srv-2   -> [1]         ->  device:srv-2   1
                                            -> device:rtr-1  1        type:link_down -> [1]         ->  type:link_down 1
{"device":"srv-2","event_type":"cpu_high"}  -> type:cpu_high 1
                                            -> device:srv-2  1
```

1. **MAP** (`mapper.py`) : chaque ligne est transformée **indépendamment** en paires *(clé, valeur)*. Comme aucune ligne ne dépend d'une autre, on peut lancer autant de mappers que de blocs de données.
2. **SHUFFLE** : les paires sont **triées et regroupées par clé**, puis chaque clé est envoyée à un reducer, choisi par `hash(clé) % nb_reducers`. Toutes les valeurs d'une même clé arrivent ainsi **au même reducer**. Dans la commande `mapper | sort | reducer`, c'est `sort` qui fait le shuffle.
3. **REDUCE** (`reducer.py`) : pour chaque clé, les valeurs sont combinées. Ici, on fait une somme.

`parallel_job.py` exécute ces phases sur plusieurs processus et ajoute l'étape **SPLIT** : le découpage de l'entrée en blocs, l'équivalent des blocs HDFS.

---

## 11. 10 questions d'entretien

**1. Pourquoi Kafka utilise-t-il des partitions ?**
Pour la **scalabilité** : les partitions d'un topic sont réparties sur plusieurs brokers, ce qui répartit le stockage et la charge. Elles permettent aussi le **parallélisme**, puisque chaque partition peut être lue par un consommateur différent d'un même groupe.

**2. Comment garantir l'ordre des messages dans Kafka ?**
L'ordre n'est garanti qu'à l'intérieur d'une partition. On donne donc la **même clé** aux messages qui doivent rester ordonnés : ici, le nom de l'équipement. Ils vont alors dans la même partition. Côté producteur, l'idempotence évite qu'un renvoi automatique change l'ordre.

**3. Que se passe-t-il quand un broker tombe ?**
Pour chaque partition dont il était leader, le controller élit un **nouveau leader parmi les répliques de l'ISR**. Les clients récupèrent les nouvelles métadonnées et continuent. Au redémarrage, le broker rattrape son retard puis réintègre l'ISR. Dans le projet : 750 messages envoyés pendant la panne, 750 confirmés, 0 perdu.

**4. À quoi sert `min.insync.replicas` ?**
C'est le nombre minimum de répliques à jour pour qu'une écriture `acks=all` soit acceptée. Avec un facteur de réplication de 3 et `min.insync.replicas=2`, on tolère la perte d'un broker sans perte de données. Si trop de brokers tombent, Kafka préfère refuser les écritures plutôt que de perdre des messages.

**5. Quelle est la différence entre `acks=0`, `acks=1` et `acks=all` ?**
- `acks=0` : le producteur n'attend aucune confirmation. C'est le plus rapide, mais des pertes sont possibles.
- `acks=1` : seul le leader confirme. Si le leader tombe avant la réplication, le message est perdu.
- `acks=all` : toutes les répliques de l'ISR confirment. C'est le plus sûr, avec un peu plus de latence.

**6. Qu'est-ce qu'un consumer group et que se passe-t-il s'il y a plus de consommateurs que de partitions ?**
C'est un ensemble de consommateurs qui se partagent les partitions d'un topic, chaque partition étant lue par un seul membre. S'il y a plus de consommateurs que de partitions, les consommateurs en trop restent **inactifs** : le nombre de partitions fixe le parallélisme maximal.

**7. Qu'est-ce qu'un rééquilibrage et quand se produit-il ?**
C'est la redistribution des partitions entre les membres d'un groupe. Il se produit quand un consommateur rejoint ou quitte le groupe, quand il ne répond plus (pas de heartbeat pendant `session.timeout.ms`), ou quand le nombre de partitions change. Pendant un rééquilibrage, la consommation est brièvement interrompue.

**8. Quelles sont les sémantiques de livraison possibles ?**
- **At-most-once** : on commite l'offset avant le traitement. Pas de doublon, mais des pertes possibles.
- **At-least-once** : on commite après le traitement. Pas de perte, mais des doublons possibles. C'est le choix de l'archiveur du projet.
- **Exactly-once** : producteur idempotent et transactions Kafka, ou traitement idempotent côté consommateur (dédoublonnage par `event_id`).

**9. Pourquoi KRaft remplace-t-il ZooKeeper ?**
Il n'y a plus qu'un seul système à déployer, sécuriser et surveiller. Les métadonnées sont stockées dans un journal Kafka répliqué par Raft, ce qui accélère le démarrage et les élections de controller et permet de gérer beaucoup plus de partitions. ZooKeeper est supprimé dans Kafka 4.0.

**10. Expliquez MapReduce et le rôle du shuffle.**
MapReduce découpe un calcul en trois phases. **Map** transforme chaque enregistrement en paires *(clé, valeur)*, en parallèle. **Shuffle** trie et regroupe les paires par clé, puis envoie chaque clé à un reducer unique. **Reduce** combine les valeurs de chaque clé. Le shuffle est essentiel parce qu'il garantit que toutes les valeurs d'une même clé sont traitées ensemble. C'est aussi l'étape la plus coûteuse, car elle déplace les données sur le réseau.
