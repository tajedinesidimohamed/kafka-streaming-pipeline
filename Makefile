# Commandes du projet. Sous Windows, lancer `make` depuis Git Bash.
SHELL := bash

ifeq ($(OS),Windows_NT)
VENV_PY := .venv/Scripts/python.exe
else
VENV_PY := .venv/bin/python
endif
# Python de l'environnement virtuel s'il existe, sinon celui du système
PYTHON ?= $(if $(wildcard $(VENV_PY)),$(VENV_PY),python)

RATE ?= 5
DEVICES ?= 12

.DEFAULT_GOAL := help
.PHONY: help install up topic produce consume monitor archive failover mapreduce mapreduce-streaming test down clean

help: ## Affiche cette aide
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-20s %s\n", $$1, $$2}'

install: ## Crée l'environnement virtuel .venv et installe les dépendances
	python -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt

up: ## Démarre le cluster Kafka (3 brokers) et kafka-ui
	docker compose up -d --wait
	@echo "Interface web : http://localhost:8080"

topic: ## Crée le topic network-events (3 partitions, réplication 3)
	bash scripts/create-topic.sh

produce: ## Lance le producteur (options : RATE=5 DEVICES=12)
	$(PYTHON) producer/producer.py --rate $(RATE) --devices $(DEVICES)

consume: ## Lance 2 consommateurs "monitoring" + l'archiveur (Ctrl+C pour tout arrêter)
	@trap 'kill $$(jobs -p) 2>/dev/null' INT TERM EXIT; \
	$(PYTHON) consumer/monitor.py --name monitor-1 & \
	$(PYTHON) consumer/monitor.py --name monitor-2 & \
	$(PYTHON) consumer/archiver.py & \
	wait

monitor: ## Lance un seul consommateur "monitoring"
	$(PYTHON) consumer/monitor.py

archive: ## Lance uniquement l'archiveur
	$(PYTHON) consumer/archiver.py

failover: ## Démo de tolérance aux pannes (broker arrêté + rééquilibrage)
	bash scripts/test-broker-failure.sh

mapreduce: ## Job MapReduce parallèle sur data/archive/
	$(PYTHON) mapreduce/parallel_job.py

mapreduce-streaming: ## MapReduce style Hadoop Streaming : mapper | sort | reducer
	cat data/archive/*.jsonl | $(PYTHON) mapreduce/mapper.py | LC_ALL=C sort | $(PYTHON) mapreduce/reducer.py --pretty

test: ## Lance les tests du MapReduce
	$(PYTHON) -m unittest discover -s mapreduce -v

down: ## Arrête le cluster (les messages Kafka sont supprimés, pas data/archive/)
	docker compose down

clean: down ## Arrête le cluster et supprime les archives locales
	rm -rf data/
