SHELL := /bin/bash

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down -v
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test-battery:
	./scripts/run_test_battery.py
.PHONY: test-battery
