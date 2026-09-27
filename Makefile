COMPOSE := docker compose -f deploy/docker-compose.yml

# Backend whose direct/Dapr pair `make sweep` compares: nats, postgres or redis.
BACKEND ?= redis
export CONNECTORS ?= $(BACKEND)-direct,$(BACKEND)-dapr

.PHONY: up sweep sweep-all wait down logs build test plots clean

## up: build and start the stack; the bench sweeps $(CONNECTORS) and exits
up:
	$(COMPOSE) up --build -d
	# The sidecar shares the app container's network namespace, which is fixed at
	# creation time. Rebuilding the app replaces that container, so the sidecar
	# has to be recreated or it is left pointing at a namespace that is gone.
	$(COMPOSE) up -d --force-recreate --no-deps dapr
	@echo "bench metrics : http://localhost:9100/metrics"
	@echo "dapr metrics  : http://localhost:9090/metrics"
	@echo "prometheus    : http://localhost:9091"

## sweep: run one backend's direct-vs-Dapr sweep and follow it, e.g. make sweep BACKEND=nats
sweep: up
	$(COMPOSE) logs -f bench

## sweep-all: the three backends back to back, each one its own run and trace file
sweep-all:
	@for b in nats postgres redis; do \
	  $(MAKE) --no-print-directory sweep BACKEND=$$b; \
	  $(MAKE) --no-print-directory wait; \
	done

## wait: block until the current bench run has exited
wait:
	docker wait $$($(COMPOSE) ps -aq bench)

## down: stop everything and remove volumes
down:
	$(COMPOSE) down -v

## logs: follow the benchmark app's output
logs:
	$(COMPOSE) logs -f bench dapr

## build: compile and vet locally, no containers involved
build:
	go vet ./...
	go build ./...

## test: unit tests for the pacing, stall accounting and sweep planning
test:
	go test ./...

## plots: chart every trace, e.g. make plots FILES="traces/*.jsonl"
FILES ?= traces/*.jsonl
plots:
	@./scripts/plot_results.py $(FILES) -o plots

## clean: remove generated charts
clean:
	rm -rf plots
