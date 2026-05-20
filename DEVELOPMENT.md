# Development Guide

This guide explains how to run the GlassFlow ClickHouse ETL project locally and in Kubernetes.

## Table of Contents

- [Architecture overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Local development](#local-development)
  - [Backend (glassflow-api)](#backend-glassflow-api)
  - [Frontend (ui)](#frontend-ui)
- [Running tests](#running-tests)
- [Demo scenarios](#demo-scenarios)
  - [Fraud detection demo](#fraud-detection-demo)
  - [Observability demo (OTLP)](#observability-demo-otlp)

---

## Architecture overview

The same Go binary (`glassflow`) runs as multiple services via the `-role` flag:

```
Source → Ingestor → NATS JetStream → [Dedup] → [Join] → [Transform/Filter] → Sink → ClickHouse
```

| Role | Responsibility |
|------|---------------|
| `ingestor` | Kafka consumer; publishes to NATS JetStream |
| `dedup` | Stateful deduplication (BadgerDB) |
| `join` | Temporal stream joining |
| `sink` | Batches events and writes to ClickHouse |
| `api` (default) | REST API + pipeline management (port 8081) |

The UI (Next.js) runs separately and talks to the API at port 8081. Nginx proxies both on port 8080.

---

## Prerequisites

### Required for backend

| Tool | Version | Install |
|------|---------|---------|
| Go | 1.25+ | https://go.dev/dl/ |
| NATS Server | 2.x | https://nats.io/download/ |

### Required for frontend

| Tool | Version | Install |
|------|---------|---------|
| Node.js | 20+ | https://nodejs.org/ |
| pnpm | 8+ | `npm install -g pnpm` |

### Required for demos (Kubernetes)

| Tool | Purpose |
|------|---------|
| `kubectl` | Kubernetes CLI |
| `helm` | Kubernetes package manager |
| `kind` | Local Kubernetes cluster |

### External services (optional for basic local run)

- **PostgreSQL** — stores pipeline configuration persistently; the service starts without it but pipeline CRUD will fail
- **ClickHouse** — required for the sink role to actually write data
- **Kafka** — required for the ingestor role to consume events

---

## Local development

### Backend (glassflow-api)

**1. Copy and edit the environment file**

```bash
cp glassflow-api/.env.example glassflow-api/.env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GLASSFLOW_NATS_SERVER` | `localhost:4222` | NATS JetStream endpoint |
| `GLASSFLOW_LOG_FORMAT` | `json` | Log format (`json` or `text`) |
| `GLASSFLOW_LOG_LEVEL` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `GLASSFLOW_DATABASE_URL` | — | PostgreSQL DSN (e.g. `postgres://user:pass@localhost:5432/glassflow`) |
| `GLASSFLOW_SERVER_ADDR` | `:8081` | HTTP listen address |

**2. Start NATS server**

```bash
nats-server -js
```

NATS with JetStream enabled is required for all roles to communicate.

**3. Run the API**

```bash
cd glassflow-api
make run
# Service is now listening on http://localhost:8081
```

Or build and run the binary directly:

```bash
make build
./bin/clickhouse-etl -role api
```

Available roles: `api` (default), `ingestor`, `sink`, `dedup`, `join`.

**4. Verify the API is up**

```bash
curl http://localhost:8081/api/v1/pipeline
```

---

### Frontend (ui)

**1. Copy and edit the environment file**

```bash
cp ui/env.example ui/.env.local
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NEXT_PUBLIC_API_URL` | `http://localhost:8081` | Backend API endpoint |
| `NEXT_PUBLIC_FILTERS_ENABLED` | `true` | Enable filter stage in pipeline wizard |
| `NEXT_PUBLIC_TRANSFORMATIONS_ENABLED` | `true` | Enable transform stage |
| `NEXT_PUBLIC_AUTH0_ENABLED` | `false` | Enable Auth0 authentication |
| `NEXT_PUBLIC_OTEL_LOGS_ENABLED` | `true` | Ship logs to OpenTelemetry collector |
| `NEXT_PUBLIC_OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTel collector endpoint |

For local development, the defaults in `env.example` work out of the box — no changes needed unless you want Auth0 or a custom API URL.

**2. Install dependencies**

```bash
cd ui
pnpm install
```

**3. Start the dev server**

```bash
pnpm dev
# UI is now at http://localhost:3000
```

The `predev` hook auto-generates `.env.local` from your existing env file on first run.

**4. Build for production**

```bash
pnpm build
pnpm start  # serves on port 8080
```

---

## Running tests

All commands run from `glassflow-api/`.

```bash
# Unit tests (with race detector)
make run-test

# Fast unit tests only (skips slow integration tests)
make run-short-test

# End-to-end tests (requires Docker for Testcontainers)
make run-e2e-test

# Lint
make lint

# Full pre-push check (lint + unit + e2e)
make pre-push-check
```

Run a single test by name:

```bash
go test ./internal/... -run TestFunctionName -race
```

Frontend tests:

```bash
cd ui
pnpm test
```

---

## Demo scenarios

Demos require the GlassFlow CLI (`glassflow`) and a running Kubernetes environment.

### Fraud detection demo

Streams login events from Kafka through a filter + dedup pipeline into ClickHouse.

**Requirements:** Docker, `kubectl`, `helm`, `kind`, Python 3.x

```bash
# 1. Start local cluster and all services
glassflow up --demo

# 2. Configure credentials
cd demos/fraud-detection
cp .env.example .env
# Edit .env with your Kafka and ClickHouse endpoints

# 3. Set up resources
./scripts/create_topic.sh          # Create Kafka topic
./scripts/create_table.sh          # Create ClickHouse table
python create_pipeline.py          # Register GlassFlow pipeline

# 4. Generate and publish data
python generate_login_events.py    # Produces data/login-events.ndjson
./scripts/publish_to_kafka.sh data/login-events.ndjson

# 5. Query results
./scripts/run_fraud_queries.sh

# 6. Tear down
glassflow down
```

---

### Observability demo (OTLP)

Streams OpenTelemetry traces into ClickHouse and visualizes them with HyperDX.

```
TelemetryGen → OTel Collector → GlassFlow (OTLP receiver) → ClickHouse → HyperDX
```

**Minimum resources:** 6 CPU cores, 8 GB RAM, 10 GB free disk

```bash
cd demos/observability-v2

# One-shot setup (creates cluster, installs everything, starts telemetry)
make cluster
make deploy-stack

# Port-forward in separate terminals
make pf-glassflow-api   # API → :8080
make pf-glassflow       # UI  → :8081
make pf-hyperdx         # HyperDX → :8090
```

Access points once running:

| Service | URL |
|---------|-----|
| GlassFlow UI | http://localhost:8081 |
| GlassFlow API | http://localhost:8080 |
| HyperDX | http://localhost:8090 |

Tear down:

```bash
make cluster-delete
```

Useful status check:

```bash
make status
```
