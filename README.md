# kotlin_ad_server

Kotlin-based ad serving runtime. Demonstrates idiomatic Kotlin coroutines, OpenRTB 2.6 (subset),
and ad-tech-authentic architecture: Postgres-backed inventory loaded into an in-memory snapshot at
boot, with the request hot path serving from memory only. Full design: `docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`.

## Status

- ✅ **Phase 1 — Skeleton + hot path**
- ✅ **Phase 2 — Frequency service + Redis**
- ✅ **Phase 3 — Kafka + Flink aggregator** (this commit)
- ⏳ Phase 4 — Observability (Micrometer, OpenTelemetry, Jaeger, Prometheus, Grafana)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README

## Modules

- `common-protocol` — OpenRTB 2.6 subset DTOs (BidRequest, BidResponse, Imp, Banner, Site, Device, User).
- `inventory-loader` — Postgres schema (Flyway) + loader → in-memory `InventorySnapshot`. ~50 sample campaigns.
- `ad-server` — Ktor service exposing `POST /openrtb/bid`. Five-stage rule pipeline: blocking → frequency+compsep → floor → selection. Phase 1 uses a fake frequency client; Phase 2 wires gRPC to the standalone frequency-service.
- `frequency-service` — standalone gRPC service (port 9090) backed by Lettuce → Redis. Owns the per-user impression counters and recent-win history. Read-only on the gRPC layer in Phase 2; Phase 3 adds Flink-driven increments.
- `flink-impression-aggregator` — Apache Flink 1.20 streaming job. Consumes `impression-events` from Kafka (Avro via Confluent Schema Registry), keys by `(user, campaign)`, tumbling 10-second event-time windows, writes counts back to Redis through Lua-scripted atomic INCRBY+EXPIRE.

## Build

```bash
./gradlew build
```

## Run

The fastest way to bring up the full stack:

```bash
docker compose up -d
./scripts/kafka-init-topics.sh

# Run the frequency service (terminal 1)
./gradlew :frequency-service:run

# Run the Flink aggregator (terminal 2)
./gradlew :flink-impression-aggregator:run

# Run the ad-server (terminal 3)
./gradlew :ad-server:run

# In a fourth terminal, send a bid request:
curl -X POST http://localhost:8080/openrtb/bid \
    -H "Content-Type: application/json" \
    -d '{
        "id": "demo-1",
        "imp": [{ "id": "1", "banner": { "w": 300, "h": 250 } }],
        "user": { "id": "demo-user" }
    }'
```

## Smoke test

```bash
./scripts/smoke-test.sh
```

This runs the full integration test (Testcontainers Postgres + Ktor + golden BidRequest) and asserts a winning Bid comes back.

## Testing

```bash
./gradlew test
```

Requires Docker daemon for Testcontainers-based integration tests.
