# kotlin_ad_server

Kotlin-based ad serving runtime. Demonstrates idiomatic Kotlin coroutines, OpenRTB 2.6 (subset),
and ad-tech-authentic architecture: Postgres-backed inventory loaded into an in-memory snapshot at
boot, with the request hot path serving from memory only. Full design: `docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`.

## Status

- ✅ **Phase 1 — Skeleton + hot path** (this commit)
- ⏳ Phase 2 — Frequency service + Redis (gRPC, Lettuce)
- ⏳ Phase 3 — Kafka + Flink aggregator
- ⏳ Phase 4 — Observability (Micrometer, OpenTelemetry, Jaeger, Prometheus, Grafana)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README

## Modules

- `common-protocol` — OpenRTB 2.6 subset DTOs (BidRequest, BidResponse, Imp, Banner, Site, Device, User).
- `inventory-loader` — Postgres schema (Flyway) + loader → in-memory `InventorySnapshot`. ~50 sample campaigns.
- `ad-server` — Ktor service exposing `POST /openrtb/bid`. Five-stage rule pipeline: blocking → frequency+compsep → floor → selection. Phase 1 uses a fake frequency client; Phase 2 wires gRPC to the standalone frequency-service.

## Build

```bash
./gradlew build
```

## Run

```bash
# Start Postgres locally first (any way you like; example with Docker):
docker run -d --name kotlin-ad-pg \
    -e POSTGRES_USER=kotlin_ad_server \
    -e POSTGRES_PASSWORD=kotlin_ad_server \
    -e POSTGRES_DB=kotlin_ad_server \
    -p 5432:5432 \
    postgres:16-alpine

# Run ad-server (migrates schema, seeds data via Phase 2+ tooling — Phase 1 expects pre-seeded DB
# or use the smoke-test script below which handles seeding via Testcontainers):
./gradlew :ad-server:run
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
