# kotlin_ad_server

Kotlin-based ad serving runtime. Demonstrates idiomatic Kotlin coroutines, OpenRTB 2.6 (subset),
and ad-tech-authentic architecture: Postgres-backed inventory loaded into an in-memory snapshot at
boot, with the request hot path serving from memory only. Full design: `docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`.

## Status

- ✅ **Phase 1 — Skeleton + hot path**
- ✅ **Phase 2 — Frequency service + Redis**
- ✅ **Phase 3 — Kafka + Flink aggregator**
- ✅ **Phase 4 — Observability** (this commit)
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

## Observability

`docker compose up -d` brings up Prometheus (port 9090) and Grafana (port 3000) alongside the rest.

- ad-server `/metrics` — `http://localhost:8080/metrics`
- frequency-service `/metrics` — `http://localhost:9091/metrics`
- Prometheus targets — `http://localhost:9090/targets`
- Grafana dashboard — `http://localhost:3000/d/kotlin-ad-server` (anonymous admin)

Headline metrics:

| Metric | Type | Tags | Source |
|---|---|---|---|
| `adserver.request.duration` | Timer (histogram) | `outcome` | ad-server |
| `adserver.stage.duration` | Timer (histogram) | `stage` | ad-server |
| `adserver.candidates.surviving` | DistributionSummary | `stage` | ad-server |
| `frequency.grpc.duration` | Timer (histogram) | `outcome` | ad-server |
| `kafka.producer.send.duration` | Timer (histogram) | `topic` | ad-server |
| `inventory.snapshot.size` | Gauge | — | ad-server |
| `inventory.snapshot.age_seconds` | Gauge | — | ad-server |
| `redis.lookup.duration` | Timer (histogram) | `op` | frequency-service |

### Distributed tracing

`docker compose up -d` starts Jaeger all-in-one alongside Prometheus + Grafana.

- Jaeger UI — `http://localhost:16686`
- OTLP gRPC ingest — `localhost:4317` (default endpoint that both services point at)

Span hierarchy per request:

```
adserver.request                          [root, in ad-server]
├─ rule.blocking                          [in ad-server]
├─ rule.freq+compsep                      [in ad-server]
│   └─ enrichForAuction                   [gRPC client span]
│       └─ frequency.Frequency/EnrichForAuction  [gRPC server span, in frequency-service]
│           └─ redis.enrich               [manual span in frequency-service]
├─ rule.floor
└─ rule.selection
```

Trace context propagates via W3C `traceparent` headers on the gRPC call, injected by
`opentelemetry-grpc-1.6` interceptors on both sides.

### Structured logging

Both services emit JSON logs via `logstash-logback-encoder`, with `trace_id` / `span_id`
auto-injected into MDC by `opentelemetry-logback-mdc-1.0`. Every log line correlates to its
trace.

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
