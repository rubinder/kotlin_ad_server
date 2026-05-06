# kotlin_ad_server

[![CI](https://github.com/rubinder/kotlin_ad_server/actions/workflows/ci.yml/badge.svg)](https://github.com/rubinder/kotlin_ad_server/actions/workflows/ci.yml)

A Kotlin-based ad serving runtime: OpenRTB 2.6 (subset) auctions over a 5-stage rule engine, with a fail-open gRPC frequency service, Kafka + Flink closing the impression event loop, and full observability (Prometheus + Grafana + Jaeger). Built as a portfolio project to demonstrate idiomatic Kotlin coroutines and an ad-tech-authentic architecture.

Full design spec: [`docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`](docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md)

## Architecture

```mermaid
flowchart TB
    Client([Client])

    subgraph services[Services]
        AdServer[ad-server<br/>Ktor :8080]
        FreqService[frequency-service<br/>gRPC :9090]
        Flink[flink-impression-aggregator<br/>Flink job]
    end

    subgraph datastores[Datastores]
        Postgres[(Postgres<br/>inventory)]
        Redis[(Redis<br/>counters + winhistory)]
        Kafka[(Kafka<br/>auction-results<br/>impression-events)]
    end

    subgraph observability[Observability]
        Prometheus[Prometheus]
        Grafana[Grafana]
        Jaeger[Jaeger]
    end

    Client -->|HTTP /openrtb/bid| AdServer
    AdServer -->|JDBC at boot| Postgres
    AdServer -->|gRPC enrichForAuction<br/>8ms timeout, fail-open| FreqService
    AdServer -->|Kafka producer<br/>Avro| Kafka
    FreqService -->|RESP MGET + ZRANGE| Redis
    Kafka -->|Kafka source<br/>Avro| Flink
    Flink -->|Lua INCRBY+EXPIRE| Redis

    AdServer -. /metrics scrape .-> Prometheus
    FreqService -. /metrics scrape .-> Prometheus
    Prometheus --> Grafana
    AdServer -. OTLP gRPC :4317 .-> Jaeger
    FreqService -. OTLP gRPC :4317 .-> Jaeger
```

`ad-server` (Ktor) is the request hot path. It hydrates inventory from Postgres at boot and serves bid requests from in-memory state. The only network call on the hot path is a single gRPC enrichment call to `frequency-service` (Lettuce → Redis), with an 8 ms timeout and fail-open fallback. After serving a bid, ad-server fire-and-forgets two Kafka events; `flink-impression-aggregator` consumes one of them and updates Redis counters in 10-second tumbling event-time windows. All three services emit Prometheus metrics, OTLP traces to Jaeger, and structured JSON logs with W3C trace correlation.

## Modules

- **`common-protocol`** — OpenRTB 2.6 subset DTOs (BidRequest, BidResponse, Imp, Banner, Site, Device, User) plus gRPC + Avro generated classes shared by the services.
- **`inventory-loader`** — Postgres schema (Flyway migrations) + boot-time loader that produces the `InventorySnapshot`. ~50 sample campaigns.
- **`ad-server`** — Ktor service exposing `POST /openrtb/bid`. Hosts the 5-stage rule engine. Calls frequency-service via gRPC and produces Kafka events.
- **`frequency-service`** — Standalone gRPC service (port 9090) backed by Lettuce → Redis. Owns per-user impression counters and recent-win history.
- **`flink-impression-aggregator`** — Apache Flink 1.20 streaming job. Consumes `impression-events` (Avro via Confluent Schema Registry), keys by `(user, campaign)`, tumbling 10 s event-time windows, writes counts back to Redis through Lua-scripted atomic INCRBY+EXPIRE.
- **`load-test`** — Gatling Kotlin DSL load scenarios (RampUp / Burst / Soak / FailFreq).

## Rule Engine

```mermaid
flowchart LR
    BR([BidRequest]) --> S1
    S1[1. Blocking<br/>category + domain blocks] --> S2
    S2[2. Frequency + Compsep<br/>per-user caps + recency] --> S3
    S3[3. Floor<br/>price floor enforcement] --> S4
    S4[4. Selection<br/>highest bid + random tiebreak] --> BResp([BidResponse])

    S2 -. gRPC enrichForAuction<br/>8ms timeout<br/>fail-open .-> Freq[frequency-service]
    Freq -. EnrichResponse<br/>freqCounts + recentCategories .-> S2
```

The hot path runs four filtering stages followed by a selection stage. Each stage emits an `adserver.stage.duration` Timer tagged by stage and an `adserver.candidates.surviving` DistributionSummary so that drop-off at each step is visible in Grafana.

| # | Stage | What it does |
|---|---|---|
| 1 | Blocking | Removes campaigns whose category or domain is in the request's blocklist. |
| 2 | Frequency + Compsep | Calls `frequency-service` over gRPC for per-user counts and recent categories. Drops campaigns at or above their cap, plus competitive-separation neighbors. |
| 3 | Floor | Drops campaigns whose bid is below the request's floor price. |
| 4 | Selection | Picks the highest-priced surviving bid; random tiebreak. |

The frequency call is the only network hop on the hot path. It enforces an 8 ms timeout and falls back to an empty enrichment response on any failure, per spec section 5.4 ("latency wins, freshness loses").

## Kafka Topology

```mermaid
flowchart LR
    AdServer[ad-server] -->|produce<br/>AuctionResult Avro| AR[(auction-results)]
    AdServer -->|produce<br/>ImpressionEvent Avro| IE[(impression-events)]

    IE -->|consume<br/>10s tumbling window<br/>keyed by user+campaign| Flink[flink-impression-aggregator]
    Flink -->|Lua INCRBY+EXPIRE<br/>freq:user:campaign| Redis[(Redis)]

    AR -. no consumer in demo .-> Future([future analytics sink])

    classDef noconsumer fill:#f5f5f5,stroke-dasharray: 4 4,color:#666
    class Future noconsumer
```

| Topic | Key | Value | Producer | Consumer |
|---|---|---|---|---|
| `auction-results` | `auctionId` | `AuctionResult` (Avro) | ad-server | (none in demo) |
| `impression-events` | `userId` | `ImpressionEvent` (Avro) | ad-server | flink-impression-aggregator |

`auction-results` is currently produce-only — it exists to demonstrate the wider analytics shape but no demo consumer is wired up. `impression-events` closes the feedback loop: Flink reads it, aggregates by `(user, campaign)` over 10-second tumbling event-time windows, and writes counts back to Redis where `frequency-service` reads them on the next bid.

Avro schemas live in `common-protocol/src/main/avro/`. Confluent Schema Registry runs at `localhost:8081` in the docker-compose stack.

## Request Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as ad-server
    participant F as frequency-service
    participant R as Redis
    participant K as Kafka

    Note over A,F: W3C traceparent header propagates over gRPC<br/>so all spans land in one Jaeger trace

    C->>+A: POST /openrtb/bid (BidRequest)
    A->>A: stage 1: blocking
    A->>+F: enrichForAuction (gRPC, 8ms timeout)
    F->>+R: MGET freq:user:* + ZRANGE winhistory:user
    R-->>-F: counters + recent categories
    F-->>-A: EnrichResponse
    A->>A: stage 2: freq + compsep filter
    A->>A: stage 3: floor enforcement
    A->>A: stage 4: selection (highest bid)
    A-)K: produce AuctionResult (async, Avro)
    A-->>-C: BidResponse
    A-)K: produce ImpressionEvent (async, Avro)
    Note over K: flink-impression-aggregator consumes<br/>impression-events later (10s windows)<br/>and writes counters back to Redis
```

## Run locally

```bash
docker compose up -d
./scripts/kafka-init-topics.sh

# Run the frequency service (terminal 1)
./gradlew :frequency-service:run

# Run the Flink aggregator (terminal 2)
./gradlew :flink-impression-aggregator:run

# Run the ad-server (terminal 3)
./gradlew :ad-server:run

# Send a bid request (terminal 4)
curl -X POST http://localhost:8080/openrtb/bid \
    -H "Content-Type: application/json" \
    -d '{
        "id": "demo-1",
        "imp": [{ "id": "1", "banner": { "w": 300, "h": 250 } }],
        "user": { "id": "demo-user" }
    }'
```

## Observability

`docker compose up -d` brings up Prometheus, Grafana, and Jaeger alongside the rest.

- ad-server `/metrics` — http://localhost:8080/metrics
- frequency-service `/metrics` — http://localhost:9091/metrics
- Prometheus targets — http://localhost:9090/targets
- Grafana dashboard — http://localhost:3000/d/kotlin-ad-server (anonymous admin)
- Jaeger UI — http://localhost:16686

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

Both services emit JSON logs via `logstash-logback-encoder`, with `trace_id` / `span_id` auto-injected into MDC by `opentelemetry-logback-mdc-1.0`. Every log line correlates to its trace.

## Load testing

Four Gatling Kotlin DSL scenarios in the `:load-test` module:

| Scenario | Profile | Goal |
|---|---|---|
| `RampUp` | 0 → 5K QPS over 5 min, then 5 min steady | sustained-load latency baseline |
| `Burst` | 1K baseline / 5K spike, 3 cycles | tail latency under load transitions |
| `Soak` | 3K QPS for 30 min | memory leaks, pool exhaustion |
| `FailFreq` | 5K QPS while frequency-service is killed | fail-open behavior under load |

Run: `./scripts/load-test.sh RampUp docs/load-test/baseline-run` (after `docker compose up -d` and the three service `./gradlew :*:run`).

### Profiling: before / after

We profiled the baseline RampUp at 5K QPS with [async-profiler](https://github.com/async-profiler/async-profiler). The flame graph showed `Dispatchers.IO` frames dominating the freq-RPC path — a leftover from Phase 2 where `withContext(Dispatchers.IO)` was added around `withTimeout` to dodge `kotlinx.coroutines.test`'s virtual-time scheduler. In production this forced a context switch per RPC.

The fix (Phase 5 Task 10): removed the wrap, switched the affected tests to `runBlocking`. Detailed results in [docs/load-test/baseline.md](docs/load-test/baseline.md) and [docs/load-test/after.md](docs/load-test/after.md), including flame graphs.

## CI

Every push and PR runs `./gradlew check` (test + ktlint) on Ubuntu (`ubuntu-latest`) with JDK 21. The first run is cold and takes ~6–10 min (Testcontainers spins up Postgres / Redis / Kafka / Schema Registry); subsequent runs hit the Gradle dependency cache and finish in ~3–5 min. Failed runs upload JUnit + ktlint reports as a `test-and-lint-reports` artifact under the run's "Artifacts" panel.

Workflow file: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

## Testing locally

```bash
./gradlew test                    # full suite (requires Docker for Testcontainers)
./scripts/smoke-test.sh           # alias for the above with a friendly success banner
./scripts/load-test.sh RampUp     # see "Load testing" above
```
