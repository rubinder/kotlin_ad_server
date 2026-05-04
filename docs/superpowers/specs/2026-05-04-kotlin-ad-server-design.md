# kotlin_ad_server — Design Spec

**Date:** 2026-05-04
**Status:** Approved (brainstorming phase)
**Author:** robsrandhawa@gmail.com

## 1. Goal & Framing

A Kotlin-based ad serving runtime, built as a portfolio piece demonstrating idiomatic Kotlin (coroutines, structured concurrency), ad-tech domain authenticity (OpenRTB, frequency caps, competitive separation, floor pricing), and full-lifecycle observability (Micrometer, OpenTelemetry, distributed tracing across services). Target completion: 4–6 weeks part-time.

The differentiating signal is **measurable latency engineering**: a Gatling-driven load test at 5K–10K QPS, a profiling-and-fix narrative documented in the README, and per-stage histograms wired through the rule engine.

## 2. Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Inventory storage | Postgres (system of record) + in-memory snapshot at boot | Ad-tech-authentic; DB never on the request path. |
| Repo structure | Single Gradle monorepo, multi-module | Expresses "frequency service is a separate microservice" cleanly. |
| Bid request schema | OpenRTB 2.6 subset (banner, single-imp, open auction) | Domain-authentic naming a recruiter recognizes. |
| Inter-service protocol (ad-server → frequency) | gRPC (HTTP/2, persistent connections) | At 5K-10K QPS, ~3-5x cheaper per call than REST/JSON; `grpc-kotlin` is fully `suspend`. |
| Stream processor | Apache Flink | User-specified; stronger signal than Kafka Streams for portfolio. |
| Kafka serialization | Avro + Confluent Schema Registry | Ad-tech-authentic; better signal than JSON-on-Kafka. |
| Observability backend | Prometheus + Grafana + Jaeger (all in docker-compose) | Common production stack; trivially renders screenshots for README. |
| Tracing protocol | OpenTelemetry SDK → OTLP → Jaeger; W3C Trace Context | Vendor-neutral, the modern default. |

## 3. Module Layout

Monorepo at `kotlin_ad_server/`. Single `settings.gradle.kts`, version catalog at `gradle/libs.versions.toml`.

| Module | Purpose | Process |
|---|---|---|
| `common-protocol` | OpenRTB 2.6 subset DTOs (`BidRequest`, `BidResponse`, `Imp`, `Banner`, `Device`, `User`), Kafka event Avro schemas (`ImpressionEvent`, `AuctionResult`), gRPC `.proto` files | library |
| `inventory-loader` | Postgres → in-memory snapshot. Flyway migrations, seed CSV. Used by `ad-server`. | library |
| `ad-server` | Ktor (Netty) service, rule engine pipeline, Kafka producer, gRPC client to frequency | port 8080 |
| `frequency-service` | gRPC server, Lettuce → Redis. Owns freq counters and win-history. | port 9090 |
| `flink-impression-aggregator` | Flink job: `impression-events` Kafka source → windowed aggregate → Redis sink | flink job |
| `load-test` | Gatling Kotlin DSL scenarios | runner |

## 4. Infrastructure (docker-compose)

- Redis 7
- Kafka (KRaft mode, no Zookeeper) + topic init container
- Confluent Schema Registry
- Postgres 16 (ad inventory)
- Flink JobManager + TaskManager
- Jaeger all-in-one (OTLP receiver enabled)
- Prometheus
- Grafana (provisioned dashboard JSON)

**Boot sequence.** Postgres seeded → ad-server hydrates in-memory snapshot → frequency-service connects to Redis → Flink job submitted → ad-server `/healthz` flips to ready. Healthcheck blocks on snapshot load.

## 5. Request Hot Path

### 5.1 Pipeline shape

```kotlin
fun interface RuleStage {
    suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate>
}
```

`AuctionContext` carries the resolved `BidRequest`, user identity, the active OTel span, and a `MeterRegistry`. Pipeline:

```kotlin
stages.fold(initial) { acc, stage -> stage.evaluate(ctx, acc) }
```

### 5.2 Stages (in order)

1. **BlockingPolicies** — in-memory predicate over `BidRequest.bcat`/`badv`/`bapp`. No I/O.
2. **FrequencyCap + CompetitiveSeparation** — *one combined* gRPC call to `frequency-service`: `EnrichForAuction(userId, campaignIds[]) → {freqCounts, recentCategories}`. Filters where `freqCount >= cap` and where category overlaps with `recentCategories` within the competitive-separation window.
3. **FloorPrice** — in-memory: drops candidates where `bid < imp.bidfloor`. USD only for the demo.
4. **Selection** — highest bid wins; ties broken random. Returns `BidResponse` with the chosen creative or no-fill (`nbr=0`).

**Why combined gRPC for stages 2+3:** at target QPS, every saved network round-trip is real p99 budget. The frequency-service owns both the counters and the win-history because both are per-user Redis state, so the boundary is natural.

### 5.3 Concurrency model

- Ktor Netty engine, default coroutine dispatcher for request handling.
- Lettuce reactive Redis client bridged to coroutines via `kotlinx-coroutines-reactor`.
- `grpc-kotlin` stubs are natively `suspend`.
- Kafka producer: `KafkaProducer.send()` wrapped in `suspendCancellableCoroutine`, **fire-and-forget on the response path**. Producer config: `acks=1`, `linger.ms=5`, batched.
- Dedicated dispatcher for Kafka producer to avoid contention with request handling.

### 5.4 Failure semantics

- gRPC frequency call timeout: 8 ms. On timeout/error → **fail open** (allow all candidates through stages 2+3), emit `frequency.fail_open` counter. Latency wins, freshness loses.
- Kafka send failure: log + counter, never fails the request.
- Postgres: not on hot path.

### 5.5 Post-response emission

After returning the response, `launch` two events in a supervisor scope rooted in the request scope's parent (so they survive request completion but cannot fail the request):

- `auction-results` topic — winner + losers + reasons.
- `impression-events` topic — winner only. (In real ad-tech this comes from the client after rendering; we simulate it here so the Flink job has data.)

## 6. Frequency Service Contract

### 6.1 gRPC surface (proto in `common-protocol`)

```proto
service Frequency {
  rpc EnrichForAuction(EnrichRequest) returns (EnrichResponse);
}

message EnrichRequest {
  string user_id = 1;
  repeated string campaign_ids = 2;
}

message EnrichResponse {
  map<string, int32> freq_counts = 1;       // campaign_id -> count
  repeated string recent_categories = 2;     // IAB categories served to this user within compsep window
}
```

### 6.2 Redis schema

| Key | Type | TTL | Purpose |
|---|---|---|---|
| `freq:{userId}:{campaignId}` | counter | cap window (default 24h) | Per-user, per-campaign impression count |
| `winhistory:{userId}` | sorted set (score=ts, member=`{campaignId}:{category}`) | 1h sliding | Recent wins for competitive separation |

### 6.3 Atomicity

- INCRBY + EXPIRE on `freq:*` keys: Lua script for atomicity.
- ZADD + ZREMRANGEBYSCORE on `winhistory:*`: Lua script.

Increments are written by the **Flink sink**, not the ad-server. Lookups happen on the request path via `EnrichForAuction`.

## 7. Event Path & Flink Aggregator

### 7.1 Kafka topics

All keyed by `userId` (so all events for one user land on the same partition). 6 partitions, replication=1 for local dev.

| Topic | Producer | Consumer | Schema | Purpose |
|---|---|---|---|---|
| `bid-requests` | ad-server (mirror) | offline | `BidRequest` (Avro) | Audit / replay |
| `auction-results` | ad-server | offline | `AuctionResult` (Avro) | Post-hoc analysis |
| `impression-events` | ad-server | Flink | `ImpressionEvent` (Avro) | Drives frequency back-write |

Avro schemas live in `common-protocol/src/main/avro/`. Codegen via `gradle-avro-plugin`.

### 7.2 Flink job

```
KafkaSource<ImpressionEvent>(impression-events, exactly-once)
  → keyBy(userId, campaignId)
  → window: TumblingEventTimeWindows.of(Time.seconds(10)), allowedLateness=2s
  → aggregate: count
  → RichSinkFunction (Lettuce):
      INCRBY freq:{userId}:{campaignId} <count> + EXPIRE 24h   (Lua)
      ZADD winhistory:{userId} score=ts member={campaignId}:{category}
      ZREMRANGEBYSCORE winhistory:{userId} -inf <ts - 1h>     (Lua)
```

Checkpointing: filesystem state backend at `/tmp/flink-checkpoints` (mounted volume), 30s interval. Demonstrates exactly-once semantics in the README.

### 7.3 Why the Flink loop instead of direct ad-server writes

Documented explicitly in README:
- Decouples impression accounting from the request path (latency).
- Real ad-tech impressions originate client-side post-render; async aggregation matches that architecture regardless.
- Windowed aggregation enables trailing-window caps cheaply.
- Fully replayable: rebuild frequency state by replaying the topic from offset 0.

## 8. Observability

### 8.1 Micrometer metrics

Prometheus registry, `/metrics` endpoint on both ad-server and frequency-service.

| Metric | Type | Tags | Purpose |
|---|---|---|---|
| `adserver.request.duration` | Timer (histogram, p50/p95/p99) | `outcome`={filled, no-fill, error} | Total request latency |
| `adserver.stage.duration` | Timer (histogram) | `stage`={blocking, freq+compsep, floor, selection} | Per-stage latency — the differentiating metric |
| `adserver.candidates.surviving` | DistributionSummary | `stage` | Funnel |
| `frequency.grpc.duration` | Timer | `outcome`={ok, timeout, error, fail_open} | Frequency RPC latency + fail-open visibility |
| `redis.lookup.duration` | Timer | `op`={enrich, ack} | Redis-side timing inside frequency-service |
| `kafka.producer.send.duration` | Timer | `topic` | Kafka send latency |
| `inventory.snapshot.size` | Gauge | — | How many campaigns loaded |
| `inventory.snapshot.age_seconds` | Gauge | — | When was last hydration |

JVM/process metrics via Micrometer's built-in binders.

### 8.2 Grafana dashboard

JSON checked into `infra/grafana/dashboards/adserver.json`, provisioned at boot. Three rows: request latency (p50/p95/p99), stage breakdown (stacked area), funnel (candidates per stage).

### 8.3 OpenTelemetry tracing

OTel SDK + autoconfigure, OTLP gRPC exporter → Jaeger.

Span hierarchy per request:

```
adserver.request                      [root: user.id, imp.id, slot.size]
├─ rule.blocking                      [candidates.in/out]
├─ rule.frequency_and_compsep         [candidates.in/out, freq.fail_open]
│   └─ grpc.frequency.enrich          [client]
│       └─ frequency.enrich           [server, in frequency-service]
│           └─ redis.mget             [client]
├─ rule.floor                         [candidates.in/out]
├─ selection                          [winner.campaign_id, winner.bid]
└─ kafka.produce                      [LINKED, not parent — see below]
```

**Cross-service propagation:** W3C Trace Context (`traceparent`) auto-injected by gRPC client interceptor, extracted by server interceptor. Trace ID also injected into log MDC.

**Kafka span as link, not parent:** the Kafka producer span gets a `link` to the request span. The request span closes before Kafka acks complete; making it a parent would distort latency. Linked spans show the relationship without timing distortion. The Flink consumer creates a new trace rooted on the link.

### 8.4 Structured logging

Logback with `logstash-logback-encoder` (JSON). MDC-injected `trace_id`, `span_id`, `request_id`, `user_id`. Console output.

### 8.5 README artifacts

- Jaeger trace screenshot: single request, all spans visible across both services.
- Grafana dashboard screenshot under load.
- Annotated structured log line.

## 9. Load Testing & Profiling

### 9.1 Gatling (Kotlin DSL)

`load-test` module. `io.gatling.gradle` plugin, `./gradlew :load-test:gatlingRun`.

**Workload generator:**
- 1M synthetic user IDs, Zipfian-distributed (heavy users hit caps; long tail does not).
- 50 site IDs × 10 slot sizes.
- Pre-generated BidRequest fixtures as a JSON feed file.
- Inventory configured with category overlap to exercise competitive separation.

**Scenarios:**

| Scenario | Profile | Goal |
|---|---|---|
| `RampUp` | 0 → 10K QPS over 5 min, then steady 5 min | p50/p95/p99 at sustained load |
| `Burst` | 1K baseline, 5× spike for 30s, repeat 5× | Tail latency under bursts |
| `Soak` | 3K QPS for 30 min | Memory leaks, pool exhaustion |
| `FailFreq` | 5K QPS while frequency-service is killed for 30s mid-run | Verify fail-open and the latency cliff |

### 9.2 Initial SLO targets (revised after first measurement)

- p50 ≤ 5 ms, p95 ≤ 15 ms, p99 ≤ 30 ms at 5K QPS sustained.
- No fill-rate degradation under burst (fail-open works).

### 9.3 Profiling toolchain

- **async-profiler** attached to ad-server JVM during `RampUp` steady state. CPU + allocation profiles. Output: collapsed stacks → `flamegraph.pl` → SVG.
- **JFR** as backup (60s peak recording) for lock contention and GC.
- Micrometer histograms are the *first* place to look — they identify which stage is slow before flame graph analysis.

### 9.4 Predicted bottlenecks (NOT fixes — README documents what was actually found)

1. JSON serialization. *Mitigation*: `kotlinx.serialization` manual serializers, or `jackson-module-blackbird`.
2. Lettuce thread-pool sizing.
3. Coroutine dispatcher contention between request handling and Kafka serialization.
4. Kafka producer `linger.ms` tuning.
5. Avro serialization allocation churn.

### 9.5 README before/after section (required)

- p50/p95/p99 from first `RampUp` run, raw.
- Flame graph SVG (committed) with the bottleneck circled.
- One-paragraph fix explanation.
- p50/p95/p99 after fix.
- Note if multiple iterations needed.

## 10. Build Phases

Each phase is independently demonstrable. If time runs short, an earlier phase still ships a coherent artifact.

### Phase 1 — Skeleton + hot path (week 1)
- Gradle multi-module scaffolding, version catalog, `.editorconfig`, ktlint.
- `common-protocol` with OpenRTB 2.6 subset DTOs (kotlinx.serialization JSON; Avro added in Phase 3).
- `inventory-loader` with Postgres schema, Flyway migrations, ~50-campaign seed CSV.
- `ad-server` Ktor: `POST /openrtb/bid`, hydrates inventory at boot, full 5-stage rule engine with **fake** frequency (always 0).
- Unit tests per stage. Integration test: real Ktor + Testcontainers Postgres, golden-file BidRequest → BidResponse.
- ✅ `curl` a bid request, get a winner back.

### Phase 2 — Frequency service + Redis (week 2)
- gRPC `.proto` in `common-protocol`, `protobuf-gradle-plugin` + `grpc-kotlin` codegen.
- `frequency-service` module: gRPC server, Lettuce reactive client, combined `EnrichForAuction` RPC.
- Lua scripts for INCRBY+EXPIRE and ZADD+ZREMRANGEBYSCORE.
- ad-server's frequency stage now calls real gRPC, fail-open verified with timeout test.
- Testcontainers Redis in `frequency-service` integration tests.
- ✅ `redis-cli SET freq:user1:campaign1 10` → that campaign no longer wins for `user1`.

### Phase 3 — Kafka + Flink (week 3)
- Schema Registry + Avro schemas. `gradle-avro-plugin` codegen.
- ad-server produces `auction-results` and `impression-events` (fire-and-forget, dedicated dispatcher).
- `flink-impression-aggregator`: Kafka source → keyed window → Redis sink. Exactly-once via checkpointing.
- docker-compose adds Kafka, Schema Registry, Flink JM+TM.
- E2E: drive 100 requests → assert Redis counters reflect impressions within ~12 s.
- ✅ Send a few hundred requests, watch counters tick up and freq caps kick in.

### Phase 4 — Observability (week 4)
- Micrometer + Prometheus registry + `/metrics` on both services.
- OpenTelemetry SDK, OTLP exporter to Jaeger.
- gRPC + Kafka instrumentations enabled, W3C propagation verified.
- Logback JSON encoder + MDC injection.
- Grafana dashboard JSON, provisioned at boot.
- ✅ Single request renders as multi-service trace in Jaeger; Grafana shows live histograms.

### Phase 5 — Load testing + profiling (week 5)
- `load-test` with the 4 Gatling scenarios.
- Run `RampUp` baseline, capture numbers + async-profiler flame graph.
- Identify actual top bottleneck. Implement fix. Re-run.
- ✅ README has before/after p50/p95/p99 with flame graph SVG.

### Phase 6 — Polish + README + buffer (week 6)
- Architecture, rule engine, Kafka topology, request lifecycle diagrams (Mermaid in README).
- README sections: overview, architecture, running locally, rule engine, Kafka topology, observability, load test results.
- Jaeger screenshot, Grafana screenshot, flame graph SVG committed.
- Final test-coverage pass on edge cases.
- GitHub Actions CI: unit + integration tests with Testcontainers.

## 11. De-scope Dial

If time runs short, drop in this order:

1. `Soak` and `Burst` Gatling scenarios — keep only `RampUp` and `FailFreq`.
2. Avro + Schema Registry → JSON-on-Kafka with a README note explaining the tradeoff.
3. Flink exactly-once → at-least-once.

**Do not drop:** the observability layer or the before/after profiling story — those are the differentiating signals.

## 12. Out of Scope

- Authentication / authorization (this is a serving-path demo, not a productionized service).
- Multi-currency floor pricing (USD only).
- Multi-impression bid requests (single-imp only).
- Deals / PMP / private auctions (open auction only).
- Video / native creative formats (banner only).
- Multi-region deployment, k8s manifests.
- Bid throttling, budget pacing, advertiser spend caps (separate problem domain).
- Real ML ranking — selection is highest-bid + random tiebreak.

## 13. Open Questions for Implementation Phase

These are deferred to the implementation plan, not blocking spec approval:

- Exact Gradle version catalog (Kotlin 2.0+, Ktor 3.x, Flink 1.19+ likely targets — pin during Phase 1).
- Specific JVM target (21 LTS recommended, virtual threads available as a mitigation lever if profiling exposes carrier-thread blocking).
- Whether to publish the docker-compose stack with healthchecks gating service startup or use a manual `wait-for-it.sh` shim.
- Whether to wire OpenTelemetry via the Java agent (zero-code) or the SDK (explicit, more controllable). Currently leaning SDK for portfolio explicitness.
