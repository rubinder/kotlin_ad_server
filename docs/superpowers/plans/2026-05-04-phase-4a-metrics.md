# Phase 4a — Micrometer Metrics + Prometheus + Grafana Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire Micrometer metrics through the request hot path (per-stage timing, total request duration, candidates surviving, frequency RPC outcome, Kafka producer send time, inventory gauges) on both `ad-server` and `frequency-service`, exposed via a Prometheus `/metrics` scrape endpoint, scraped into a Prometheus container, visualized in a provisioned Grafana dashboard. This is the differentiating signal called out in the design ("the differentiating layer: latency instrumentation").

**Architecture:** Each service owns a `PrometheusMeterRegistry` constructed at boot. JVM/process metrics come from Micrometer's built-in binders. Domain metrics are passed into the components that emit them via constructor injection (matches the spec's `AuctionContext.meterRegistry` design intent). `ad-server` exposes `/metrics` as a Ktor route (Ktor is already running). `frequency-service` is gRPC-only, so it spins up a tiny `com.sun.net.httpserver.HttpServer` on a separate port for `/metrics` (no extra deps). Prometheus scrapes both, Grafana provisions one dashboard JSON checked into the repo.

**Tech Stack additions:**
- `io.micrometer:micrometer-core` 1.13.6
- `io.micrometer:micrometer-registry-prometheus` 1.13.6
- `io.ktor:ktor-server-metrics-micrometer` (for HTTP request timing on Ktor)
- Prometheus 2.55 (docker image `prom/prometheus:v2.55.0`)
- Grafana 11.3 (docker image `grafana/grafana:11.3.0`)

Phase 4b (tracing — OpenTelemetry + Jaeger + MDC) is a separate plan that ships next.

---

## Scope

**In:**
- Per-stage Timer + total request Timer (histograms, p50/p95/p99) on ad-server
- DistributionSummary for candidates surviving per stage on ad-server
- Tagged Timer for `frequency.grpc.duration` (outcome ∈ {ok, timeout, error, fail_open})
- Tagged Timer for `kafka.producer.send.duration` (topic ∈ {auction-results, impression-events})
- Tagged Timer for `redis.lookup.duration` on frequency-service
- Inventory gauges (`inventory.snapshot.size`, `inventory.snapshot.age_seconds`)
- JVM binders (memory, GC, threads, processor)
- `/metrics` Prometheus scrape endpoint on both services
- Prometheus + Grafana in docker-compose with provisioned dashboard

**Out (deferred to 4b or polish):**
- OpenTelemetry tracing
- Jaeger
- Structured JSON logging with MDC trace_id injection
- Flink job metrics (uses Flink's own metric system; integrate later if Phase 5 profiling shows it matters)

---

## File Structure

```
kotlin_ad_server/
├── docker-compose.yml                                            (modify: add prometheus + grafana)
├── gradle/libs.versions.toml                                     (modify: micrometer + ktor-server-metrics)
├── infra/                                                        (NEW directory)
│   ├── prometheus/prometheus.yml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/prometheus.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/
│           └── kotlin_ad_server.json
│
├── ad-server/
│   ├── build.gradle.kts                                          (modify: add micrometer + ktor-metrics deps)
│   └── src/main/kotlin/com/github/robran/adserver/
│       ├── AppConfig.kt                                          (modify: MetricsConfig)
│       ├── Application.kt                                        (modify: wire registry + /metrics route)
│       ├── metrics/                                              (NEW)
│       │   ├── MeterRegistryFactory.kt
│       │   ├── PipelineMetrics.kt                                # metric name constants + helpers
│       │   └── InventoryGauges.kt
│       ├── auction/
│       │   ├── AuctionPipeline.kt                                (modify: timing + candidates)
│       │   └── GrpcFrequencyClient.kt                            (modify: outcome-tagged timer)
│       ├── kafka/
│       │   └── KafkaEventEmitter.kt                              (modify: per-topic send timer)
│       └── http/
│           └── MetricsRoute.kt                                   (NEW: /metrics scrape route)
│   ├── src/main/resources/application.conf                       (modify: metrics block)
│   └── src/test/kotlin/com/github/robran/adserver/metrics/
│       └── MeterRegistryFactoryTest.kt                           (NEW)
│
└── frequency-service/
    ├── build.gradle.kts                                          (modify: add micrometer)
    └── src/main/kotlin/com/github/robran/adserver/frequency/
        ├── AppConfig.kt                                          (modify: MetricsConfig)
        ├── Application.kt                                        (modify: start MetricsHttpServer + wire registry)
        ├── EnrichService.kt                                      (modify: time the redis lookup)
        ├── RedisClient.kt                                        (modify: optional MeterRegistry param)
        └── metrics/                                              (NEW)
            ├── MeterRegistryFactory.kt
            └── MetricsHttpServer.kt                              # com.sun.net.httpserver wrapper
    └── src/main/resources/application.conf                       (modify: metrics block)
    └── src/test/kotlin/com/github/robran/adserver/frequency/metrics/
        └── MetricsHttpServerTest.kt                              (NEW)
```

---

## Task 1: Version Catalog Additions

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1: Add new versions**

In `[versions]`, append after the Phase 3 group:

```toml

# Phase 4a additions
micrometer = "1.13.6"
```

(Prometheus and Grafana versions live in `docker-compose.yml`, not the catalog.)

- [ ] **Step 2: Add new libraries**

In `[libraries]`, append:

```toml

# Phase 4a: Metrics
micrometer-core = { module = "io.micrometer:micrometer-core", version.ref = "micrometer" }
micrometer-registry-prometheus = { module = "io.micrometer:micrometer-registry-prometheus", version.ref = "micrometer" }
ktor-server-metrics-micrometer = { module = "io.ktor:ktor-server-metrics-micrometer", version.ref = "ktor" }
```

(`ktor-server-metrics-micrometer` reuses the existing `ktor` version 3.0.3.)

- [ ] **Step 3: Verify**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add gradle/libs.versions.toml
git commit -m "Phase 4a task 1: catalog adds micrometer + ktor-server-metrics-micrometer"
```

---

## Task 2: ad-server `MeterRegistryFactory` + `/metrics` route

**Files:**
- Modify: `ad-server/build.gradle.kts`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`
- Modify: `ad-server/src/main/resources/application.conf`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/metrics/MeterRegistryFactory.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/http/MetricsRoute.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/metrics/MeterRegistryFactoryTest.kt`

This task adds the registry, JVM binders, and the scrape endpoint. Subsequent tasks plug into the registry from each component.

- [ ] **Step 1: Add deps to `ad-server/build.gradle.kts`**

In the existing `implementation` section, add:

```kotlin
    // Phase 4a: Micrometer metrics
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.ktor.server.metrics.micrometer)
```

- [ ] **Step 2: Add `MetricsConfig` to `AppConfig.kt`**

```kotlin
data class MetricsConfig(
    val enabled: Boolean,
    val commonTags: Map<String, String>,
)
```

Append to `AppConfig`:

```kotlin
data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
    val frequency: FrequencyConfig,
    val kafka: KafkaConfig,
    val metrics: MetricsConfig,
) { ... }
```

- [ ] **Step 3: Add `metrics` block to `application.conf`**

Append inside the `adserver { ... }` block:

```hocon
    metrics {
        enabled = true
        enabled = ${?METRICS_ENABLED}
        commonTags {
            service = "ad-server"
            env = "local"
            env = ${?METRICS_ENV}
        }
    }
```

- [ ] **Step 4: Write `MeterRegistryFactory.kt`**

```kotlin
package com.github.robran.adserver.metrics

import com.github.robran.adserver.MetricsConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

/**
 * Builds the process-wide Prometheus registry, attaches JVM/process binders, and applies common
 * tags so every emitted metric is labeled by service+env without each call site repeating itself.
 */
object MeterRegistryFactory {
    fun build(config: MetricsConfig): PrometheusMeterRegistry {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        config.commonTags.forEach { (k, v) -> registry.config().commonTags(k, v) }
        bindJvmMetrics(registry)
        return registry
    }

    private fun bindJvmMetrics(registry: MeterRegistry) {
        ClassLoaderMetrics().bindTo(registry)
        JvmMemoryMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        JvmThreadMetrics().bindTo(registry)
        ProcessorMetrics().bindTo(registry)
        UptimeMetrics().bindTo(registry)
    }
}
```

- [ ] **Step 5: Write `MetricsRoute.kt`**

```kotlin
package com.github.robran.adserver.http

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

fun Route.metricsRoutes(registry: PrometheusMeterRegistry) {
    get("/metrics") {
        call.respondText(registry.scrape(), ContentType.parse("text/plain; version=0.0.4"), HttpStatusCode.OK)
    }
}
```

- [ ] **Step 6: Update `Application.kt`**

Read the current file. Make these changes:

(a) Add imports near the top:

```kotlin
import com.github.robran.adserver.metrics.MeterRegistryFactory
import com.github.robran.adserver.http.metricsRoutes
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
```

(b) In `main()`, **before** building the pipeline, construct the registry:

```kotlin
    val meterRegistry = MeterRegistryFactory.build(config.metrics)
```

(c) Pass `meterRegistry` to `adServerModule`. Update the signature:

```kotlin
fun Application.adServerModule(
    healthState: HealthState,
    pipeline: AuctionPipeline,
    meterRegistry: PrometheusMeterRegistry,
) { ... }
```

Inside `adServerModule`, install the Ktor Micrometer plugin and add the scrape route. Add right after `install(CallLogging)`:

```kotlin
    install(MicrometerMetrics) {
        registry = meterRegistry
        // Default Ktor metrics: ktor.http.server.requests Timer with histogram, status, method, route
    }
```

In the `routing { ... }` block, add the metrics route alongside health:

```kotlin
    routing {
        healthRoutes(healthState)
        bidRoutes(pipeline)
        metricsRoutes(meterRegistry)
    }
```

(d) In `main()`, update the `embeddedServer { adServerModule(...) }` call to pass the registry:

```kotlin
    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, pipeline, meterRegistry)
    }.start(wait = true)
```

(e) Update the shutdown hook to close the registry:

```kotlin
    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down ad-server")
            eventEmitter.close()
            frequencyChannel.shutdown()
            meterRegistry.close()
        },
    )
```

- [ ] **Step 7: Update integration tests**

Both `BidRouteIntegrationTest` and `Phase2EndToEndTest` and `Phase3EndToEndTest` call `adServerModule(HealthState(), pipeline)` (two args). The new third param has no default — explicitly pass a fresh registry per test.

In each of those three test files, find the `application { adServerModule(HealthState().apply { ready.set(true) }, pipeline) }` invocation and change it to:

```kotlin
application {
    adServerModule(
        HealthState().apply { ready.set(true) },
        pipeline,
        io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT),
    )
}
```

(Use the FQN imports inline rather than adding to each test's import list — keeps the change minimal.)

- [ ] **Step 8: Write the unit test**

`ad-server/src/test/kotlin/com/github/robran/adserver/metrics/MeterRegistryFactoryTest.kt`:

```kotlin
package com.github.robran.adserver.metrics

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import com.github.robran.adserver.MetricsConfig
import org.junit.jupiter.api.Test

class MeterRegistryFactoryTest {

    @Test
    fun `registry has JVM and process binders attached`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, commonTags = mapOf("service" to "ad-server")),
        )
        // Sentinel metrics emitted by JVM binders
        val names = registry.meters.map { it.id.name }.toSet()
        assertThat(names).contains("jvm.memory.used")
        assertThat(names).contains("jvm.threads.live")
        assertThat(names).contains("system.cpu.count")
        registry.close()
    }

    @Test
    fun `commonTags are applied to every metric`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(
                enabled = true,
                commonTags = mapOf("service" to "ad-server", "env" to "test"),
            ),
        )
        // Common tags are applied via registry.config().commonTags(); they appear on every meter.
        val sample = registry.meters.first()
        val tagKeys = sample.id.tags.map { it.key }
        assertThat(tagKeys).contains("service")
        assertThat(tagKeys).contains("env")
        assertThat(sample.id.getTag("service")).isEqualTo("ad-server")
        registry.close()
    }

    @Test
    fun `scrape returns Prometheus-formatted text`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, commonTags = mapOf("service" to "ad-server")),
        )
        val text = registry.scrape()
        assertThat(text).contains("# TYPE")
        assertThat(text).contains("jvm_memory_used_bytes")
        registry.close()
    }
}
```

- [ ] **Step 9: Run the tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.metrics.MeterRegistryFactoryTest"`
Expected: PASS, 3 tests green.

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL — all earlier tests still pass with the new signature.

- [ ] **Step 10: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt \
        ad-server/src/main/resources/application.conf \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/metrics/MeterRegistryFactory.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/http/MetricsRoute.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/metrics/MeterRegistryFactoryTest.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase2EndToEndTest.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase3EndToEndTest.kt
git commit -m "Phase 4a task 2: ad-server MeterRegistryFactory + /metrics endpoint"
```

---

## Task 3: ad-server Auction Pipeline Metrics

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/metrics/PipelineMetrics.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt`

This task adds three metrics:
- `adserver.request.duration` — Timer with histogram (p50/p95/p99), tagged `outcome` ∈ {filled, no-fill, error}
- `adserver.stage.duration` — Timer with histogram, tagged `stage` ∈ {blocking, freq+compsep, floor, selection}
- `adserver.candidates.surviving` — DistributionSummary, tagged `stage`

`AuctionPipeline` accepts a `MeterRegistry` (defaulting to a no-op registry) and times each stage's `evaluate` call.

- [ ] **Step 1: Write `PipelineMetrics.kt`**

```kotlin
package com.github.robran.adserver.metrics

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

/**
 * Owns the metric names + handles for the auction pipeline. Constructed once at boot from the
 * process registry, reused for every request. Tag values are bounded:
 *
 *   adserver.request.duration : outcome ∈ {filled, no-fill, error}
 *   adserver.stage.duration   : stage   ∈ {blocking, freq+compsep, floor, selection}
 *   adserver.candidates.surviving : stage ∈ same as above + "initial"
 *
 * For tests / no-op contexts, [defaultRegistry] returns a SimpleMeterRegistry — counters still
 * accumulate but nothing is exposed.
 */
class PipelineMetrics(private val registry: MeterRegistry) {

    fun requestTimer(outcome: String): Timer =
        Timer.builder(REQUEST_DURATION)
            .tag("outcome", outcome)
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry)

    fun stageTimer(stage: String): Timer =
        Timer.builder(STAGE_DURATION)
            .tag("stage", stage)
            .publishPercentileHistogram()
            .register(registry)

    fun candidatesSurvivingSummary(stage: String): DistributionSummary =
        DistributionSummary.builder(CANDIDATES_SURVIVING)
            .tag("stage", stage)
            .register(registry)

    companion object {
        const val REQUEST_DURATION = "adserver.request.duration"
        const val STAGE_DURATION = "adserver.stage.duration"
        const val CANDIDATES_SURVIVING = "adserver.candidates.surviving"

        /** Default no-op registry for tests that don't care about metrics. */
        fun defaultRegistry(): MeterRegistry = SimpleMeterRegistry()
    }
}
```

- [ ] **Step 2: Modify `AuctionPipeline.kt`**

Add a `metrics: PipelineMetrics` constructor parameter (with default), and time stages + total request:

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.kafka.EventEmitter
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.metrics.PipelineMetrics
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import io.micrometer.core.instrument.MeterRegistry
import java.util.UUID
import kotlin.system.measureNanoTime

class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
    private val eventEmitter: EventEmitter = NoOpEventEmitter,
    private val clock: () -> Long = { System.currentTimeMillis() },
    meterRegistry: MeterRegistry = PipelineMetrics.defaultRegistry(),
) {
    private val metrics = PipelineMetrics(meterRegistry)

    // Stage names parallel `stages`. Index 0 = blocking, 1 = freq+compsep, 2 = floor, 3 = selection.
    private val stageNames = listOf("blocking", "freq+compsep", "floor", "selection")

    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val totalNanos: Long
        val response: BidResponse
        var outcomeTag = "no-fill"
        try {
            totalNanos = measureNanoTime {
                response = runAuctionInner(request).also { resp ->
                    outcomeTag = if (resp.seatbid.isNotEmpty()) "filled" else "no-fill"
                }
            }
        } catch (t: Throwable) {
            metrics.requestTimer("error").record(0, java.util.concurrent.TimeUnit.NANOSECONDS)
            throw t
        }
        metrics.requestTimer(outcomeTag).record(totalNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
        return response
    }

    private suspend fun runAuctionInner(request: BidRequest): BidResponse {
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)
        metrics.candidatesSurvivingSummary("initial").record(initial.size.toDouble())

        val sizes = IntArray(4)
        sizes[0] = initial.size

        if (initial.isEmpty()) {
            emitOutcome(request, ctx, sizes, Outcome.NO_FILL_BLOCKING, winner = null)
            return BidResponse(id = request.id, nbr = NoBidReason.NO_MATCHING_CREATIVE)
        }

        var current = initial
        for ((idx, stage) in stages.withIndex()) {
            val stageName = stageNames.getOrElse(idx) { "stage-$idx" }
            val stageNanos: Long
            val newCurrent: List<Candidate>
            stageNanos = measureNanoTime {
                newCurrent = stage.evaluate(ctx, current)
            }
            current = newCurrent
            metrics.stageTimer(stageName).record(stageNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
            metrics.candidatesSurvivingSummary(stageName).record(current.size.toDouble())
            if (idx + 1 < sizes.size) sizes[idx + 1] = current.size
            if (current.isEmpty()) {
                val outcome = when (idx) {
                    0 -> Outcome.NO_FILL_BLOCKING
                    1 -> Outcome.NO_FILL_FREQ_COMPSEP
                    2 -> Outcome.NO_FILL_FLOOR
                    else -> Outcome.NO_FILL_OTHER
                }
                emitOutcome(request, ctx, sizes, outcome, winner = null)
                return BidResponse(id = request.id, nbr = noBidReasonFor(idx))
            }
        }

        val winner = current.single()
        emitOutcome(request, ctx, sizes, Outcome.FILLED, winner = winner)
        emitImpression(ctx, winner)

        return BidResponse(
            id = request.id,
            seatbid = listOf(
                SeatBid(
                    seat = winner.campaign.advertiserId,
                    bid = listOf(
                        Bid(
                            id = UUID.randomUUID().toString(),
                            impid = ctx.imp.id,
                            price = winner.bidPrice,
                            cid = winner.campaign.id,
                            crid = winner.creative.id,
                            adid = winner.creative.id,
                            cat = listOf(winner.campaign.category),
                            w = winner.creative.width,
                            h = winner.creative.height,
                            adm = winner.creative.markup,
                        ),
                    ),
                ),
            ),
        )
    }

    private fun emitOutcome(
        request: BidRequest,
        ctx: AuctionContext,
        sizes: IntArray,
        outcome: Outcome,
        winner: Candidate?,
    ) {
        val event = AuctionResultEvent.newBuilder()
            .setRequestId(request.id)
            .setUserId(ctx.userId)
            .setImpId(ctx.imp.id)
            .setTsMillis(clock())
            .setOutcome(outcome)
            .setWinnerCampaignId(winner?.campaign?.id)
            .setWinnerPrice(winner?.bidPrice)
            .setCandidatesInitial(sizes[0])
            .setCandidatesAfterBlocking(sizes[1])
            .setCandidatesAfterFreqCompsep(sizes[2])
            .setCandidatesAfterFloor(sizes[3])
            .build()
        eventEmitter.emitAuctionResult(event)
    }

    private fun emitImpression(ctx: AuctionContext, winner: Candidate) {
        val event = ImpressionEvent.newBuilder()
            .setUserId(ctx.userId)
            .setCampaignId(winner.campaign.id)
            .setCreativeId(winner.creative.id)
            .setCategory(winner.campaign.category)
            .setPrice(winner.bidPrice)
            .setTsMillis(clock())
            .build()
        eventEmitter.emitImpression(event)
    }

    private fun resolveUserId(request: BidRequest): String =
        request.user?.id
            ?: request.user?.buyeruid
            ?: "anonymous"

    private fun noBidReasonFor(stageIndex: Int): Int = when (stageIndex) {
        0 -> NoBidReason.NO_CANDIDATES_AFTER_BLOCKING
        1 -> NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP
        2 -> NoBidReason.NO_CANDIDATES_AFTER_FLOOR
        else -> NoBidReason.UNKNOWN_ERROR
    }
}
```

Note on the `var response` + `var outcomeTag` pattern at the top of `runAuction`: the `measureNanoTime { ... }` lambda has to assign to outer locals. Kotlin requires those be `var` and assigned before the timer reads them. Test: ensure all paths through `runAuctionInner` either return a value or throw — the `response` local is always assigned after the `measureNanoTime` block (because the lambda completes only when `runAuctionInner` returns).

- [ ] **Step 3: Wire MeterRegistry through `Application.kt`**

In `buildPipeline`, add `meterRegistry` parameter:

```kotlin
fun buildPipeline(
    snapshot: InventorySnapshot,
    frequencyClient: FrequencyClient,
    eventEmitter: com.github.robran.adserver.kafka.EventEmitter = com.github.robran.adserver.kafka.NoOpEventEmitter,
    meterRegistry: io.micrometer.core.instrument.MeterRegistry =
        com.github.robran.adserver.metrics.PipelineMetrics.defaultRegistry(),
): AuctionPipeline = AuctionPipeline(
    candidateBuilder = CandidateBuilder(snapshot),
    stages = listOf(
        BlockingPolicyStage(),
        FrequencyAndCompsepStage(frequencyClient),
        FloorPriceStage(),
        SelectionStage(Random.Default),
    ),
    eventEmitter = eventEmitter,
    meterRegistry = meterRegistry,
)
```

In `main()`, pass the registry:

```kotlin
    val pipeline = buildPipeline(snapshot, frequencyClient, eventEmitter, meterRegistry)
```

- [ ] **Step 4: Add a metrics test to `AuctionPipelineTest`**

In `ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt`, append a new `@Test`:

```kotlin
    @Test
    fun `records request total + per-stage timers + candidates surviving`() = runTest {
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            meterRegistry = registry,
        )
        pipe.runAuction(req())

        // adserver.request.duration with outcome=filled has at least one observation
        val requestTimer = registry.timer("adserver.request.duration", "outcome", "filled")
        assertThat(requestTimer.count()).isEqualTo(1L)

        // Each stage has a timer with at least one observation
        for (stage in listOf("blocking", "freq+compsep", "floor", "selection")) {
            val t = registry.timer("adserver.stage.duration", "stage", stage)
            assertThat(t.count()).isEqualTo(1L)
        }

        // candidates.surviving for "initial" recorded at least once
        val survivingInitial = registry.summary("adserver.candidates.surviving", "stage", "initial")
        assertThat(survivingInitial.count()).isEqualTo(1L)
    }
```

You'll need an additional import at the top of the file:

```kotlin
import com.github.robran.adserver.kafka.NoOpEventEmitter
```

- [ ] **Step 5: Run tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTest"`
Expected: PASS, 8 tests green (7 existing + 1 new).

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/metrics/PipelineMetrics.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt
git commit -m "Phase 4a task 3: AuctionPipeline metrics (request total + per-stage + candidates surviving)"
```

---

## Task 4: ad-server `frequency.grpc.duration` (outcome-tagged) on `GrpcFrequencyClient`

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`

`GrpcFrequencyClient` records `frequency.grpc.duration` tagged `outcome` ∈ {ok, timeout, error, fail_open}. Note: `timeout` and `error` both currently fail-open in behavior; the tag distinguishes them for diagnostics. (`fail_open` is reserved for a future explicit fail-open path; for now it's never emitted but the tag value is documented.)

- [ ] **Step 1: Modify `GrpcFrequencyClient.kt`**

Replace the file contents:

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.ManagedChannel
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

/**
 * Real gRPC implementation of [FrequencyClient]. Calls the standalone frequency-service.
 * Enforces an 8 ms timeout (configurable for tests) and falls back to an empty [EnrichResult]
 * on any error (timeout, server error, channel failure). Per spec section 5.4: latency wins,
 * freshness loses.
 *
 * Records `frequency.grpc.duration` (Timer with histogram) tagged by outcome:
 *   - ok      : RPC completed successfully within timeout
 *   - timeout : 8ms budget exceeded; fail-open empty response
 *   - error   : any other Throwable; fail-open empty response
 */
class GrpcFrequencyClient(
    channel: ManagedChannel,
    private val timeoutMs: Long = 8L,
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : FrequencyClient {

    private val log = LoggerFactory.getLogger(javaClass)
    private val stub = FrequencyGrpcKt.FrequencyCoroutineStub(channel)

    private val okTimer: Timer = newTimer(meterRegistry, "ok")
    private val timeoutTimer: Timer = newTimer(meterRegistry, "timeout")
    private val errorTimer: Timer = newTimer(meterRegistry, "error")

    override suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult {
        val request = EnrichRequest.newBuilder()
            .setUserId(userId)
            .addAllCampaignIds(campaignIds)
            .build()
        var nanos = 0L
        return try {
            val response = withContext(Dispatchers.IO) {
                var resp: com.github.robran.adserver.protocol.frequency.EnrichResponse
                nanos = measureNanoTime {
                    resp = withTimeout(timeoutMs) { stub.enrichForAuction(request) }
                }
                resp
            }
            okTimer.record(nanos, TimeUnit.NANOSECONDS)
            EnrichResult(
                freqCounts = response.freqCountsMap.toMap(),
                recentCategories = response.recentCategoriesList.toSet(),
            )
        } catch (e: TimeoutCancellationException) {
            timeoutTimer.record(nanos, TimeUnit.NANOSECONDS)
            log.debug("frequency.fail_open: timeout after {}ms", timeoutMs)
            EnrichResult(freqCounts = emptyMap(), recentCategories = emptySet())
        } catch (e: Throwable) {
            errorTimer.record(nanos, TimeUnit.NANOSECONDS)
            log.debug("frequency.fail_open: {}", e.javaClass.simpleName)
            EnrichResult(freqCounts = emptyMap(), recentCategories = emptySet())
        }
    }

    private fun newTimer(registry: MeterRegistry, outcome: String): Timer =
        Timer.builder(METRIC_NAME)
            .tag("outcome", outcome)
            .publishPercentileHistogram()
            .register(registry)

    companion object {
        const val METRIC_NAME = "frequency.grpc.duration"
    }
}
```

- [ ] **Step 2: Pass meter registry to `GrpcFrequencyClient` in `Application.kt`**

In `main()`:

```kotlin
    val frequencyClient = GrpcFrequencyClient(
        frequencyChannel,
        timeoutMs = config.frequency.timeoutMs,
        meterRegistry = meterRegistry,
    )
```

- [ ] **Step 3: Add a metrics assertion to `GrpcFrequencyClientTest`**

In `ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`, the existing `newClient(...)` helper builds a client without a registry. Modify it to accept an optional registry:

Replace the helper:

```kotlin
    private fun newClient(
        timeoutMs: Long = 8L,
        registry: io.micrometer.core.instrument.MeterRegistry =
            io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
    ): GrpcFrequencyClient {
        val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        return GrpcFrequencyClient(channel, timeoutMs, registry)
    }
```

Append a new `@Test`:

```kotlin
    @Test
    fun `records frequency_grpc_duration with outcome=ok on success`() = runTest {
        fakeBehavior = { _ -> EnrichResponse.getDefaultInstance() }
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val client = newClient(timeoutMs = 5_000L, registry = registry)
        client.enrich("u1", listOf("c1"))

        val ok = registry.timer("frequency.grpc.duration", "outcome", "ok")
        assertThat(ok.count()).isEqualTo(1L)
    }

    @Test
    fun `records frequency_grpc_duration with outcome=timeout on slow server`() = runTest {
        fakeBehavior = { _ ->
            kotlinx.coroutines.delay(50)
            EnrichResponse.getDefaultInstance()
        }
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val client = newClient(timeoutMs = 8L, registry = registry)
        client.enrich("u1", listOf("c1"))

        val to = registry.timer("frequency.grpc.duration", "outcome", "timeout")
        assertThat(to.count()).isEqualTo(1L)
    }

    @Test
    fun `records frequency_grpc_duration with outcome=error on server failure`() = runTest {
        fakeBehavior = { _ ->
            throw io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE)
        }
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val client = newClient(timeoutMs = 5_000L, registry = registry)
        client.enrich("u1", listOf("c1"))

        val err = registry.timer("frequency.grpc.duration", "outcome", "error")
        assertThat(err.count()).isEqualTo(1L)
    }
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.GrpcFrequencyClientTest"`
Expected: PASS, 7 tests green (4 existing + 3 new).

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL across all suites.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt
git commit -m "Phase 4a task 4: GrpcFrequencyClient frequency.grpc.duration tagged by outcome"
```

---

## Task 5: ad-server `kafka.producer.send.duration` on `KafkaEventEmitter`

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitter.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt`

The producer's `send()` returns immediately, but Kafka's callback fires on ack/error. Time from `producer.send()` call to the callback firing — that's the end-to-end producer latency. Tag by topic.

- [ ] **Step 1: Modify `KafkaEventEmitter.kt`**

Replace the file contents:

```kotlin
package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

interface EventEmitter {
    fun emitImpression(event: ImpressionEvent)
    fun emitAuctionResult(event: AuctionResultEvent)
}

object NoOpEventEmitter : EventEmitter {
    override fun emitImpression(event: ImpressionEvent) {}
    override fun emitAuctionResult(event: AuctionResultEvent) {}
}

class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : EventEmitter, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val impressionSendTimer: Timer = sendTimer(meterRegistry, config.topicImpressionEvents)
    private val auctionSendTimer: Timer = sendTimer(meterRegistry, config.topicAuctionResults)

    override fun emitImpression(event: ImpressionEvent) {
        send(impressionSendTimer, config.topicImpressionEvents, event.userId.toString(), event)
    }

    override fun emitAuctionResult(event: AuctionResultEvent) {
        send(auctionSendTimer, config.topicAuctionResults, event.userId.toString(), event)
    }

    private fun <T : Any> send(timer: Timer, topic: String, key: String, value: T) {
        val startNanos = System.nanoTime()
        emitterScope.launch {
            try {
                producer.send(ProducerRecord(topic, key, value)) { _, ex ->
                    val elapsed = System.nanoTime() - startNanos
                    timer.record(elapsed, TimeUnit.NANOSECONDS)
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", topic, ex.message)
                }
            } catch (e: Throwable) {
                val elapsed = System.nanoTime() - startNanos
                timer.record(elapsed, TimeUnit.NANOSECONDS)
                log.warn("kafka.emit.fail topic={} error={}", topic, e.message)
            }
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }

    companion object {
        const val METRIC_NAME = "kafka.producer.send.duration"

        private fun sendTimer(registry: MeterRegistry, topic: String): Timer =
            Timer.builder(METRIC_NAME)
                .tag("topic", topic)
                .publishPercentileHistogram()
                .register(registry)
    }
}
```

- [ ] **Step 2: Wire registry in `Application.kt`**

```kotlin
    val eventEmitter = com.github.robran.adserver.kafka.KafkaEventEmitter(
        kafkaProducer,
        config.kafka,
        meterRegistry = meterRegistry,
    )
```

- [ ] **Step 3: Add a metrics assertion to `KafkaEventEmitterTest`**

In `KafkaEventEmitterTest.kt`, modify the existing `setup()` to use a registry, and add a new test:

Find the `emitter = KafkaEventEmitter(producer, config)` line in `setup()`. Replace with:

```kotlin
        registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        emitter = KafkaEventEmitter(producer, config, meterRegistry = registry)
```

And add a `private lateinit var registry: io.micrometer.core.instrument.simple.SimpleMeterRegistry` field at the top.

Append a new `@Test`:

```kotlin
    @Timeout(value = 30)
    @Test
    fun `records kafka_producer_send_duration tagged by topic`() {
        emitter.emitImpression(
            ImpressionEvent.newBuilder()
                .setUserId("u1")
                .setCampaignId("c1")
                .setCreativeId("cre1")
                .setCategory("IAB1")
                .setPrice(1.0)
                .setTsMillis(1L)
                .build(),
        )
        producer.flush()
        // Wait for the async callback to record the timing.
        val deadline = System.currentTimeMillis() + 5_000
        var observed = 0L
        while (System.currentTimeMillis() < deadline && observed == 0L) {
            observed = registry.timer(
                "kafka.producer.send.duration",
                "topic",
                config.topicImpressionEvents,
            ).count()
            if (observed == 0L) Thread.sleep(50)
        }
        assertThat(observed).isEqualTo(1L)
    }
```

- [ ] **Step 4: Run tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.kafka.KafkaEventEmitterTest"`
Expected: PASS, 3 tests green (2 existing + 1 new).

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitter.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt
git commit -m "Phase 4a task 5: KafkaEventEmitter kafka.producer.send.duration tagged by topic"
```

---

## Task 6: ad-server Inventory Gauges

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/metrics/InventoryGauges.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`

Two gauges:
- `inventory.snapshot.size` — current campaign count
- `inventory.snapshot.age_seconds` — seconds since the snapshot was hydrated

The gauges are registered once at boot and re-evaluate on every Prometheus scrape.

- [ ] **Step 1: Write `InventoryGauges.kt`**

```kotlin
package com.github.robran.adserver.metrics

import com.github.robran.adserver.inventory.InventorySnapshot
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Gauges over the current inventory snapshot. The snapshot reference is held in an
 * [AtomicReference] so future hot-reload (Phase 5/6) can swap it in atomically.
 */
class InventoryGauges(snapshot: InventorySnapshot, registry: MeterRegistry) {
    private val snapshotRef = AtomicReference(snapshot)

    init {
        Gauge.builder("inventory.snapshot.size") { snapshotRef.get().size.toDouble() }
            .description("Number of campaigns currently in the in-memory inventory snapshot")
            .register(registry)

        Gauge.builder("inventory.snapshot.age_seconds") {
            val loadedAt = snapshotRef.get().loadedAt
            (Instant.now().epochSecond - loadedAt.epochSecond).toDouble().coerceAtLeast(0.0)
        }
            .description("Seconds since the inventory snapshot was last hydrated")
            .register(registry)
    }

    /** Swap in a freshly hydrated snapshot. Future hot-reload feature. */
    fun update(snapshot: InventorySnapshot) {
        snapshotRef.set(snapshot)
    }
}
```

- [ ] **Step 2: Register gauges in `Application.kt`**

After `val snapshot = ...load()` and before `val pipeline = buildPipeline(...)`:

```kotlin
    com.github.robran.adserver.metrics.InventoryGauges(snapshot, meterRegistry)
```

(The gauges register themselves in their `init` block; we just need to construct one instance and let it stay alive — captured by JVM root via the registry's reference. For safety, assign to a `val` so it's not eligible for GC.)

```kotlin
    @Suppress("UNUSED_VARIABLE")
    val inventoryGauges = com.github.robran.adserver.metrics.InventoryGauges(snapshot, meterRegistry)
```

- [ ] **Step 3: Verify**

Run: `./gradlew :ad-server:build`
Expected: BUILD SUCCESSFUL.

There's no dedicated test — the gauges are simple enough that the smoke test covers them (the `/metrics` scrape will include `inventory_snapshot_size` once Task 2 + Task 6 are both in place).

- [ ] **Step 4: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/metrics/InventoryGauges.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt
git commit -m "Phase 4a task 6: inventory.snapshot.size and age_seconds gauges"
```

---

## Task 7: frequency-service `MeterRegistryFactory` + `MetricsHttpServer`

**Files:**
- Modify: `frequency-service/build.gradle.kts`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt`
- Modify: `frequency-service/src/main/resources/application.conf`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/metrics/MeterRegistryFactory.kt`
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/metrics/MetricsHttpServer.kt`
- Create: `frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/metrics/MetricsHttpServerTest.kt`

The frequency-service is gRPC-only; we serve `/metrics` via Java's built-in `com.sun.net.httpserver.HttpServer` on a separate port (default 9091).

- [ ] **Step 1: Add Micrometer deps to `frequency-service/build.gradle.kts`**

In the existing `implementation` section:

```kotlin
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)
```

- [ ] **Step 2: Add `MetricsConfig` + `metrics` block**

In `AppConfig.kt`, add:

```kotlin
data class MetricsConfig(
    val enabled: Boolean,
    val port: Int,
    val commonTags: Map<String, String>,
)
```

Append to `AppConfig`:

```kotlin
data class AppConfig(
    val server: ServerConfig,
    val redis: RedisConfig,
    val metrics: MetricsConfig,
) { ... }
```

In `application.conf`, append inside `frequency { ... }`:

```hocon
    metrics {
        enabled = true
        enabled = ${?METRICS_ENABLED}
        port = 9091
        port = ${?METRICS_PORT}
        commonTags {
            service = "frequency-service"
            env = "local"
            env = ${?METRICS_ENV}
        }
    }
```

- [ ] **Step 3: Write `MeterRegistryFactory.kt`**

`frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/metrics/MeterRegistryFactory.kt`:

```kotlin
package com.github.robran.adserver.frequency.metrics

import com.github.robran.adserver.frequency.MetricsConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

object MeterRegistryFactory {
    fun build(config: MetricsConfig): PrometheusMeterRegistry {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        config.commonTags.forEach { (k, v) -> registry.config().commonTags(k, v) }
        bindJvmMetrics(registry)
        return registry
    }

    private fun bindJvmMetrics(registry: MeterRegistry) {
        ClassLoaderMetrics().bindTo(registry)
        JvmMemoryMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        JvmThreadMetrics().bindTo(registry)
        ProcessorMetrics().bindTo(registry)
        UptimeMetrics().bindTo(registry)
    }
}
```

- [ ] **Step 4: Write `MetricsHttpServer.kt`**

```kotlin
package com.github.robran.adserver.frequency.metrics

import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * Tiny HTTP server backing /metrics for the frequency-service. We use Java's built-in
 * [com.sun.net.httpserver.HttpServer] rather than Ktor because frequency-service is gRPC-only —
 * adding a full Ktor stack just for /metrics is overkill.
 */
class MetricsHttpServer(
    private val registry: PrometheusMeterRegistry,
    private val port: Int,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private var server: HttpServer? = null

    fun start() {
        val s = HttpServer.create(InetSocketAddress(port), 0)
        s.createContext("/metrics") { exchange ->
            val body = registry.scrape().toByteArray()
            exchange.responseHeaders.set("Content-Type", "text/plain; version=0.0.4")
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        s.createContext("/healthz") { exchange ->
            val body = "ok\n".toByteArray()
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        s.executor = null
        s.start()
        server = s
        log.info("MetricsHttpServer listening on port {}", port)
    }

    override fun close() {
        server?.stop(0)
        server = null
    }
}
```

- [ ] **Step 5: Wire into `Application.kt`**

Read the current `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`, then update:

```kotlin
package com.github.robran.adserver.frequency

import com.github.robran.adserver.frequency.metrics.MeterRegistryFactory
import com.github.robran.adserver.frequency.metrics.MetricsHttpServer
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val meterRegistry = MeterRegistryFactory.build(config.metrics)

    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis, meterRegistry)

    val server = NettyServerBuilder.forPort(config.server.port)
        .addService(service)
        .build()
        .start()

    val metricsServer = MetricsHttpServer(meterRegistry, config.metrics.port)
    metricsServer.start()

    log.info(
        "frequency-service listening on port {} (redis={}) — /metrics on port {}",
        config.server.port,
        config.redis.url,
        config.metrics.port,
    )

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down frequency-service")
            server.shutdown()
            metricsServer.close()
            redis.close()
            meterRegistry.close()
        },
    )

    server.awaitTermination()
}
```

(Note: `EnrichService` constructor now takes a registry — Task 8 implements that. For now, until Task 8 is done, you may need to temporarily pass `meterRegistry` as an extra arg or keep `EnrichService(redis)` as-is. Easiest: do this Task 7 and Task 8 together as one PR even though they commit separately.)

For this Task 7's commit, keep the EnrichService call as `EnrichService(redis)` — Task 8 will update it. The `meterRegistry` is built and only used by `MetricsHttpServer`. Update Step 5's Application.kt to:

```kotlin
    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis)  // Task 8 will add meterRegistry
```

- [ ] **Step 6: Write the test**

`frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/metrics/MetricsHttpServerTest.kt`:

```kotlin
package com.github.robran.adserver.frequency.metrics

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import com.github.robran.adserver.frequency.MetricsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.HttpURLConnection
import java.net.URL

class MetricsHttpServerTest {

    private lateinit var registry: io.micrometer.prometheusmetrics.PrometheusMeterRegistry
    private lateinit var server: MetricsHttpServer
    private val port = freePort()

    @BeforeEach
    fun setup() {
        registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, port = port, commonTags = mapOf("service" to "frequency-service")),
        )
        server = MetricsHttpServer(registry, port)
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.close()
        registry.close()
    }

    @Test
    fun `GET /metrics returns 200 with Prometheus text`() {
        val conn = URL("http://localhost:$port/metrics").openConnection() as HttpURLConnection
        try {
            assertThat(conn.responseCode).isEqualTo(200)
            val body = conn.inputStream.bufferedReader().readText()
            assertThat(body).contains("# TYPE")
            assertThat(body).contains("jvm_memory_used_bytes")
        } finally {
            conn.disconnect()
        }
    }

    @Test
    fun `GET /healthz returns 200`() {
        val conn = URL("http://localhost:$port/healthz").openConnection() as HttpURLConnection
        try {
            assertThat(conn.responseCode).isEqualTo(200)
            assertThat(conn.inputStream.bufferedReader().readText().trim()).isEqualTo("ok")
        } finally {
            conn.disconnect()
        }
    }

    private fun freePort(): Int {
        java.net.ServerSocket(0).use { return it.localPort }
    }
}
```

- [ ] **Step 7: Run the tests**

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.metrics.MetricsHttpServerTest"`
Expected: PASS, 2 tests green.

Run: `./gradlew :frequency-service:test`
Expected: BUILD SUCCESSFUL — all earlier tests still pass.

- [ ] **Step 8: Commit**

```bash
git add frequency-service/build.gradle.kts \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt \
        frequency-service/src/main/resources/application.conf \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/metrics/MeterRegistryFactory.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/metrics/MetricsHttpServer.kt \
        frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/metrics/MetricsHttpServerTest.kt
git commit -m "Phase 4a task 7: frequency-service MeterRegistryFactory + MetricsHttpServer"
```

---

## Task 8: frequency-service `redis.lookup.duration` Timer

**Files:**
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`

`EnrichService.enrichForAuction` does TWO Redis hits per RPC: an `MGET` (freq counters) and a `ZRANGEBYSCORE` (winhistory). The `redis.lookup.duration` Timer wraps both, tagged `op` ∈ {mget_freq, zrange_winhistory}.

- [ ] **Step 1: Update `EnrichService.kt`**

```kotlin
package com.github.robran.adserver.frequency

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.EnrichResponse
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

class EnrichService(
    private val redis: RedisClient,
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : FrequencyGrpcKt.FrequencyCoroutineImplBase() {

    private val log = LoggerFactory.getLogger(javaClass)

    private val mgetTimer: Timer = lookupTimer(meterRegistry, "mget_freq")
    private val zrangeTimer: Timer = lookupTimer(meterRegistry, "zrange_winhistory")

    override suspend fun enrichForAuction(request: EnrichRequest): EnrichResponse {
        val userId = request.userId
        require(userId.isNotEmpty()) { "user_id is required" }
        val campaignIds = request.campaignIdsList

        val freqCounts = if (campaignIds.isEmpty()) {
            emptyMap()
        } else {
            val freqKeys = campaignIds.map { "freq:$userId:$it" }
            var values: List<String?>
            val nanos = measureNanoTime { values = redis.mget(freqKeys) }
            mgetTimer.record(nanos, TimeUnit.NANOSECONDS)
            campaignIds.zip(values).mapNotNull { (campaignId, raw) ->
                val count = raw?.toIntOrNull() ?: return@mapNotNull null
                if (count <= 0) null else campaignId to count
            }.toMap()
        }

        val winhistoryKey = "winhistory:$userId"
        var rawWins: List<String>
        val nanos = measureNanoTime {
            rawWins = redis.zrangeByScore(winhistoryKey, 0.0, Double.POSITIVE_INFINITY)
        }
        zrangeTimer.record(nanos, TimeUnit.NANOSECONDS)
        val recentCategories = rawWins.mapNotNullTo(mutableSetOf()) { entry ->
            val sep = entry.indexOf(':')
            if (sep < 0) null else entry.substring(sep + 1)
        }

        return EnrichResponse.newBuilder()
            .putAllFreqCounts(freqCounts)
            .addAllRecentCategories(recentCategories)
            .build()
    }

    companion object {
        const val METRIC_NAME = "redis.lookup.duration"

        private fun lookupTimer(registry: MeterRegistry, op: String): Timer =
            Timer.builder(METRIC_NAME)
                .tag("op", op)
                .publishPercentileHistogram()
                .register(registry)
    }
}
```

- [ ] **Step 2: Pass registry through `Application.kt`**

```kotlin
    val service = EnrichService(redis, meterRegistry)
```

- [ ] **Step 3: Update existing `EnrichServiceIntegrationTest`** (if any test instantiates `EnrichService(redis)` directly, it still compiles since the second arg has a default)

Verify by running:

Run: `./gradlew :frequency-service:test`
Expected: BUILD SUCCESSFUL — all 11 existing tests still pass.

- [ ] **Step 4: Add a metrics test**

In `EnrichServiceIntegrationTest.kt`, add a new test (using the existing `redisClient` + `stub`):

```kotlin
    @Test
    fun `enrichForAuction records redis_lookup_duration with op tags`() = runTest {
        // Pre-populate Redis to ensure both ops touch real data.
        redisClient.set("freq:user-metrics:c1", "1")
        redisClient.zadd("winhistory:user-metrics", "c1:IAB1" to 1.0)

        // Use a custom EnrichService with a recording registry, in-process gRPC.
        val recordingRegistry = io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
            io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT,
        )
        val recordingService = EnrichService(redisClient, recordingRegistry)
        val testServerName = io.grpc.inprocess.InProcessServerBuilder.generateName()
        val testServer = io.grpc.inprocess.InProcessServerBuilder.forName(testServerName)
            .directExecutor()
            .addService(recordingService)
            .build()
            .start()
        val testChannel = io.grpc.inprocess.InProcessChannelBuilder.forName(testServerName)
            .directExecutor()
            .build()
        val testStub = com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt.FrequencyCoroutineStub(testChannel)

        try {
            testStub.enrichForAuction(
                com.github.robran.adserver.protocol.frequency.EnrichRequest.newBuilder()
                    .setUserId("user-metrics")
                    .addCampaignIds("c1")
                    .build(),
            )
            val mget = recordingRegistry.timer("redis.lookup.duration", "op", "mget_freq")
            val zrange = recordingRegistry.timer("redis.lookup.duration", "op", "zrange_winhistory")
            assertThat(mget.count()).isEqualTo(1L)
            assertThat(zrange.count()).isEqualTo(1L)
        } finally {
            testChannel.shutdownNow()
            testServer.shutdownNow()
            recordingRegistry.close()
        }
    }
```

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.EnrichServiceIntegrationTest"`
Expected: PASS, 6 tests green (5 existing + 1 new).

- [ ] **Step 5: Commit**

```bash
git add frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt \
        frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/EnrichServiceIntegrationTest.kt
git commit -m "Phase 4a task 8: EnrichService redis.lookup.duration tagged by op"
```

---

## Task 9: docker-compose Prometheus + Grafana

**Files:**
- Modify: `docker-compose.yml`
- Create: `infra/prometheus/prometheus.yml`
- Create: `infra/grafana/provisioning/datasources/prometheus.yml`
- Create: `infra/grafana/provisioning/dashboards/dashboards.yml`

(The dashboard JSON itself is Task 10 because it's heavy.)

- [ ] **Step 1: Create `infra/prometheus/prometheus.yml`**

```bash
mkdir -p infra/prometheus
mkdir -p infra/grafana/provisioning/datasources
mkdir -p infra/grafana/provisioning/dashboards
mkdir -p infra/grafana/dashboards
```

`infra/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: ad-server
    static_configs:
      - targets: ["host.docker.internal:8080"]
    metrics_path: /metrics

  - job_name: frequency-service
    static_configs:
      - targets: ["host.docker.internal:9091"]
    metrics_path: /metrics
```

(`host.docker.internal` is how compose-network containers reach the host's network on Docker Desktop. The JVM apps run on the host via `./gradlew run`; Prometheus runs in a container.)

- [ ] **Step 2: Create `infra/grafana/provisioning/datasources/prometheus.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 3: Create `infra/grafana/provisioning/dashboards/dashboards.yml`**

```yaml
apiVersion: 1
providers:
  - name: kotlin_ad_server
    orgId: 1
    folder: ""
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /etc/grafana/dashboards
```

- [ ] **Step 4: Add Prometheus + Grafana services to `docker-compose.yml`**

Read the current `docker-compose.yml`. After the existing `flink-taskmanager` service and BEFORE the `volumes:` block at the bottom, add:

```yaml
  prometheus:
    image: prom/prometheus:v2.55.0
    container_name: kotlin-ad-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:11.3.0
    container_name: kotlin-ad-grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_AUTH_BASIC_ENABLED: "false"
    volumes:
      - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./infra/grafana/dashboards:/etc/grafana/dashboards:ro
```

- [ ] **Step 5: Verify the compose file**

Run: `docker compose config --quiet`
Expected: exit 0 (no errors).

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml infra/
git commit -m "Phase 4a task 9: docker-compose Prometheus + Grafana with provisioning"
```

---

## Task 10: Grafana Dashboard JSON

**Files:**
- Create: `infra/grafana/dashboards/kotlin_ad_server.json`

A minimal but useful dashboard with three rows:
- Row 1: Request latency (p50/p95/p99 from `adserver.request.duration`)
- Row 2: Stage breakdown (timeseries from `adserver.stage.duration`, tagged by stage)
- Row 3: Funnel (max `adserver.candidates.surviving` per stage)

- [ ] **Step 1: Write the dashboard JSON**

`infra/grafana/dashboards/kotlin_ad_server.json`:

```json
{
  "title": "kotlin_ad_server",
  "uid": "kotlin-ad-server",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "Request latency (filled outcomes)",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.5, sum by (le) (rate(adserver_request_duration_seconds_bucket{outcome=\"filled\"}[1m])))",
          "legendFormat": "p50"
        },
        {
          "refId": "B",
          "expr": "histogram_quantile(0.95, sum by (le) (rate(adserver_request_duration_seconds_bucket{outcome=\"filled\"}[1m])))",
          "legendFormat": "p95"
        },
        {
          "refId": "C",
          "expr": "histogram_quantile(0.99, sum by (le) (rate(adserver_request_duration_seconds_bucket{outcome=\"filled\"}[1m])))",
          "legendFormat": "p99"
        }
      ]
    },
    {
      "id": 2,
      "title": "Per-stage p95 latency",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 8 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by (le, stage) (rate(adserver_stage_duration_seconds_bucket[1m])))",
          "legendFormat": "{{stage}}"
        }
      ]
    },
    {
      "id": 3,
      "title": "Auction funnel (avg candidates surviving)",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 16 },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "rate(adserver_candidates_surviving_sum[1m]) / rate(adserver_candidates_surviving_count[1m])",
          "legendFormat": "{{stage}}"
        }
      ]
    },
    {
      "id": 4,
      "title": "Frequency RPC latency by outcome",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by (le, outcome) (rate(frequency_grpc_duration_seconds_bucket[1m])))",
          "legendFormat": "{{outcome}}"
        }
      ]
    },
    {
      "id": 5,
      "title": "JVM heap used",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 24 },
      "fieldConfig": { "defaults": { "unit": "decbytes" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "sum by (service) (jvm_memory_used_bytes{area=\"heap\"})",
          "legendFormat": "{{service}}"
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add infra/grafana/dashboards/kotlin_ad_server.json
git commit -m "Phase 4a task 10: Grafana dashboard JSON (latency + stages + funnel + JVM)"
```

---

## Task 11: Smoke Test the Full Stack Manually (no commit, no test code — just verification)

This is a one-time human verification, not a TDD task. Recipe:

1. `docker compose up -d`
2. `./scripts/kafka-init-topics.sh`
3. `./gradlew :frequency-service:run &` (terminal 1)
4. `./gradlew :ad-server:run &` (terminal 2)
5. `curl -X POST http://localhost:8080/openrtb/bid -H 'Content-Type: application/json' -d '{"id":"d1","imp":[{"id":"1","banner":{"w":300,"h":250}}],"user":{"id":"smoke"}}'`
6. `curl http://localhost:8080/metrics | grep adserver_request_duration` — should show timer histogram lines
7. `curl http://localhost:9091/metrics | grep redis_lookup_duration` — should show frequency-service redis ops
8. Open `http://localhost:9090/targets` — both ad-server and frequency-service should be `UP`
9. Open `http://localhost:3000/d/kotlin-ad-server` — dashboard should be visible with metrics flowing

If any step fails, file an issue or fix in a follow-up commit. **No commit for this task** — it's manual verification only. Document the recipe in the README (Task 12).

---

## Task 12: README + smoke-test update

**Files:**
- Modify: `README.md`
- Modify: `scripts/smoke-test.sh`

- [ ] **Step 1: Mark Phase 4 partial in README**

Find the `## Status` block. Update the Phase 4 line:

```markdown
- 🟡 **Phase 4 — Observability** (4a metrics ✅; 4b tracing pending) (this commit)
```

- [ ] **Step 2: Add observability section to README**

Append a new section before `## Smoke test`:

```markdown
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
```

- [ ] **Step 3: Update smoke-test success line**

In `scripts/smoke-test.sh`:

```bash
echo "==> Phase 1+2+3+4a smoke test PASSED. Metrics surfaces wired (Prometheus/Grafana stack via docker compose)."
```

- [ ] **Step 4: Run the smoke test**

Run: `./scripts/smoke-test.sh`
Expected: BUILD SUCCESSFUL ending with the new line.

- [ ] **Step 5: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 4a task 12: README + smoke-test update for metrics"
```

---

## Phase 4a Done

Working software:
- Both services expose `/metrics` (ad-server via Ktor route, frequency-service via `com.sun.net.httpserver.HttpServer`)
- All 8 spec metrics emit on the request path
- Prometheus + Grafana provisioned via docker-compose with a 5-panel dashboard JSON
- 13 new unit/integration tests covering metric emission

**Next:** Phase 4b — OpenTelemetry tracing across services with W3C trace propagation, Jaeger backend, structured JSON logging with MDC trace_id injection. Will need its own plan.
