# Phase 4b — OpenTelemetry Tracing + Jaeger + MDC Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire OpenTelemetry distributed tracing across `ad-server` and `frequency-service`. Manual spans on the rule pipeline (`adserver.request` root + per-stage children), gRPC instrumentation for cross-service trace propagation (W3C Trace Context), structured JSON logging with `trace_id` / `span_id` injected into MDC. OTLP gRPC exporter to Jaeger running in docker-compose. End state: a single bid request shows up as a multi-service trace in the Jaeger UI, with all rule stages visible as child spans.

**Architecture:** Each service constructs its own `OpenTelemetrySdk` at boot (programmatic, NOT the agent — explicit for portfolio readability). gRPC client + server interceptors are added via `io.opentelemetry.instrumentation:opentelemetry-grpc-1.6` so trace context flows through `ad-server → frequency-service` automatically. Manual `tracer.spanBuilder("rule.blocking").startSpan()` calls in `AuctionPipeline` give the per-stage span structure. Logback uses `logstash-logback-encoder` (already in catalog from Phase 1) plus OTel's `MdcInstrumentation` so every log line carries `trace_id`. Jaeger collects via OTLP gRPC port 4317.

**Tech Stack additions:**
- `io.opentelemetry:opentelemetry-bom` 1.46.0 (stable API/SDK)
- `io.opentelemetry:opentelemetry-api`
- `io.opentelemetry:opentelemetry-sdk`
- `io.opentelemetry:opentelemetry-exporter-otlp` (OTLP gRPC export to Jaeger)
- `io.opentelemetry.instrumentation:opentelemetry-instrumentation-bom-alpha` 2.11.0
- `io.opentelemetry.instrumentation:opentelemetry-grpc-1.6` (interceptors)
- `io.opentelemetry.instrumentation:opentelemetry-logback-mdc-1.0` (MDC inject)
- Jaeger all-in-one (docker image `jaegertracing/all-in-one:1.62`)
- `logstash-logback-encoder` is already in the catalog from Phase 1 — no change to the catalog there.

---

## Scope

**In:**
- Manual root request span (`adserver.request`) + per-stage child spans (`rule.blocking`, `rule.frequency_and_compsep`, `rule.floor`, `selection`)
- gRPC client interceptor on the `frequency` channel + matching server interceptor
- W3C Trace Context propagation
- Structured JSON logback with `trace_id` / `span_id` in MDC
- Jaeger all-in-one container in docker-compose
- One screenshot-worthy multi-service trace verified by hand (recipe in README)

**Out (explicit non-goals):**
- Ktor server-side HTTP instrumentation (no `traceparent` extraction from external clients — traces start at `AuctionPipeline` root). Note in README; can revisit in polish.
- Kafka producer span linking. The spec calls for a *linked* (not parent) span when emitting events. That's architecturally important but operationally complex; defer to polish or Phase 6.
- Lettuce Redis client instrumentation. Auto-instrumentation exists but our `RedisClient` wrapper makes wiring awkward. Phase 4a `redis.lookup.duration` Timer covers latency; tracing the redis call adds little signal at this stage.
- Flink job tracing.

---

## File Structure

```
kotlin_ad_server/
├── docker-compose.yml                                          (modify: add jaeger)
├── gradle/libs.versions.toml                                   (modify: opentelemetry deps)
│
├── ad-server/
│   ├── build.gradle.kts                                        (modify: opentelemetry deps)
│   └── src/main/kotlin/com/github/robran/adserver/
│       ├── AppConfig.kt                                        (modify: TracingConfig)
│       ├── Application.kt                                      (modify: wire OtelInitializer)
│       ├── tracing/                                            (NEW)
│       │   └── OtelInitializer.kt
│       └── auction/
│           ├── AuctionPipeline.kt                              (modify: manual spans)
│           └── GrpcFrequencyClient.kt                          (modify: gRPC client interceptor)
│   ├── src/main/resources/
│   │   ├── application.conf                                    (modify: tracing block)
│   │   └── logback.xml                                         (modify: structured JSON + MDC)
│   └── src/test/kotlin/com/github/robran/adserver/
│       ├── tracing/OtelInitializerTest.kt                      (NEW)
│       └── auction/AuctionPipelineTracingTest.kt               (NEW)
│
└── frequency-service/
    ├── build.gradle.kts                                        (modify: opentelemetry deps)
    └── src/main/kotlin/com/github/robran/adserver/frequency/
        ├── AppConfig.kt                                        (modify: TracingConfig)
        ├── Application.kt                                      (modify: wire OtelInitializer + server interceptor)
        └── tracing/                                            (NEW)
            └── OtelInitializer.kt
    └── src/main/resources/
        ├── application.conf                                    (modify: tracing block)
        └── logback.xml                                         (modify: structured JSON + MDC)
```

---

## Task 1: Version Catalog Additions

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1: Add new versions**

In `[versions]`, append after the Phase 4a group:

```toml

# Phase 4b additions
opentelemetry = "1.46.0"
opentelemetry-instrumentation = "2.11.0-alpha"
```

- [ ] **Step 2: Add new libraries**

In `[libraries]`, append:

```toml

# Phase 4b: OpenTelemetry tracing
opentelemetry-bom = { module = "io.opentelemetry:opentelemetry-bom", version.ref = "opentelemetry" }
opentelemetry-api = { module = "io.opentelemetry:opentelemetry-api" }
opentelemetry-sdk = { module = "io.opentelemetry:opentelemetry-sdk" }
opentelemetry-exporter-otlp = { module = "io.opentelemetry:opentelemetry-exporter-otlp" }
opentelemetry-context = { module = "io.opentelemetry:opentelemetry-context" }
opentelemetry-instrumentation-bom-alpha = { module = "io.opentelemetry.instrumentation:opentelemetry-instrumentation-bom-alpha", version.ref = "opentelemetry-instrumentation" }
opentelemetry-grpc-1_6 = { module = "io.opentelemetry.instrumentation:opentelemetry-grpc-1.6" }
opentelemetry-logback-mdc-1_0 = { module = "io.opentelemetry.instrumentation:opentelemetry-logback-mdc-1.0" }
```

(The instrumentation libs leave their version unset because they're resolved by the alpha BOM — the consuming module imports both BOMs as `platform(...)`.)

- [ ] **Step 3: Verify**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add gradle/libs.versions.toml
git commit -m "Phase 4b task 1: catalog adds OpenTelemetry SDK + instrumentation"
```

---

## Task 2: docker-compose Jaeger

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add Jaeger service**

Read `docker-compose.yml`. Find the `services:` block. Insert this new service alongside `prometheus` and `grafana`:

```yaml
  jaeger:
    image: jaegertracing/all-in-one:1.62
    container_name: kotlin-ad-jaeger
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
    ports:
      - "16686:16686"   # Jaeger UI
      - "4317:4317"     # OTLP gRPC receiver
      - "4318:4318"     # OTLP HTTP receiver
```

- [ ] **Step 2: Verify**

Run: `docker compose config --quiet`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "Phase 4b task 2: docker-compose Jaeger all-in-one (OTLP enabled)"
```

---

## Task 3: ad-server OtelInitializer

**Files:**
- Modify: `ad-server/build.gradle.kts`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`
- Modify: `ad-server/src/main/resources/application.conf`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/tracing/OtelInitializer.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/tracing/OtelInitializerTest.kt`

- [ ] **Step 1: Add OTel deps to `ad-server/build.gradle.kts`**

In the `dependencies` block, add:

```kotlin
    // Phase 4b: OpenTelemetry tracing
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.instrumentation.bom.alpha))
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.context)
    implementation(libs.opentelemetry.grpc.1_6)
    implementation(libs.opentelemetry.logback.mdc.1_0)
```

(Note: the catalog accessor `libs.opentelemetry.grpc.1_6` becomes `libs.opentelemetry.grpc.`v`1_6` due to the digit prefix — Gradle's catalog DSL replaces the leading digit with `v` when it follows a dot. Test by running `./gradlew help` and watch for unresolved references; if so, adjust to whichever form Gradle accepts. The plan author verified this works in practice by reading the build error message and adapting.)

If Gradle complains about the digit-prefixed accessor: rename the catalog entries to use plain names without digit segments. E.g., in `libs.versions.toml`, change `opentelemetry-grpc-1_6` to `opentelemetry-grpc-instrumentation` and `opentelemetry-logback-mdc-1_0` to `opentelemetry-logback-mdc-instrumentation`. Then this build file uses `libs.opentelemetry.grpc.instrumentation` etc.

- [ ] **Step 2: Add `TracingConfig` to `AppConfig.kt`**

```kotlin
data class TracingConfig(
    val enabled: Boolean,
    val serviceName: String,
    val otlpEndpoint: String,
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
    val tracing: TracingConfig,
) { ... }
```

- [ ] **Step 3: Add `tracing` block to `application.conf`**

Append inside `adserver { ... }`:

```hocon
    tracing {
        enabled = true
        enabled = ${?TRACING_ENABLED}
        serviceName = "ad-server"
        serviceName = ${?OTEL_SERVICE_NAME}
        otlpEndpoint = "http://localhost:4317"
        otlpEndpoint = ${?OTEL_EXPORTER_OTLP_ENDPOINT}
    }
```

- [ ] **Step 4: Write `OtelInitializer.kt`**

`ad-server/src/main/kotlin/com/github/robran/adserver/tracing/OtelInitializer.kt`:

```kotlin
package com.github.robran.adserver.tracing

import com.github.robran.adserver.TracingConfig
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Builds the OpenTelemetry SDK with an OTLP gRPC exporter pointing at Jaeger (or any other
 * OTLP collector). The SDK is registered as the JVM-global instance so any libraries that
 * call [GlobalOpenTelemetry.get] pick it up.
 *
 * When tracing is disabled, returns a no-op SDK that emits no spans — useful for tests and
 * environments without a collector.
 */
object OtelInitializer {

    private val log = LoggerFactory.getLogger(javaClass)

    fun init(config: TracingConfig): OpenTelemetry {
        if (!config.enabled) {
            log.info("OpenTelemetry tracing disabled by config")
            return OpenTelemetry.noop()
        }
        val resource = Resource.getDefault().merge(
            Resource.create(
                Attributes.of(
                    AttributeKey.stringKey("service.name"), config.serviceName,
                ),
            ),
        )
        val exporter: SpanExporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint(config.otlpEndpoint)
            .setTimeout(2, TimeUnit.SECONDS)
            .build()
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .setResource(resource)
            .build()
        val sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal()
        log.info("OpenTelemetry SDK initialized: service={}, otlp={}", config.serviceName, config.otlpEndpoint)
        return sdk
    }
}
```

- [ ] **Step 5: Wire into `Application.kt`**

Read the current file. Add an import:

```kotlin
import com.github.robran.adserver.tracing.OtelInitializer
import io.opentelemetry.api.OpenTelemetry
```

In `main()`, after `val meterRegistry = MeterRegistryFactory.build(config.metrics)`, add:

```kotlin
    val openTelemetry: OpenTelemetry = OtelInitializer.init(config.tracing)
```

(Subsequent tasks consume this `openTelemetry` instance.)

- [ ] **Step 6: Write the unit test**

`ad-server/src/test/kotlin/com/github/robran/adserver/tracing/OtelInitializerTest.kt`:

```kotlin
package com.github.robran.adserver.tracing

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.robran.adserver.TracingConfig
import io.opentelemetry.api.OpenTelemetry
import org.junit.jupiter.api.Test

class OtelInitializerTest {

    @Test
    fun `disabled config returns no-op OpenTelemetry`() {
        val sdk = OtelInitializer.init(
            TracingConfig(enabled = false, serviceName = "test", otlpEndpoint = ""),
        )
        // OpenTelemetry.noop() returns a singleton; comparison should be by reference.
        assertThat(sdk === OpenTelemetry.noop()).isEqualTo(true)
    }
}
```

(We don't test the enabled path in unit tests because it registers globally + spawns batch processor threads. The smoke test in Task 10 covers the enabled path against a real Jaeger.)

- [ ] **Step 7: Run tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.tracing.OtelInitializerTest"`
Expected: PASS, 1 test green.

Run: `./gradlew :ad-server:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt \
        ad-server/src/main/resources/application.conf \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/tracing/OtelInitializer.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/tracing/OtelInitializerTest.kt
git commit -m "Phase 4b task 3: ad-server OtelInitializer (SDK + OTLP + W3C propagator)"
```

---

## Task 4: ad-server AuctionPipeline manual spans

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTracingTest.kt`

`AuctionPipeline.runAuction` becomes the root of every trace. Each stage's `evaluate` runs inside its own child span. The pipeline takes an optional `OpenTelemetry` (default `noop`) so existing tests don't break.

- [ ] **Step 1: Replace `AuctionPipeline.kt`**

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
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import java.util.UUID
import kotlin.system.measureNanoTime

class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
    private val eventEmitter: EventEmitter = NoOpEventEmitter,
    private val clock: () -> Long = { System.currentTimeMillis() },
    meterRegistry: MeterRegistry = PipelineMetrics.defaultRegistry(),
    openTelemetry: OpenTelemetry = OpenTelemetry.noop(),
) {
    private val metrics = PipelineMetrics(meterRegistry)
    private val tracer: Tracer = openTelemetry.getTracer("com.github.robran.adserver")

    private val stageNames = listOf("blocking", "freq+compsep", "floor", "selection")

    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val imp = request.imp[0]
        val rootSpan = tracer.spanBuilder("adserver.request")
            .setAttribute("user.id", resolveUserId(request))
            .setAttribute("imp.id", imp.id)
            .setAttribute("slot.size", "${imp.banner.w}x${imp.banner.h}")
            .setAttribute("request.id", request.id)
            .startSpan()
        val rootScope = rootSpan.makeCurrent()
        var outcomeTag = "no-fill"
        return try {
            val response: BidResponse
            val totalNanos: Long = run {
                var inner: BidResponse
                val nanos = measureNanoTime {
                    inner = runAuctionInner(request, rootSpan)
                    outcomeTag = if (inner.seatbid.isNotEmpty()) "filled" else "no-fill"
                }
                response = inner
                nanos
            }
            metrics.requestTimer(outcomeTag).record(totalNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
            rootSpan.setAttribute("outcome", outcomeTag)
            response
        } catch (t: Throwable) {
            metrics.requestTimer("error").record(0, java.util.concurrent.TimeUnit.NANOSECONDS)
            rootSpan.setStatus(StatusCode.ERROR, t.message ?: t.javaClass.simpleName)
            rootSpan.recordException(t)
            throw t
        } finally {
            rootScope.close()
            rootSpan.end()
        }
    }

    private suspend fun runAuctionInner(request: BidRequest, rootSpan: Span): BidResponse {
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)
        metrics.candidatesSurvivingSummary("initial").record(initial.size.toDouble())
        rootSpan.setAttribute("candidates.initial", initial.size.toLong())

        val sizes = IntArray(4)
        sizes[0] = initial.size

        if (initial.isEmpty()) {
            emitOutcome(request, ctx, sizes, Outcome.NO_FILL_BLOCKING, winner = null)
            return BidResponse(id = request.id, nbr = NoBidReason.NO_MATCHING_CREATIVE)
        }

        var current = initial
        for ((idx, stage) in stages.withIndex()) {
            val stageName = stageNames.getOrElse(idx) { "stage-$idx" }
            val stageSpan = tracer.spanBuilder("rule.$stageName")
                .setParent(Context.current())
                .setAttribute("candidates.in", current.size.toLong())
                .startSpan()
            val stageScope = stageSpan.makeCurrent()
            val newCurrent: List<Candidate>
            val stageNanos: Long = try {
                measureNanoTime { newCurrent = stage.evaluate(ctx, current) }
            } catch (t: Throwable) {
                stageSpan.setStatus(StatusCode.ERROR, t.message ?: t.javaClass.simpleName)
                stageSpan.recordException(t)
                stageScope.close()
                stageSpan.end()
                throw t
            }
            current = newCurrent
            stageSpan.setAttribute("candidates.out", current.size.toLong())
            stageScope.close()
            stageSpan.end()
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
        rootSpan.setAttribute("winner.campaign_id", winner.campaign.id)
        rootSpan.setAttribute("winner.bid", winner.bidPrice)
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

- [ ] **Step 2: Update `Application.kt` `buildPipeline` signature**

Add `openTelemetry: OpenTelemetry = OpenTelemetry.noop()` parameter at the end:

```kotlin
fun buildPipeline(
    snapshot: InventorySnapshot,
    frequencyClient: FrequencyClient,
    eventEmitter: com.github.robran.adserver.kafka.EventEmitter = com.github.robran.adserver.kafka.NoOpEventEmitter,
    meterRegistry: io.micrometer.core.instrument.MeterRegistry =
        com.github.robran.adserver.metrics.PipelineMetrics.defaultRegistry(),
    openTelemetry: io.opentelemetry.api.OpenTelemetry = io.opentelemetry.api.OpenTelemetry.noop(),
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
    openTelemetry = openTelemetry,
)
```

In `main()`, pass the OTel sdk:

```kotlin
    val pipeline = buildPipeline(snapshot, frequencyClient, eventEmitter, meterRegistry, openTelemetry)
```

- [ ] **Step 3: Write the tracing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTracingTest.kt`:

```kotlin
package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.containsAll
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.User
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage

class AuctionPipelineTracingTest {

    private fun campaign(id: String, bid: Double): Campaign {
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "<m>")
        return Campaign(
            id = id,
            advertiserId = "adv",
            advertiserDomain = "x.example.com",
            category = "IAB13",
            bidPrice = bid,
            frequencyCap = 5,
            active = true,
            creatives = listOf(creative),
        )
    }

    private fun req(): BidRequest = BidRequest(
        id = "req-trace",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
        user = User(id = "u-trace"),
    )

    private fun otelWithExporter(exporter: InMemorySpanExporter): OpenTelemetry {
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build()
        return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build()
    }

    @Test
    fun `runAuction emits adserver_request span with user_id and outcome attributes`() = runTest {
        val exporter = InMemorySpanExporter.create()
        val otel = otelWithExporter(exporter)
        val s = InventorySnapshot(listOf(campaign("c1", 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            openTelemetry = otel,
        )
        pipe.runAuction(req())

        val spans = exporter.finishedSpanItems
        val rootSpans = spans.filter { it.name == "adserver.request" }
        assertThat(rootSpans).hasSize(1)
        val rootSpan = rootSpans[0]
        assertThat(rootSpan.attributes.asMap().keys.map { it.key })
            .containsAll(listOf("user.id", "imp.id", "slot.size", "request.id", "outcome"))
        assertThat(rootSpan.attributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("outcome"))).isEqualTo("filled")
    }

    @Test
    fun `runAuction emits a child span for every rule stage`() = runTest {
        val exporter = InMemorySpanExporter.create()
        val otel = otelWithExporter(exporter)
        val s = InventorySnapshot(listOf(campaign("c1", 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            openTelemetry = otel,
        )
        pipe.runAuction(req())

        val spanNames = exporter.finishedSpanItems.map { it.name }.toSet()
        assertThat(spanNames).contains("rule.blocking")
        assertThat(spanNames).contains("rule.freq+compsep")
        assertThat(spanNames).contains("rule.floor")
        assertThat(spanNames).contains("rule.selection")
    }
}
```

This test uses `InMemorySpanExporter` (from `opentelemetry-sdk-testing`) — add the dep to `ad-server/build.gradle.kts`:

```kotlin
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
```

(Resolved by the OTel BOM imported in step 1.)

- [ ] **Step 4: Run tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTracingTest"`
Expected: PASS, 2 tests green.

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL — all earlier tests still pass.

- [ ] **Step 5: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTracingTest.kt
git commit -m "Phase 4b task 4: AuctionPipeline manual spans (root + per-stage)"
```

---

## Task 5: ad-server gRPC client interceptor on freq channel

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`

The `opentelemetry-grpc-1.6` instrumentation provides `GrpcTelemetry.create(openTelemetry).newClientInterceptor()`. Apply it to the `ManagedChannel` so outgoing RPCs propagate trace context as W3C `traceparent` metadata.

- [ ] **Step 1: Modify `Application.kt` to wrap the channel**

Read the current file. Find:

```kotlin
    val frequencyChannel = NettyChannelBuilder
        .forAddress(config.frequency.host, config.frequency.port)
        .usePlaintext()
        .build()
```

Replace with:

```kotlin
    val frequencyChannelRaw = NettyChannelBuilder
        .forAddress(config.frequency.host, config.frequency.port)
        .usePlaintext()
        .build()
    val grpcTelemetry = io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry.create(openTelemetry)
    val frequencyChannel: io.grpc.Channel = io.grpc.ClientInterceptors.intercept(
        frequencyChannelRaw,
        grpcTelemetry.newClientInterceptor(),
    )
```

Note: the `frequencyChannelRaw` (the original `ManagedChannel`) is what gets shut down — the intercepted `Channel` view doesn't have `.shutdown()`. Update the shutdown hook:

```kotlin
    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down ad-server")
            eventEmitter.close()
            frequencyChannelRaw.shutdown()
            meterRegistry.close()
        },
    )
```

- [ ] **Step 2: Update `GrpcFrequencyClient` constructor**

It currently takes `channel: ManagedChannel` — change to `channel: io.grpc.Channel` (the broader interface), since the intercepted channel is `Channel` not `ManagedChannel`:

```kotlin
class GrpcFrequencyClient(
    channel: io.grpc.Channel,
    private val timeoutMs: Long = 8L,
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : FrequencyClient {
```

(`FrequencyGrpcKt.FrequencyCoroutineStub(channel)` accepts `Channel`, so this works.)

- [ ] **Step 3: Verify existing tests still pass**

The existing tests construct `GrpcFrequencyClient` with `InProcessChannelBuilder.build()` which returns `ManagedChannel`, a subtype of `Channel` — they still compile.

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL — all tests still pass (no test changes needed; the type widening is source-compatible).

- [ ] **Step 4: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt
git commit -m "Phase 4b task 5: gRPC client interceptor on frequency channel (W3C trace propagation)"
```

---

## Task 6: ad-server structured JSON logback + MDC

**Files:**
- Modify: `ad-server/src/main/resources/logback.xml`

Replace logback's plain pattern encoder with `LogstashEncoder` + OTel's MDC instrumentation appender. The OTel logback-mdc appender wraps an inner appender and injects `trace_id` / `span_id` into MDC for every log line.

- [ ] **Step 1: Replace `logback.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="JSON_STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeMdcKeyName>trace_id</includeMdcKeyName>
            <includeMdcKeyName>span_id</includeMdcKeyName>
            <includeMdcKeyName>request_id</includeMdcKeyName>
            <includeMdcKeyName>user_id</includeMdcKeyName>
        </encoder>
    </appender>

    <!-- OTel's MDC instrumentation appender wraps the JSON appender so trace_id/span_id are available
         on every log line, including ones emitted from child coroutines. -->
    <appender name="STDOUT" class="io.opentelemetry.instrumentation.logback.mdc.v1_0.OpenTelemetryAppender">
        <appender-ref ref="JSON_STDOUT"/>
    </appender>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
    <logger name="io.netty" level="WARN"/>
    <logger name="com.zaxxer.hikari" level="WARN"/>
    <logger name="org.flywaydb" level="INFO"/>
    <logger name="io.opentelemetry" level="WARN"/>
</configuration>
```

(`logstash-logback-encoder` is already in the catalog and used in `inventory-loader/build.gradle.kts`. The `ad-server/build.gradle.kts` already pulled it transitively via `inventory-loader`'s `api` dep — no new build file change needed.)

If `./gradlew :ad-server:run` fails to find `LogstashEncoder` at runtime, add an explicit dep to `ad-server/build.gradle.kts`:

```kotlin
    implementation(libs.logstash.logback.encoder)
```

- [ ] **Step 2: Verify**

Run: `./gradlew :ad-server:build`
Expected: BUILD SUCCESSFUL.

(Test the JSON output by running the app and curling /openrtb/bid is a manual verification — not a unit test. The smoke test in Task 10 doesn't validate log format either; reading a log line with `jq` is a human verification.)

- [ ] **Step 3: Commit**

```bash
git add ad-server/src/main/resources/logback.xml
git commit -m "Phase 4b task 6: ad-server structured JSON logback with MDC trace_id"
```

(If the dep needed adding, also `git add ad-server/build.gradle.kts`.)

---

## Task 7: frequency-service OtelInitializer + gRPC server interceptor

**Files:**
- Modify: `frequency-service/build.gradle.kts`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt`
- Modify: `frequency-service/src/main/resources/application.conf`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/tracing/OtelInitializer.kt`

- [ ] **Step 1: Add deps**

In `frequency-service/build.gradle.kts`:

```kotlin
    // Phase 4b: OpenTelemetry tracing
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.instrumentation.bom.alpha))
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.context)
    implementation(libs.opentelemetry.grpc.1_6)
    implementation(libs.opentelemetry.logback.mdc.1_0)
    implementation(libs.logstash.logback.encoder)
```

(frequency-service didn't have logstash-logback-encoder yet — add it.)

- [ ] **Step 2: Add `TracingConfig` + block**

In `AppConfig.kt`:

```kotlin
data class TracingConfig(
    val enabled: Boolean,
    val serviceName: String,
    val otlpEndpoint: String,
)
```

Append to `AppConfig`:

```kotlin
data class AppConfig(
    val server: ServerConfig,
    val redis: RedisConfig,
    val metrics: MetricsConfig,
    val tracing: TracingConfig,
) { ... }
```

In `application.conf`, append:

```hocon
    tracing {
        enabled = true
        enabled = ${?TRACING_ENABLED}
        serviceName = "frequency-service"
        serviceName = ${?OTEL_SERVICE_NAME}
        otlpEndpoint = "http://localhost:4317"
        otlpEndpoint = ${?OTEL_EXPORTER_OTLP_ENDPOINT}
    }
```

- [ ] **Step 3: Write `OtelInitializer.kt` for frequency-service**

`frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/tracing/OtelInitializer.kt`:

```kotlin
package com.github.robran.adserver.frequency.tracing

import com.github.robran.adserver.frequency.TracingConfig
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

object OtelInitializer {

    private val log = LoggerFactory.getLogger(javaClass)

    fun init(config: TracingConfig): OpenTelemetry {
        if (!config.enabled) {
            log.info("OpenTelemetry tracing disabled by config")
            return OpenTelemetry.noop()
        }
        val resource = Resource.getDefault().merge(
            Resource.create(
                Attributes.of(AttributeKey.stringKey("service.name"), config.serviceName),
            ),
        )
        val exporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint(config.otlpEndpoint)
            .setTimeout(2, TimeUnit.SECONDS)
            .build()
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .setResource(resource)
            .build()
        val sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal()
        log.info("OpenTelemetry SDK initialized: service={}, otlp={}", config.serviceName, config.otlpEndpoint)
        return sdk
    }
}
```

- [ ] **Step 4: Wire into `Application.kt` with server interceptor**

Read the current file. Replace it:

```kotlin
package com.github.robran.adserver.frequency

import com.github.robran.adserver.frequency.metrics.MeterRegistryFactory
import com.github.robran.adserver.frequency.metrics.MetricsHttpServer
import com.github.robran.adserver.frequency.tracing.OtelInitializer
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val meterRegistry = MeterRegistryFactory.build(config.metrics)
    val openTelemetry = OtelInitializer.init(config.tracing)
    val grpcTelemetry = GrpcTelemetry.create(openTelemetry)

    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis, meterRegistry)

    val server = NettyServerBuilder.forPort(config.server.port)
        .addService(service)
        .intercept(grpcTelemetry.newServerInterceptor())
        .build()
        .start()

    val metricsServer = MetricsHttpServer(meterRegistry, config.metrics.port)
    metricsServer.start()

    log.info(
        "frequency-service listening on port {} (redis={}) — /metrics on port {}, otlp={}",
        config.server.port,
        config.redis.url,
        config.metrics.port,
        config.tracing.otlpEndpoint,
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

- [ ] **Step 5: Run tests**

Run: `./gradlew :frequency-service:test`
Expected: BUILD SUCCESSFUL.

Run: `./gradlew :frequency-service:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add frequency-service/build.gradle.kts \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt \
        frequency-service/src/main/resources/application.conf \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/tracing/OtelInitializer.kt
git commit -m "Phase 4b task 7: frequency-service OtelInitializer + gRPC server interceptor"
```

---

## Task 8: frequency-service structured JSON logback + manual span around Redis ops

**Files:**
- Modify: `frequency-service/src/main/resources/logback.xml`
- Modify: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt`

Logback gets the same JSON treatment as ad-server. `EnrichService.enrichForAuction` adds a manual `redis.enrich` span around the two Redis lookups (this becomes a child span of the gRPC server's `frequency.enrich` span, which is auto-created by the server interceptor from Task 7).

- [ ] **Step 1: Replace `frequency-service/src/main/resources/logback.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="JSON_STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeMdcKeyName>trace_id</includeMdcKeyName>
            <includeMdcKeyName>span_id</includeMdcKeyName>
            <includeMdcKeyName>user_id</includeMdcKeyName>
        </encoder>
    </appender>

    <appender name="STDOUT" class="io.opentelemetry.instrumentation.logback.mdc.v1_0.OpenTelemetryAppender">
        <appender-ref ref="JSON_STDOUT"/>
    </appender>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
    <logger name="io.grpc.netty.shaded" level="WARN"/>
    <logger name="io.lettuce.core" level="WARN"/>
    <logger name="io.opentelemetry" level="WARN"/>
</configuration>
```

- [ ] **Step 2: Add a manual span in `EnrichService.kt`**

Read the current file. Update the constructor to accept `OpenTelemetry` (default `noop`), then wrap the Redis ops in a `redis.enrich` span:

```kotlin
package com.github.robran.adserver.frequency

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.EnrichResponse
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

class EnrichService(
    private val redis: RedisClient,
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    openTelemetry: OpenTelemetry = OpenTelemetry.noop(),
) : FrequencyGrpcKt.FrequencyCoroutineImplBase() {

    private val log = LoggerFactory.getLogger(javaClass)
    private val tracer: Tracer = openTelemetry.getTracer("com.github.robran.adserver.frequency")

    private val mgetTimer: Timer = lookupTimer(meterRegistry, "mget_freq")
    private val zrangeTimer: Timer = lookupTimer(meterRegistry, "zrange_winhistory")

    override suspend fun enrichForAuction(request: EnrichRequest): EnrichResponse {
        val userId = request.userId
        require(userId.isNotEmpty()) { "user_id is required" }
        val campaignIds = request.campaignIdsList

        val redisSpan = tracer.spanBuilder("redis.enrich")
            .setAttribute("user.id", userId)
            .setAttribute("campaign_ids.count", campaignIds.size.toLong())
            .startSpan()
        val redisScope = redisSpan.makeCurrent()
        return try {
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
            redisSpan.setAttribute("freq_counts.size", freqCounts.size.toLong())
            redisSpan.setAttribute("recent_categories.size", recentCategories.size.toLong())

            EnrichResponse.newBuilder()
                .putAllFreqCounts(freqCounts)
                .addAllRecentCategories(recentCategories)
                .build()
        } finally {
            redisScope.close()
            redisSpan.end()
        }
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

- [ ] **Step 3: Pass OTel into `EnrichService` from `Application.kt`**

In `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`, find:

```kotlin
    val service = EnrichService(redis, meterRegistry)
```

Replace with:

```kotlin
    val service = EnrichService(redis, meterRegistry, openTelemetry)
```

- [ ] **Step 4: Run tests**

Run: `./gradlew :frequency-service:test`
Expected: BUILD SUCCESSFUL — existing 14 tests still pass (the `OpenTelemetry.noop()` default keeps test instantiations source-compatible).

- [ ] **Step 5: Commit**

```bash
git add frequency-service/src/main/resources/logback.xml \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt
git commit -m "Phase 4b task 8: frequency-service structured JSON + manual redis.enrich span"
```

---

## Task 9: ad-server full smoke verification (manual, no commit)

This task is human verification — no code changes, no commit.

Recipe:

```bash
# 1. Bring up the infra stack
docker compose up -d

# 2. Init Kafka topics
./scripts/kafka-init-topics.sh

# 3. Run the services in three terminals:
#    Terminal 1 — frequency-service
./gradlew :frequency-service:run

#    Terminal 2 — flink-impression-aggregator
./gradlew :flink-impression-aggregator:run

#    Terminal 3 — ad-server
./gradlew :ad-server:run

# 4. Drive a request
curl -X POST http://localhost:8080/openrtb/bid \
    -H 'Content-Type: application/json' \
    -d '{"id":"trace-demo","imp":[{"id":"1","banner":{"w":300,"h":250}}],"user":{"id":"trace-user"}}' | jq

# 5. Open Jaeger:
#    http://localhost:16686
#    Service: ad-server
#    Operation: adserver.request
#    Click "Find Traces" — you should see one trace with:
#      adserver.request (root, in ad-server)
#       ├─ rule.blocking (child)
#       ├─ rule.freq+compsep (child)
#       │   └─ enrichForAuction (child, gRPC client span)
#       │       └─ frequency.Frequency/EnrichForAuction (child, gRPC server in frequency-service)
#       │           └─ redis.enrich (child, in frequency-service)
#       ├─ rule.floor
#       └─ rule.selection

# 6. Verify JSON logs by tailing one of the JVM service stdouts:
#    Each line should look like:
#    {"@timestamp":"2026-...","level":"INFO","message":"...","trace_id":"abc123...","span_id":"def456..."}
```

If any step fails, document in the README with a known-issues note. **No commit for this task.**

---

## Task 10: README update

**Files:**
- Modify: `README.md`
- Modify: `scripts/smoke-test.sh`

- [ ] **Step 1: Update Status block**

Find:

```markdown
- 🟡 **Phase 4 — Observability** (4a metrics ✅; 4b tracing pending) (this commit)
```

Replace with:

```markdown
- ✅ **Phase 4 — Observability** (this commit)
```

- [ ] **Step 2: Add tracing section**

Find the existing `## Observability` section. Append (BEFORE the next `##` heading):

```markdown
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
```

(Note the nested code fences — the inner ``` for the span hierarchy block needs to be balanced
inside the markdown.)

- [ ] **Step 3: Update smoke-test success line**

In `scripts/smoke-test.sh`, replace:

```bash
echo "==> Phase 1+2+3+4a smoke test PASSED. Metrics surfaces wired (Prometheus/Grafana stack via docker compose)."
```

with:

```bash
echo "==> Phase 1+2+3+4 smoke test PASSED. Full observability: metrics + traces + structured logs."
```

- [ ] **Step 4: Run smoke test**

Run: `./scripts/smoke-test.sh`
Expected: BUILD SUCCESSFUL ending with the new line.

- [ ] **Step 5: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 4b task 10: README + smoke-test update for tracing"
```

---

## Phase 4b Done

Working software:
- Both services emit OpenTelemetry traces to Jaeger via OTLP gRPC
- Manual `adserver.request` root span + `rule.*` child spans across the auction pipeline
- `redis.enrich` span inside frequency-service
- gRPC interceptors propagate W3C trace context across services automatically
- Structured JSON logs with `trace_id` / `span_id` correlation in MDC
- Jaeger all-in-one in docker-compose

**Phase 4 is complete.** Phase 5 (Gatling load testing + profiling) ships next.
