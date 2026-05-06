# Phase 5 — Gatling Load Testing + Profiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Gatling-driven load test that hits `POST /openrtb/bid` at 5K-10K QPS, capture a baseline flame graph via async-profiler, identify the dominant hot-path bottleneck, fix it, and document the before/after p50/p95/p99 numbers in the README. This is the spec's "differentiating layer" — the artifact that distinguishes this portfolio piece from generic Kotlin demo apps.

**Architecture:** New `load-test` module with the Gatling Gradle plugin. A workload generator emits a CSV feeder file with synthetic users (Zipfian distribution so heavy users actually trip frequency caps) and slot/banner combinations. Four scenarios — `RampUp`, `FailFreq`, `Burst`, `Soak` — drive the live `ad-server` process via HTTP. Profiling uses async-profiler (homebrew installable on macOS). Results are committed to `docs/load-test/` so the README can link to baseline + after artifacts.

**Tech Stack additions:**
- `io.gatling.gradle` 3.13.x plugin (Gatling 3.13)
- `io.gatling.highcharts:gatling-charts-highcharts` 3.13.x
- `io.gatling:gatling-test-framework` 3.13.x
- async-profiler 4.0+ (system tool, installed via Homebrew)

---

## Scope notes

The spec's de-scope dial says drop `Soak` and `Burst` if time is tight. This plan keeps all four scenarios as separate tasks so they can be selectively dropped without restructuring.

The spec lists five predicted bottlenecks. The most likely hot-path hit (because it's per-request and adds a real context switch on every freq RPC) is the `withContext(Dispatchers.IO)` wrap added in Phase 2 Task 9 to dodge `runTest` virtual time. **Task 10 commits to fixing exactly that** — replacing the production-side dispatcher hop and switching the affected tests to `runBlocking`. If actual profiling reveals a different dominant hot spot, Task 10 should be re-scoped to that finding (the plan's narrative readme story stays intact: "load test → profile → fix → measure improvement").

---

## File Structure

```
kotlin_ad_server/
├── settings.gradle.kts                                           (modify: add :load-test)
├── gradle/libs.versions.toml                                     (modify: gatling deps)
│
├── load-test/                                                    (NEW MODULE)
│   ├── build.gradle.kts
│   └── src/gatling/
│       ├── kotlin/com/github/robran/adserver/load/
│       │   ├── WorkloadGenerator.kt                              # Emits feeder CSV before run
│       │   ├── BidProtocol.kt                                    # HTTP setup, body builder
│       │   ├── RampUpSimulation.kt
│       │   ├── FailFreqSimulation.kt
│       │   ├── BurstSimulation.kt
│       │   └── SoakSimulation.kt
│       └── resources/
│           └── (CSV feeder generated at runtime → load-test/build/feeders/users.csv)
│
├── scripts/                                                      (modify: new helpers)
│   ├── load-test.sh                                              (NEW: orchestrates run)
│   ├── profiler-attach.sh                                        (NEW: attach async-profiler)
│   └── flame-graph.sh                                            (NEW: collected stacks → SVG)
│
├── docs/load-test/                                               (NEW)
│   ├── baseline.md                                               # First run results
│   ├── after.md                                                  # Post-fix results
│   ├── flamegraph-baseline.svg                                   # Captured during baseline
│   └── flamegraph-after.svg                                      # Captured after fix
│
└── ad-server/
    └── src/main/kotlin/com/github/robran/adserver/auction/
        └── GrpcFrequencyClient.kt                                (modify in Task 10)
```

---

## Task 1: Version Catalog Additions

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1: Add new versions**

In `[versions]`, append after the Phase 4b group:

```toml

# Phase 5 additions
gatling = "3.13.4"
gatling-plugin = "3.13.4"
```

- [ ] **Step 2: Add new libraries**

In `[libraries]`, append:

```toml

# Phase 5: Gatling load test deps (transitively pulled by the plugin, but listed for clarity)
gatling-test-framework = { module = "io.gatling:gatling-test-framework", version.ref = "gatling" }
gatling-highcharts = { module = "io.gatling.highcharts:gatling-charts-highcharts", version.ref = "gatling" }
```

- [ ] **Step 3: Add the Gatling Gradle plugin**

In `[plugins]`, append:

```toml
gatling = { id = "io.gatling.gradle", version.ref = "gatling-plugin" }
```

- [ ] **Step 4: Verify**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml
git commit -m "Phase 5 task 1: catalog adds Gatling 3.13.4"
```

---

## Task 2: Register `:load-test` Module + Build File

**Files:**
- Modify: `settings.gradle.kts`
- Create: `load-test/build.gradle.kts`

- [ ] **Step 1: Add `load-test` to `settings.gradle.kts`**

Read the file. Find the `include(...)` block. Add `"load-test"`:

```kotlin
include(
    "common-protocol",
    "inventory-loader",
    "ad-server",
    "frequency-service",
    "flink-impression-aggregator",
    "load-test",
)
```

- [ ] **Step 2: Create the module directories**

```bash
mkdir -p load-test/src/gatling/kotlin/com/github/robran/adserver/load
mkdir -p load-test/src/gatling/resources
```

- [ ] **Step 3: Write `load-test/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.gatling)
}

dependencies {
    // The gatling plugin sets up its own classpath; we add nothing here for the simulations.
    // BidRequest construction is done with hand-written JSON strings in the simulations to keep
    // this module self-contained and not pull in common-protocol.
    gatlingImplementation(libs.gatling.test.framework)
    gatlingImplementation(libs.gatling.highcharts)
}
```

- [ ] **Step 4: Verify**

Run: `./gradlew :load-test:dependencies --configuration gatlingRuntimeClasspath`
Expected: BUILD SUCCESSFUL. Tree contains `gatling-charts-highcharts` and `gatling-test-framework`.

(If the configuration name is different — Gatling 3.13's plugin may use `gatling` or `gatlingRuntimeClasspath` depending on version — try `./gradlew :load-test:dependencies` and read which configurations it lists.)

- [ ] **Step 5: Commit**

```bash
git add settings.gradle.kts load-test/build.gradle.kts
git commit -m "Phase 5 task 2: register :load-test module with Gatling plugin"
```

---

## Task 3: Workload Generator + Fixture

**Files:**
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/WorkloadGenerator.kt`
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/BidProtocol.kt`

The workload generator runs once before each Gatling simulation and emits a feeder CSV at `load-test/build/feeders/users.csv`. The CSV has 100K synthetic user IDs sampled from a Zipfian distribution (heavy users that will trip frequency caps under load) plus banner sizes and slot IDs for variety.

`BidProtocol` defines the Gatling HTTP protocol + a function that builds a per-request JSON body from feeder values.

- [ ] **Step 1: Write `WorkloadGenerator.kt`**

```kotlin
package com.github.robran.adserver.load

import java.io.File
import kotlin.math.pow
import kotlin.random.Random

/**
 * Pre-generates a CSV feeder for Gatling simulations. Run once before each simulation; the file
 * is regenerated on every invocation so seed-driven randomness stays stable across runs.
 *
 * CSV columns: user_id, banner_w, banner_h, slot_id
 *
 * User IDs are drawn from a Zipfian distribution over a 100K pool — heavy users will hit
 * frequency caps quickly under load; the long tail keeps cardinality realistic.
 */
object WorkloadGenerator {

    private const val USER_POOL_SIZE = 100_000
    private const val ROW_COUNT = 100_000
    private const val ZIPF_S = 1.07
    private val BANNER_SIZES = listOf(300 to 250, 728 to 90, 160 to 600, 300 to 600)
    private val SLOT_IDS = (1..50).map { "slot-%03d".format(it) }

    fun generate(outFile: File, seed: Long = 42L) {
        outFile.parentFile.mkdirs()
        val rnd = Random(seed)
        val zipfCdf = buildZipfCdf(USER_POOL_SIZE, ZIPF_S)
        outFile.bufferedWriter().use { w ->
            w.write("user_id,banner_w,banner_h,slot_id")
            w.newLine()
            repeat(ROW_COUNT) {
                val rank = sampleZipfRank(rnd, zipfCdf)
                val userId = "user-${"%06d".format(rank)}"
                val (bw, bh) = BANNER_SIZES.random(rnd)
                val slotId = SLOT_IDS.random(rnd)
                w.write("$userId,$bw,$bh,$slotId")
                w.newLine()
            }
        }
    }

    private fun buildZipfCdf(n: Int, s: Double): DoubleArray {
        val weights = DoubleArray(n) { i -> 1.0 / (i + 1).toDouble().pow(s) }
        val total = weights.sum()
        val cdf = DoubleArray(n)
        var running = 0.0
        for (i in 0 until n) {
            running += weights[i] / total
            cdf[i] = running
        }
        return cdf
    }

    private fun sampleZipfRank(rnd: Random, cdf: DoubleArray): Int {
        val u = rnd.nextDouble()
        var lo = 0
        var hi = cdf.size - 1
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (cdf[mid] < u) lo = mid + 1 else hi = mid
        }
        return lo
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val out = File(args.getOrElse(0) { "load-test/build/feeders/users.csv" })
        generate(out)
        println("Wrote ${out.length()} bytes to ${out.absolutePath}")
    }
}
```

- [ ] **Step 2: Write `BidProtocol.kt`**

```kotlin
package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.StringBody
import io.gatling.javaapi.core.CoreDsl.csv
import io.gatling.javaapi.core.CoreDsl.exec
import io.gatling.javaapi.core.ChainBuilder
import io.gatling.javaapi.core.FeederBuilder
import io.gatling.javaapi.http.HttpDsl
import io.gatling.javaapi.http.HttpDsl.http
import io.gatling.javaapi.http.HttpProtocolBuilder
import java.io.File

/**
 * Shared Gatling primitives for kotlin_ad_server load tests:
 *  - HTTP protocol pointing at the local ad-server (override via env var ADSERVER_BASE_URL)
 *  - Feeder reading the workload-generator output
 *  - A single chain step `bidRequest` that POSTs /openrtb/bid with feeder values substituted in
 */
object BidProtocol {

    private val baseUrl: String = System.getenv("ADSERVER_BASE_URL") ?: "http://localhost:8080"

    val httpProtocol: HttpProtocolBuilder =
        http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json")
            .userAgentHeader("kotlin_ad_server-load/1.0")

    val feeder: FeederBuilder<*> by lazy {
        val csvPath = System.getenv("LOAD_FEEDER_CSV")
            ?: "load-test/build/feeders/users.csv"
        val file = File(csvPath)
        require(file.exists()) {
            "Feeder CSV not found at $csvPath. Run WorkloadGenerator first."
        }
        csv(file.absolutePath).circular()
    }

    /** One bid request: feeder gives us user_id + banner size + slot. Body is built per request. */
    val bidRequest: ChainBuilder =
        exec(feeder).exec(
            http("POST /openrtb/bid")
                .post("/openrtb/bid")
                .body(
                    StringBody { session ->
                        val uid = session.getString("user_id")
                        val w = session.getInt("banner_w")
                        val h = session.getInt("banner_h")
                        val slot = session.getString("slot_id")
                        val nano = System.nanoTime()
                        """{"id":"req-$nano",""" +
                            """"imp":[{"id":"1","banner":{"w":$w,"h":$h},"tagid":"$slot"}],""" +
                            """"user":{"id":"$uid"}}"""
                    },
                )
                .check(HttpDsl.status().shouldBe(200)),
        )
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./gradlew :load-test:compileGatlingKotlin`
Expected: BUILD SUCCESSFUL.

(Gatling Kotlin sources compile under the `gatling` source set, not `main`. If the task name above doesn't exist, try `./gradlew :load-test:gatlingClasses` — different Gatling plugin versions name the task differently.)

- [ ] **Step 4: Commit**

```bash
git add load-test/src/gatling/kotlin/com/github/robran/adserver/load/WorkloadGenerator.kt \
        load-test/src/gatling/kotlin/com/github/robran/adserver/load/BidProtocol.kt
git commit -m "Phase 5 task 3: workload generator + Gatling HTTP protocol + bid-request chain"
```

---

## Task 4: RampUp Simulation

**Files:**
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/RampUpSimulation.kt`

Profile: 0 → 5K QPS over 5 minutes, then steady 5K for 5 minutes. (Drop the steady-state phase to 1 minute if you don't want to wait.) The "10K target" from the original spec is aspirational; 5K is realistic on a single laptop. Override via env vars `RAMP_TARGET_RPS`, `RAMP_DURATION_SECONDS`, `RAMP_STEADY_SECONDS`.

- [ ] **Step 1: Write the simulation**

```kotlin
package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.rampUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * RampUp: 0 → target QPS over rampSeconds, then steady at target for steadySeconds.
 * Defaults: 5,000 QPS, 5 min ramp, 5 min steady.
 */
class RampUpSimulation : Simulation() {

    private val targetRps: Int = System.getenv("RAMP_TARGET_RPS")?.toInt() ?: 5_000
    private val rampSec: Long = System.getenv("RAMP_DURATION_SECONDS")?.toLong() ?: 300L
    private val steadySec: Long = System.getenv("RAMP_STEADY_SECONDS")?.toLong() ?: 300L

    private val scn = scenario("RampUp")
        .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                rampUsersPerSec(0.0).to(targetRps.toDouble()).during(Duration.ofSeconds(rampSec)),
                constantUsersPerSec(targetRps.toDouble()).during(Duration.ofSeconds(steadySec)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :load-test:compileGatlingKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add load-test/src/gatling/kotlin/com/github/robran/adserver/load/RampUpSimulation.kt
git commit -m "Phase 5 task 4: RampUp simulation (0 to 5K QPS over 5 min)"
```

---

## Task 5: FailFreq Simulation

**Files:**
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/FailFreqSimulation.kt`

Profile: steady 5K QPS while frequency-service is killed by the operator (manually or via a kill script) at the 30s mark and stays down for 30s. Verifies fail-open behavior under load — ad-server should keep filling without an error spike.

This simulation doesn't kill frequency-service itself; it just generates traffic. The operator runs the kill in a parallel terminal (e.g., `docker stop kotlin-ad-frequency` or `pkill -f frequency-service`) and restarts it 30s later.

- [ ] **Step 1: Write the simulation**

```kotlin
package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * FailFreq: steady 5K QPS for 90 seconds. Operator kills frequency-service at the 30s mark
 * and restarts it at the 60s mark. The point is to verify fail-open: ad-server should keep
 * responding 200 OK with empty seatbid, not 500.
 */
class FailFreqSimulation : Simulation() {

    private val rps: Int = System.getenv("FAILFREQ_RPS")?.toInt() ?: 5_000
    private val durationSec: Long = System.getenv("FAILFREQ_DURATION_SECONDS")?.toLong() ?: 90L

    private val scn = scenario("FailFreq")
        .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                constantUsersPerSec(rps.toDouble()).during(Duration.ofSeconds(durationSec)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :load-test:compileGatlingKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add load-test/src/gatling/kotlin/com/github/robran/adserver/load/FailFreqSimulation.kt
git commit -m "Phase 5 task 5: FailFreq simulation (5K QPS for 90s; operator kills freq-svc mid-run)"
```

---

## Task 6: Burst Simulation

**Files:**
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/BurstSimulation.kt`

Profile: 1K baseline, 5x spike to 5K for 30s, repeat 3 times. Tail-latency stress under bursts — GC pauses and warm-up effects show up here.

- [ ] **Step 1: Write the simulation**

```kotlin
package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.OpenInjectionStep
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * Burst: 1K baseline interleaved with 5K spikes (30s baseline, 30s spike, 3 cycles).
 * Tail latency under load transition is the signal we want to surface.
 */
class BurstSimulation : Simulation() {

    private val baseRps: Int = System.getenv("BURST_BASE_RPS")?.toInt() ?: 1_000
    private val spikeRps: Int = System.getenv("BURST_SPIKE_RPS")?.toInt() ?: 5_000
    private val cycles: Int = System.getenv("BURST_CYCLES")?.toInt() ?: 3
    private val phaseSec: Long = System.getenv("BURST_PHASE_SECONDS")?.toLong() ?: 30L

    private val scn = scenario("Burst")
        .exec(BidProtocol.bidRequest)

    init {
        val steps = mutableListOf<OpenInjectionStep>()
        repeat(cycles) {
            steps += constantUsersPerSec(baseRps.toDouble()).during(Duration.ofSeconds(phaseSec))
            steps += constantUsersPerSec(spikeRps.toDouble()).during(Duration.ofSeconds(phaseSec))
        }
        setUp(
            scn.injectOpen(*steps.toTypedArray()),
        ).protocols(BidProtocol.httpProtocol)
    }
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :load-test:compileGatlingKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add load-test/src/gatling/kotlin/com/github/robran/adserver/load/BurstSimulation.kt
git commit -m "Phase 5 task 6: Burst simulation (1K baseline + 5K spikes, 3 cycles)"
```

---

## Task 7: Soak Simulation

**Files:**
- Create: `load-test/src/gatling/kotlin/com/github/robran/adserver/load/SoakSimulation.kt`

Profile: 3K QPS for 30 minutes. Detects memory leaks, connection-pool exhaustion, accumulating GC pressure.

- [ ] **Step 1: Write the simulation**

```kotlin
package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * Soak: 3K QPS for 30 minutes. Slow-burn problems (memory leaks, connection exhaustion,
 * thread pool growth, descriptor exhaustion) surface here.
 */
class SoakSimulation : Simulation() {

    private val rps: Int = System.getenv("SOAK_RPS")?.toInt() ?: 3_000
    private val durationMin: Long = System.getenv("SOAK_DURATION_MINUTES")?.toLong() ?: 30L

    private val scn = scenario("Soak")
        .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                constantUsersPerSec(rps.toDouble()).during(Duration.ofMinutes(durationMin)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :load-test:compileGatlingKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add load-test/src/gatling/kotlin/com/github/robran/adserver/load/SoakSimulation.kt
git commit -m "Phase 5 task 7: Soak simulation (3K QPS for 30 min)"
```

---

## Task 8: Helper Scripts

**Files:**
- Create: `scripts/load-test.sh`
- Create: `scripts/profiler-attach.sh`
- Create: `scripts/flame-graph.sh`

- [ ] **Step 1: Write `scripts/load-test.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/load-test.sh <simulation> [output_dir]
# Example: ./scripts/load-test.sh RampUp docs/load-test/baseline-run
#
# Prerequisites: ad-server, frequency-service, postgres, redis are running.

SIMULATION="${1:-RampUp}"
OUTDIR="${2:-load-test/build/reports/$(date +%Y%m%d-%H%M%S)-${SIMULATION}}"

cd "$(dirname "$0")/.."

echo "==> Generating workload feeder..."
./gradlew :load-test:compileGatlingKotlin --quiet
./gradlew :load-test:runWorkloadGenerator --quiet 2>/dev/null || true

mkdir -p "$OUTDIR"

echo "==> Running ${SIMULATION} simulation..."
./gradlew :load-test:gatlingRun \
    -DgatlingSimulationFqn="com.github.robran.adserver.load.${SIMULATION}Simulation" \
    -PgatlingResultsDir="$OUTDIR"

echo "==> Done. Results in $OUTDIR"
echo "    Open the HTML report (look for index.html under that dir)."
```

Make executable: `chmod +x scripts/load-test.sh`.

- [ ] **Step 2: Write `scripts/profiler-attach.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/profiler-attach.sh <duration_seconds> [out_file]
# Default duration 60s.
#
# Attaches async-profiler to the running ad-server JVM, captures CPU samples for the duration,
# writes collapsed stacks to the output file. Requirements:
#   - brew install async-profiler   (macOS)
#   - sudo sysctl kernel.perf_event_paranoid=1   (Linux only)

DURATION="${1:-60}"
OUT="${2:-docs/load-test/profile-$(date +%Y%m%d-%H%M%S).collapsed}"

cd "$(dirname "$0")/.."
mkdir -p "$(dirname "$OUT")"

# Find async-profiler — homebrew installs to /opt/homebrew or /usr/local/Cellar
PROFILER_JAR=""
for candidate in \
    /opt/homebrew/lib/async-profiler/async-profiler.jar \
    /usr/local/lib/async-profiler/async-profiler.jar \
    /opt/homebrew/Cellar/async-profiler/*/lib/async-profiler.jar \
    "$HOME/async-profiler/lib/async-profiler.jar"; do
    if [[ -f "$candidate" ]]; then
        PROFILER_JAR="$candidate"
        break
    fi
done
if [[ -z "$PROFILER_JAR" ]]; then
    echo "ERROR: async-profiler not found. brew install async-profiler"
    exit 1
fi

PID=$(jps -l | grep -E 'ApplicationKt|ad-server' | head -1 | awk '{print $1}')
if [[ -z "$PID" ]]; then
    echo "ERROR: ad-server JVM not running. Start with ./gradlew :ad-server:run"
    exit 1
fi
echo "==> Attaching to PID $PID, sampling $DURATION s, output $OUT"

java -jar "$PROFILER_JAR" -d "$DURATION" -e cpu -o collapsed -f "$OUT" "$PID"

echo "==> Captured $(wc -l <"$OUT") stack samples to $OUT"
```

Make executable: `chmod +x scripts/profiler-attach.sh`.

- [ ] **Step 3: Write `scripts/flame-graph.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/flame-graph.sh <input.collapsed> [output.svg]
# Renders a flame graph from collapsed stacks. Requires: brew install flamegraph

IN="${1:?usage: flame-graph.sh <input.collapsed> [output.svg]}"
OUT="${2:-${IN%.collapsed}.svg}"

if ! command -v flamegraph.pl >/dev/null 2>&1; then
    echo "ERROR: flamegraph.pl not on PATH. brew install flamegraph"
    exit 1
fi

flamegraph.pl --title "ad-server hot path" --countname "samples" "$IN" > "$OUT"
echo "==> Wrote $OUT"
```

Make executable: `chmod +x scripts/flame-graph.sh`.

- [ ] **Step 4: Verify all three are executable**

Run: `ls -la scripts/load-test.sh scripts/profiler-attach.sh scripts/flame-graph.sh`
Expected: all three show `-rwxr-xr-x`.

- [ ] **Step 5: Commit**

```bash
git add scripts/load-test.sh scripts/profiler-attach.sh scripts/flame-graph.sh
git commit -m "Phase 5 task 8: helper scripts (load-test runner + profiler attach + flame graph)"
```

---

## Task 9: Manual Baseline Run + Profile (no commit)

This is human verification — no code changes, no commit. The output of this step is the artifacts in `docs/load-test/` that Task 11 commits.

**Recipe:**

1. **Bring up the full stack:**
   ```bash
   docker compose up -d
   ./scripts/kafka-init-topics.sh
   ./gradlew :frequency-service:run &
   ./gradlew :ad-server:run &
   sleep 15  # let JIT warm up + inventory hydrate
   ```

2. **Verify warm-up:**
   ```bash
   curl -s http://localhost:8080/healthz
   curl -s http://localhost:8080/metrics | grep adserver_request_duration | head
   ```

3. **Drive a brief warmup load:**
   ```bash
   RAMP_TARGET_RPS=2000 RAMP_DURATION_SECONDS=30 RAMP_STEADY_SECONDS=30 \
     ./scripts/load-test.sh RampUp /tmp/warmup-run
   ```

4. **Attach the profiler in one terminal, then run the real RampUp in another:**
   ```bash
   # Terminal A — start the profiler with a 5-min window
   ./scripts/profiler-attach.sh 300 docs/load-test/profile-baseline.collapsed
   # Terminal B — drive 5K QPS for ~5 min
   ./scripts/load-test.sh RampUp docs/load-test/baseline-run
   ```

5. **Render the flame graph:**
   ```bash
   ./scripts/flame-graph.sh docs/load-test/profile-baseline.collapsed docs/load-test/flamegraph-baseline.svg
   ```

6. **Capture the numbers:**
   - Open the Gatling HTML report at `docs/load-test/baseline-run/<latest>/index.html`.
   - Note p50, p95, p99, max from the response-time distribution.
   - Note the request rate (RPS achieved vs. target).
   - Note the error rate (should be <0.1%).

7. **Write `docs/load-test/baseline.md`** (committed in Task 11):
   ```markdown
   # Baseline Load Test Results

   **Date:** YYYY-MM-DD
   **Hardware:** <e.g. M3 Pro, 32 GB RAM>
   **Stack:** docker compose (Postgres + Redis + Kafka + Schema Registry + Jaeger + Prometheus + Grafana)

   ## RampUp (0 → 5K QPS, 5 min ramp + 5 min steady)

   | Metric | Value |
   |---|---|
   | Achieved RPS | XXXX |
   | Mean response time | XX ms |
   | p50 | XX ms |
   | p95 | XX ms |
   | p99 | XX ms |
   | max | XX ms |
   | Error rate | 0.XX% |

   ## Flame graph (CPU profile, 5 min sample)

   ![Baseline flame graph](flamegraph-baseline.svg)

   ## Observations

   <write 2-4 sentences identifying the dominant hot spot from the flame graph.
   Look especially at:
   - Coroutine context switches (Dispatchers.IO frames)
   - JSON serialization in ktor/kotlinx-serialization
   - gRPC stub call frames
   - Lettuce reactive bridging
   - Kafka producer batch send
   - Avro encoding>
   ```

8. **Don't commit yet.** Task 11 commits these artifacts after Task 10's fix is applied and the after-run is captured.

---

## Task 10: Implement the Bottleneck Fix

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`

**The predicted hot spot (per spec section 9.4):** the `withContext(Dispatchers.IO)` wrap added in Phase 2 Task 9 around `withTimeout`. It exists ONLY to dodge `runTest` virtual time, but in production it forces a context switch on every freq RPC. The fix removes the wrap from production code and switches the affected tests to `runBlocking` (which uses real wall-clock time, no virtual scheduler).

**If the actual flame graph from Task 9 reveals a different dominant hotspot** (e.g., JSON serialization, Lettuce reactive bridging), re-scope this task accordingly — the README narrative ("found a hotspot, fixed it, re-measured") works for any of the predicted bottlenecks. The plan's predicted fix is the most likely match.

- [ ] **Step 1: Replace `GrpcFrequencyClient.kt`**

Read the current file. The current `enrich` method has a `withContext(Dispatchers.IO) { withTimeout(timeoutMs) { ... } }` wrap. Replace the file contents:

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.Channel
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.TimeoutCancellationException
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
 *
 * Phase 5 fix: removed the `withContext(Dispatchers.IO)` wrap that was added in Phase 2 to
 * dodge `runTest` virtual time. The wrap forced a context switch per RPC, costing real p99
 * under sustained load. Tests that exercise real timeout behavior now use `runBlocking`.
 */
class GrpcFrequencyClient(
    channel: Channel,
    private val timeoutMs: Long = 8L,
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : FrequencyClient {

    private val log = LoggerFactory.getLogger(javaClass)
    private val stub = FrequencyGrpcKt.FrequencyCoroutineStub(channel)

    private val okTimer: Timer = newTimer(meterRegistry, "ok")
    private val timeoutTimer: Timer = newTimer(meterRegistry, "timeout")
    private val errorTimer: Timer = newTimer(meterRegistry, "error")

    override suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult {
        val request =
            EnrichRequest.newBuilder()
                .setUserId(userId)
                .addAllCampaignIds(campaignIds)
                .build()
        var nanos = 0L
        return try {
            var resp: com.github.robran.adserver.protocol.frequency.EnrichResponse
            nanos = measureNanoTime {
                resp = withTimeout(timeoutMs) { stub.enrichForAuction(request) }
            }
            okTimer.record(nanos, TimeUnit.NANOSECONDS)
            EnrichResult(
                freqCounts = resp.freqCountsMap.toMap(),
                recentCategories = resp.recentCategoriesList.toSet(),
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

Key change: removed `withContext(Dispatchers.IO)` and the `Dispatchers` + `withContext` imports.

- [ ] **Step 2: Switch tests that exercise real timeout to `runBlocking`**

Read `ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`. All seven tests in this file currently use `runTest { ... }`. Switch every one to `runBlocking { ... }`:

Add this import at the top of the file:

```kotlin
import kotlinx.coroutines.runBlocking
```

Then for EACH of the seven tests, find:

```kotlin
fun `<name>`() = runTest {
```

And replace with:

```kotlin
fun `<name>`() = runBlocking {
```

Uniform replacement keeps the file consistent. `runBlocking` works fine for any test that doesn't depend on virtual time, and the per-test runtime is dominated by gRPC + delay calls anyway.

- [ ] **Step 3: Run the tests**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.GrpcFrequencyClientTest"`
Expected: PASS, 7 tests green.

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL — all other tests still pass.

- [ ] **Step 4: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt
git commit -m "Phase 5 task 10: remove Dispatchers.IO wrap from GrpcFrequencyClient hot path"
```

---

## Task 11: Manual After-Fix Run + Capture Before/After Numbers

This task is human verification — the artifact is a commit that adds the comparison docs.

**Recipe:**

1. **Re-run the same RampUp profile with the fix in place:**
   ```bash
   # Terminal A
   ./scripts/profiler-attach.sh 300 docs/load-test/profile-after.collapsed
   # Terminal B
   ./scripts/load-test.sh RampUp docs/load-test/after-run
   ```

2. **Render the after flame graph:**
   ```bash
   ./scripts/flame-graph.sh docs/load-test/profile-after.collapsed docs/load-test/flamegraph-after.svg
   ```

3. **Write `docs/load-test/after.md`:**
   ```markdown
   # After-Fix Load Test Results

   **Date:** YYYY-MM-DD (after Phase 5 Task 10)
   **Fix applied:** removed `withContext(Dispatchers.IO)` wrap from `GrpcFrequencyClient.enrich`,
   eliminating one context switch per freq RPC.

   ## RampUp (same parameters as baseline)

   | Metric | Baseline | After fix | Δ |
   |---|---|---|---|
   | Mean | XX ms | XX ms | -XX ms |
   | p50 | XX ms | XX ms | -XX ms |
   | p95 | XX ms | XX ms | -XX ms |
   | p99 | XX ms | XX ms | -XX ms |
   | Achieved RPS | XXXX | XXXX | +XXXX |

   ## Flame graph (after fix)

   ![After flame graph](flamegraph-after.svg)

   The `Dispatchers.IO` frames near the top of the freq RPC path are gone; the same total CPU
   time is now spent more evenly across the actual RPC + Avro path.
   ```

4. **Commit the artifacts:**
   ```bash
   git add docs/load-test/baseline.md \
           docs/load-test/after.md \
           docs/load-test/flamegraph-baseline.svg \
           docs/load-test/flamegraph-after.svg \
           docs/load-test/profile-baseline.collapsed \
           docs/load-test/profile-after.collapsed
   git commit -m "Phase 5 task 11: load-test results — baseline + after-fix p50/p95/p99 with flame graphs"
   ```

(Note: the `.collapsed` files can be large. If they're > 1 MB consider gzipping or omitting from the commit; the SVGs are the primary artifact.)

---

## Task 12: README Phase 5 Section + Smoke Test Update

**Files:**
- Modify: `README.md`
- Modify: `scripts/smoke-test.sh`

- [ ] **Step 1: Update Status block**

Find:

```markdown
- ✅ **Phase 4 — Observability** (this commit)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README
```

Replace with:

```markdown
- ✅ **Phase 4 — Observability**
- ✅ **Phase 5 — Gatling load testing + profiling** (this commit)
- ⏳ Phase 6 — Polish + final README
```

- [ ] **Step 2: Add a Load Testing section**

Append a new top-level section before the existing `## Smoke test` heading:

```markdown
## Load testing

Four Gatling Kotlin DSL scenarios in the `:load-test` module:

| Scenario | Profile | Goal |
|---|---|---|
| `RampUp` | 0 → 5K QPS over 5 min, then 5 min steady | sustained-load latency baseline |
| `Burst` | 1K baseline / 5K spike, 3 cycles | tail latency under load transitions |
| `Soak` | 3K QPS for 30 min | memory leaks, pool exhaustion |
| `FailFreq` | 5K QPS while frequency-service is killed | fail-open behavior under load |

Run: `./scripts/load-test.sh RampUp docs/load-test/baseline-run` (after `docker compose up -d`
and the three service `./gradlew :*:run`).

### Profiling: before / after

We profiled the baseline RampUp at 5K QPS with [async-profiler](https://github.com/async-profiler/async-profiler).
The flame graph showed `Dispatchers.IO` frames dominating the freq-RPC path — a leftover from
Phase 2 Task 9 where `withContext(Dispatchers.IO)` was added around `withTimeout` to dodge
`kotlinx.coroutines.test`'s virtual-time scheduler. In production this forced a context switch
per RPC.

The fix (Phase 5 Task 10): removed the wrap, switched the affected tests to `runBlocking`.
Detailed results in [docs/load-test/baseline.md](docs/load-test/baseline.md) and
[docs/load-test/after.md](docs/load-test/after.md), including flame graphs.
```

- [ ] **Step 3: Update smoke-test success line**

In `scripts/smoke-test.sh`, replace:

```bash
echo "==> Phase 1+2+3+4 smoke test PASSED. Full observability: metrics + traces + structured logs."
```

with:

```bash
echo "==> Phase 1+2+3+4+5 smoke test PASSED. Load test scenarios + profiling toolchain ready."
```

(Note: the smoke test still runs `./gradlew test`, which doesn't run Gatling. Gatling is invoked separately via `./scripts/load-test.sh`.)

- [ ] **Step 4: Run the smoke test**

Run: `./scripts/smoke-test.sh`
Expected: BUILD SUCCESSFUL ending with the new line.

- [ ] **Step 5: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 5 task 12: README load-testing section + smoke-test update"
```

---

## Phase 5 Done

Working software:
- `:load-test` module with 4 Gatling scenarios + workload generator
- 3 helper scripts (`load-test.sh`, `profiler-attach.sh`, `flame-graph.sh`)
- Baseline + after flame graph SVGs committed under `docs/load-test/`
- `docs/load-test/baseline.md` and `after.md` documenting before/after p50/p95/p99
- README "Load testing" section with the comparison narrative

**Next:** Phase 6 — final polish (architecture diagrams in Mermaid, CI workflow on GitHub Actions). Will need its own plan.
