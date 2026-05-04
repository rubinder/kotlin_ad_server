# Phase 2 — Frequency Service + Redis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a standalone `frequency-service` (gRPC, port 9090) backed by Redis, and swap the `ad-server`'s `FakeFrequencyClient` for a real `GrpcFrequencyClient` that calls it with an 8 ms timeout and fail-open behavior.

**Architecture:** New Gradle module `frequency-service` runs Lettuce-based reactive Redis access behind a single combined `EnrichForAuction` RPC. The proto definition lives in `common-protocol` so both server and client share one source of truth. The `ad-server` keeps `FakeFrequencyClient` available (for tests) but production wiring uses the real gRPC client. Increments to Redis are deferred to Phase 3 (Flink); Phase 2's frequency-service is read-only on the gRPC layer.

**Tech Stack:**
- Existing: Kotlin 2.1.0 / JVM 21, Gradle 8.11, Ktor 3.0.3
- New:
  - protobuf 4.28.3
  - grpc-java 1.68.2
  - grpc-kotlin-stub 1.4.1
  - protobuf-gradle-plugin 0.9.4
  - lettuce-core 6.5.1.RELEASE
  - kotlinx-coroutines-reactor 1.9.0 (bridges Lettuce reactive Mono → suspend)
  - testcontainers Redis module (already in catalog as `testcontainers-bom`; add a `testcontainers-redis` library entry)

---

## File Structure

```
kotlin_ad_server/
├── settings.gradle.kts                                          (modify: add :frequency-service)
├── gradle/libs.versions.toml                                    (modify: protobuf, grpc, lettuce, etc.)
│
├── common-protocol/
│   ├── build.gradle.kts                                         (modify: protobuf plugin + grpc deps)
│   └── src/main/proto/
│       └── frequency.proto                                      (NEW)
│
├── frequency-service/                                           (NEW MODULE)
│   ├── build.gradle.kts
│   └── src/
│       ├── main/kotlin/com/github/robran/adserver/frequency/
│       │   ├── Application.kt                                   # gRPC server bootstrap
│       │   ├── AppConfig.kt
│       │   ├── RedisClient.kt                                   # Lettuce → coroutines bridge
│       │   └── EnrichService.kt                                 # gRPC service impl
│       ├── main/resources/
│       │   ├── application.conf
│       │   └── logback.xml
│       └── test/kotlin/com/github/robran/adserver/frequency/
│           ├── EnrichServiceIntegrationTest.kt                  # Testcontainers Redis + in-process gRPC
│           └── RedisClientTest.kt                               # Standalone Redis ops test
│
└── ad-server/
    ├── build.gradle.kts                                         (modify: grpc client deps)
    ├── src/main/kotlin/com/github/robran/adserver/
    │   ├── Application.kt                                       (modify: parameterize buildPipeline, wire gRPC)
    │   ├── AppConfig.kt                                         (modify: add FrequencyConfig)
    │   └── auction/
    │       └── GrpcFrequencyClient.kt                           (NEW)
    ├── src/main/resources/application.conf                       (modify: frequency host/port)
    └── src/test/kotlin/com/github/robran/adserver/
        └── auction/GrpcFrequencyClientTest.kt                   (NEW: in-process gRPC server)
```

---

## Task 1: Version Catalog Additions

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1: Add new versions and libraries**

In the `[versions]` section, add (alphabetically sorted in the Phase 2 group):

```toml
# Phase 2 additions
grpc = "1.68.2"
grpc-kotlin = "1.4.1"
lettuce = "6.5.1.RELEASE"
protobuf = "4.28.3"
protobuf-plugin = "0.9.4"
```

In the `[libraries]` section, append a new block for Phase 2 deps. Place it after the existing test entries:

```toml
# Phase 2: gRPC + protobuf
grpc-netty-shaded = { module = "io.grpc:grpc-netty-shaded", version.ref = "grpc" }
grpc-protobuf = { module = "io.grpc:grpc-protobuf", version.ref = "grpc" }
grpc-stub = { module = "io.grpc:grpc-stub", version.ref = "grpc" }
grpc-kotlin-stub = { module = "io.grpc:grpc-kotlin-stub", version.ref = "grpc-kotlin" }
protobuf-java = { module = "com.google.protobuf:protobuf-java", version.ref = "protobuf" }
protobuf-kotlin = { module = "com.google.protobuf:protobuf-kotlin", version.ref = "protobuf" }

# Phase 2: Redis (Lettuce + reactor coroutines bridge)
lettuce-core = { module = "io.lettuce:lettuce-core", version.ref = "lettuce" }
kotlinx-coroutines-reactor = { module = "org.jetbrains.kotlinx:kotlinx-coroutines-reactor", version.ref = "kotlinx-coroutines" }

# Phase 2: testcontainers Redis (the Redis container module is bundled in testcontainers core)
# The testcontainers-bom already provides version coordination — no extra entry needed.
```

In the `[plugins]` section, add the protobuf plugin:

```toml
protobuf = { id = "com.google.protobuf", version.ref = "protobuf-plugin" }
```

- [ ] **Step 2: Verify the catalog parses**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL. The catalog still parses; new entries are not yet referenced anywhere.

- [ ] **Step 3: Commit**

```bash
git add gradle/libs.versions.toml
git commit -m "Phase 2 task 1: add grpc, protobuf, lettuce, coroutines-reactor to catalog"
```

---

## Task 2: Register the `frequency-service` Module

**Files:**
- Modify: `settings.gradle.kts`

- [ ] **Step 1: Update `settings.gradle.kts`**

In the existing `include(...)` block, add `"frequency-service"`:

```kotlin
include(
    "common-protocol",
    "inventory-loader",
    "ad-server",
    "frequency-service",
)
```

(Order doesn't matter to Gradle, but keep it consistent with the dependency order: common-protocol first, then services that consume it.)

- [ ] **Step 2: Create the empty module directory**

Run:

```bash
mkdir -p frequency-service/src/main/kotlin/com/github/robran/adserver/frequency
mkdir -p frequency-service/src/main/resources
mkdir -p frequency-service/src/test/kotlin/com/github/robran/adserver/frequency
mkdir -p frequency-service/src/test/resources
```

- [ ] **Step 3: Create a placeholder `build.gradle.kts`**

Write `frequency-service/build.gradle.kts`:

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
}
```

(This is a stub so Gradle can configure the module. Task 4 fleshes it out.)

- [ ] **Step 4: Verify Gradle still configures**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL. The new `frequency-service` project appears (you can verify with `./gradlew projects`).

- [ ] **Step 5: Commit**

```bash
git add settings.gradle.kts frequency-service/build.gradle.kts
git commit -m "Phase 2 task 2: register :frequency-service module"
```

---

## Task 3: `frequency.proto` and Codegen in `common-protocol`

**Files:**
- Modify: `common-protocol/build.gradle.kts`
- Create: `common-protocol/src/main/proto/frequency.proto`

- [ ] **Step 1: Write `frequency.proto`**

```bash
mkdir -p common-protocol/src/main/proto
```

Then write `common-protocol/src/main/proto/frequency.proto`:

```proto
syntax = "proto3";

package com.github.robran.adserver.protocol.frequency;

option java_multiple_files = true;
option java_package = "com.github.robran.adserver.protocol.frequency";

// Single combined RPC: returns per-campaign impression counts AND the IAB categories
// recently served to this user, in one round-trip. See spec section 5.2 for rationale.
service Frequency {
    rpc EnrichForAuction(EnrichRequest) returns (EnrichResponse);
}

message EnrichRequest {
    string user_id = 1;
    repeated string campaign_ids = 2;
}

message EnrichResponse {
    // campaign_id -> current count over the cap window. Missing campaign means count=0.
    map<string, int32> freq_counts = 1;
    // distinct IAB categories served to the user within the competitive-separation window
    repeated string recent_categories = 2;
}
```

- [ ] **Step 2: Update `common-protocol/build.gradle.kts`**

Replace the entire file contents with:

```kotlin
import com.google.protobuf.gradle.id

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.protobuf)
    `java-library`
}

dependencies {
    api(libs.kotlinx.serialization.json)

    // Generated proto + grpc stubs are exported via api so consumers (frequency-service, ad-server)
    // can use the message types and stub classes directly.
    api(libs.protobuf.java)
    api(libs.protobuf.kotlin)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.grpc.kotlin.stub)
    api(libs.kotlinx.coroutines.core)

    // grpc-stub references annotation-only javax.annotation; provide it explicitly.
    api("javax.annotation:javax.annotation-api:1.3.2")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:${libs.versions.grpc.kotlin.get()}:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
                id("grpckt")
            }
            task.builtins {
                id("kotlin")
            }
        }
    }
}

// Tell Kotlin compile to depend on generated sources (the protobuf plugin sets up the source dirs,
// but Kotlin compilation needs an explicit dependency to wait for codegen).
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    dependsOn(tasks.named("generateProto"))
}
```

- [ ] **Step 3: Run codegen and verify**

Run: `./gradlew :common-protocol:generateProto`
Expected: BUILD SUCCESSFUL. Files appear under `common-protocol/build/generated/source/proto/main/`:
- `java/com/github/robran/adserver/protocol/frequency/EnrichRequest.java`
- `java/com/github/robran/adserver/protocol/frequency/EnrichResponse.java`
- `java/com/github/robran/adserver/protocol/frequency/FrequencyGrpc.java`
- `grpckt/com/github/robran/adserver/protocol/frequency/FrequencyGrpcKt.kt`
- `kotlin/com/github/robran/adserver/protocol/frequency/EnrichRequestKt.kt` (and similar)

Then run: `./gradlew :common-protocol:compileKotlin`
Expected: BUILD SUCCESSFUL. The generated Kotlin compiles cleanly.

- [ ] **Step 4: Add a smoke test that the generated stubs are wired**

Write `common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/frequency/FrequencyProtoTest.kt`:

```kotlin
package com.github.robran.adserver.protocol.frequency

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class FrequencyProtoTest {

    @Test
    fun `EnrichRequest builder accepts user_id and campaign_ids`() {
        val req = EnrichRequest.newBuilder()
            .setUserId("user-1")
            .addCampaignIds("camp-001")
            .addCampaignIds("camp-002")
            .build()

        assertThat(req.userId).isEqualTo("user-1")
        assertThat(req.campaignIdsCount).isEqualTo(2)
        assertThat(req.getCampaignIds(0)).isEqualTo("camp-001")
    }

    @Test
    fun `EnrichResponse builder accepts freq_counts and recent_categories`() {
        val resp = EnrichResponse.newBuilder()
            .putFreqCounts("camp-001", 5)
            .putFreqCounts("camp-002", 3)
            .addRecentCategories("IAB1")
            .addRecentCategories("IAB13-1")
            .build()

        assertThat(resp.freqCountsMap["camp-001"]).isEqualTo(5)
        assertThat(resp.recentCategoriesCount).isEqualTo(2)
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :common-protocol:test --tests "com.github.robran.adserver.protocol.frequency.FrequencyProtoTest"`
Expected: PASS, 2 tests green.

- [ ] **Step 6: Commit**

```bash
git add common-protocol/build.gradle.kts \
        common-protocol/src/main/proto/frequency.proto \
        common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/frequency/FrequencyProtoTest.kt
git commit -m "Phase 2 task 3: frequency.proto + grpc-kotlin codegen in common-protocol"
```

---

## Task 4: `frequency-service` Module Build File

**Files:**
- Modify: `frequency-service/build.gradle.kts`
- Create: `frequency-service/src/main/resources/application.conf`
- Create: `frequency-service/src/main/resources/logback.xml`

- [ ] **Step 1: Replace `frequency-service/build.gradle.kts` entirely**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

application {
    mainClass.set("com.github.robran.adserver.frequency.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))

    // gRPC server (only the netty-shaded transport — keeps the deployment JAR self-contained)
    implementation(libs.grpc.netty.shaded)

    // Redis: Lettuce reactive + coroutine bridge
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.reactor)

    // Config + logging
    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation(libs.logstash.logback.encoder)
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    // Generic Testcontainers GenericContainer is enough for Redis — no dedicated module needed.
}
```

- [ ] **Step 2: Write `frequency-service/src/main/resources/application.conf`**

```hocon
frequency {
    server {
        port = 9090
        port = ${?FREQ_SERVICE_PORT}
    }
    redis {
        url = "redis://localhost:6379"
        url = ${?FREQ_REDIS_URL}
    }
}
```

- [ ] **Step 3: Write `frequency-service/src/main/resources/logback.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{ISO8601} %-5level [%thread] %logger{30} - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
    <logger name="io.grpc.netty.shaded" level="WARN"/>
    <logger name="io.lettuce.core" level="WARN"/>
</configuration>
```

- [ ] **Step 4: Verify the module configures**

Run: `./gradlew :frequency-service:dependencies --configuration runtimeClasspath` (any non-empty stanza is fine; check there are no errors)
Expected: BUILD SUCCESSFUL. `lettuce-core`, `grpc-netty-shaded`, and the `common-protocol` project are all in the dependency tree.

- [ ] **Step 5: Commit**

```bash
git add frequency-service/build.gradle.kts \
        frequency-service/src/main/resources/application.conf \
        frequency-service/src/main/resources/logback.xml
git commit -m "Phase 2 task 4: frequency-service module setup (Lettuce + grpc-netty-shaded)"
```

---

## Task 5: `RedisClient` (Lettuce reactive → coroutines bridge)

**Files:**
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/RedisClient.kt`
- Create: `frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/RedisClientTest.kt`
- Create: `frequency-service/src/test/resources/logback-test.xml`

The `RedisClient` is a thin facade over Lettuce that exposes the few operations the read-side service needs as `suspend` functions. We use Lettuce's reactive API and bridge to coroutines via `kotlinx-coroutines-reactor`'s `awaitSingleOrNull` / `awaitSingle`.

- [ ] **Step 1: Write `logback-test.xml`**

`frequency-service/src/test/resources/logback-test.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} %-5level [%thread] %logger{30} - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
    <logger name="org.testcontainers" level="WARN"/>
    <logger name="com.github.dockerjava" level="WARN"/>
    <logger name="io.lettuce.core" level="WARN"/>
</configuration>
```

- [ ] **Step 2: Write the failing test**

`frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/RedisClientTest.kt`:

```kotlin
package com.github.robran.adserver.frequency

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// Manual @BeforeAll/@AfterAll lifecycle — same rationale as InventoryLoaderTest.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisClientTest {

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private lateinit var client: RedisClient

    @BeforeAll
    fun setup() {
        redis.start()
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        client = RedisClient.connect(url)
    }

    @AfterAll
    fun tearDown() {
        client.close()
        redis.stop()
    }

    @Test
    fun `get returns null for missing key`() = runTest {
        assertThat(client.get("missing-key")).isNull()
    }

    @Test
    fun `set then get round-trips a value`() = runTest {
        client.set("my-key", "my-value")
        assertThat(client.get("my-key")).isEqualTo("my-value")
    }

    @Test
    fun `mget returns values in input order, nulls for missing keys`() = runTest {
        client.set("k1", "v1")
        client.set("k3", "v3")
        val values = client.mget(listOf("k1", "k2", "k3"))
        assertThat(values).isEqualTo(listOf("v1", null, "v3"))
    }

    @Test
    fun `mget with empty input returns empty list without calling Redis`() = runTest {
        val values = client.mget(emptyList())
        assertThat(values).isEmpty()
    }

    @Test
    fun `zrangeByScore reads members ordered by score`() = runTest {
        client.zadd("scores", "alpha" to 1.0, "bravo" to 2.0, "charlie" to 3.0)
        val members = client.zrangeByScore("scores", 1.5, 3.5)
        assertThat(members).containsExactlyInAnyOrder("bravo", "charlie")
    }

    @Test
    fun `zrangeByScore returns empty for missing key`() = runTest {
        assertThat(client.zrangeByScore("missing-zset", 0.0, Double.POSITIVE_INFINITY)).isEmpty()
    }
}
```

- [ ] **Step 3: Run the test to confirm it fails**

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.RedisClientTest"`
Expected: FAIL — `RedisClient` does not exist.

- [ ] **Step 4: Write `RedisClient.kt`**

`frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/RedisClient.kt`:

```kotlin
package com.github.robran.adserver.frequency

import io.lettuce.core.Range
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.reactive.RedisReactiveCommands
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import io.lettuce.core.RedisClient as LettuceClient

/**
 * Thin coroutine facade over Lettuce's reactive API. Exposes only the operations the read-side
 * frequency service needs. Closing the client closes the underlying connection and shuts down the
 * Lettuce client thread pool.
 */
class RedisClient(
    private val lettuce: LettuceClient,
    private val connection: StatefulRedisConnection<String, String>,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val cmd: RedisReactiveCommands<String, String> = connection.reactive()

    suspend fun get(key: String): String? =
        cmd.get(key).awaitSingleOrNull()

    suspend fun set(key: String, value: String): String =
        cmd.set(key, value).awaitSingle()

    suspend fun mget(keys: List<String>): List<String?> {
        if (keys.isEmpty()) return emptyList()
        // Lettuce returns KeyValue<K,V>; map to V or null in the input order.
        val results = cmd.mget(*keys.toTypedArray())
            .collectList()
            .awaitSingle()
        // The reactive mget preserves input order. Each result is a KeyValue with hasValue() flag.
        val byKey = results.associateBy { it.key }
        return keys.map { k -> byKey[k]?.let { kv -> if (kv.hasValue()) kv.value else null } }
    }

    suspend fun zadd(key: String, vararg members: Pair<String, Double>): Long {
        val scoredValues = members.map { (m, s) -> io.lettuce.core.ScoredValue.just(s, m) }.toTypedArray()
        return cmd.zadd(key, *scoredValues).awaitSingle()
    }

    suspend fun zrangeByScore(key: String, min: Double, max: Double): List<String> {
        val range = Range.create(min, max)
        return cmd.zrangebyscore(key, range)
            .collectList()
            .awaitSingle()
    }

    override fun close() {
        try {
            connection.close()
        } finally {
            lettuce.shutdown()
        }
    }

    companion object {
        fun connect(redisUrl: String): RedisClient {
            val uri = RedisURI.create(redisUrl)
            val lettuce = LettuceClient.create(uri)
            val connection = lettuce.connect()
            return RedisClient(lettuce, connection)
        }
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.RedisClientTest"`
Expected: PASS, 6 tests green.

- [ ] **Step 6: Commit**

```bash
git add frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/RedisClient.kt \
        frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/RedisClientTest.kt \
        frequency-service/src/test/resources/logback-test.xml
git commit -m "Phase 2 task 5: RedisClient (Lettuce reactive → coroutines bridge)"
```

---

## Task 6: `EnrichService` (gRPC service implementation)

**Files:**
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt`

The service implements the generated `FrequencyCoroutineImplBase` (from grpc-kotlin). Its single method reads `freq:{userId}:{campaignId}` for each requested campaign and `winhistory:{userId}` for the user's recent categories, in two pipelined Redis round-trips.

Redis schema details for this service:
- `freq:{userId}:{campaignId}` → integer count (string-encoded by Redis)
- `winhistory:{userId}` → sorted set, members are `"{campaignId}:{category}"`, score is unix-epoch-ms timestamp

The service does NOT enforce window bounds — Phase 3's Flink sink prunes old entries via `ZREMRANGEBYSCORE`. Phase 2 just reads what's there.

- [ ] **Step 1: Write `EnrichService.kt`**

```kotlin
package com.github.robran.adserver.frequency

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.EnrichResponse
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import org.slf4j.LoggerFactory

/**
 * Read-side implementation of the Frequency gRPC service. Consults Redis for per-campaign counts
 * and the user's recent-win history, returns both in one response.
 *
 * Phase 2 is read-only on the gRPC layer. Increments come from Phase 3's Flink sink via Lettuce
 * directly — see spec section 7.2.
 */
class EnrichService(private val redis: RedisClient) :
    FrequencyGrpcKt.FrequencyCoroutineImplBase() {

    private val log = LoggerFactory.getLogger(javaClass)

    override suspend fun enrichForAuction(request: EnrichRequest): EnrichResponse {
        val userId = request.userId
        require(userId.isNotEmpty()) { "user_id is required" }
        val campaignIds = request.campaignIdsList

        val freqCounts = if (campaignIds.isEmpty()) {
            emptyMap()
        } else {
            val freqKeys = campaignIds.map { "freq:$userId:$it" }
            val values = redis.mget(freqKeys)
            campaignIds.zip(values).mapNotNull { (campaignId, raw) ->
                val count = raw?.toIntOrNull() ?: return@mapNotNull null
                if (count <= 0) null else campaignId to count
            }.toMap()
        }

        // Read the entire winhistory zset; Phase 3 trims it to a 1h window so this is bounded.
        val winhistoryKey = "winhistory:$userId"
        val rawWins = redis.zrangeByScore(winhistoryKey, 0.0, Double.POSITIVE_INFINITY)
        val recentCategories = rawWins.mapNotNullTo(mutableSetOf()) { entry ->
            val sep = entry.indexOf(':')
            if (sep < 0) null else entry.substring(sep + 1)
        }

        return EnrichResponse.newBuilder()
            .putAllFreqCounts(freqCounts)
            .addAllRecentCategories(recentCategories)
            .build()
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./gradlew :frequency-service:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

(No tests yet — they live in Task 7's integration test, which exercises this service end-to-end through a real gRPC channel.)

```bash
git add frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/EnrichService.kt
git commit -m "Phase 2 task 6: EnrichService (read-side gRPC impl backed by Redis)"
```

---

## Task 7: gRPC Server Bootstrap + Integration Test

**Files:**
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt`
- Create: `frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt`
- Create: `frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/EnrichServiceIntegrationTest.kt`

This task wires the gRPC server bootstrap and verifies end-to-end via Testcontainers Redis + a real gRPC client connecting to a real (in-process) gRPC server.

- [ ] **Step 1: Write `AppConfig.kt`**

```kotlin
package com.github.robran.adserver.frequency

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(val port: Int)

data class RedisConfig(val url: String)

data class AppConfig(
    val server: ServerConfig,
    val redis: RedisConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig =
            raw.extract("frequency")
    }
}
```

- [ ] **Step 2: Write `Application.kt`**

```kotlin
package com.github.robran.adserver.frequency

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis)

    val server = NettyServerBuilder.forPort(config.server.port)
        .addService(service)
        .build()
        .start()

    log.info("frequency-service listening on port {} (redis={})", config.server.port, config.redis.url)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down frequency-service")
            server.shutdown()
            redis.close()
        },
    )

    server.awaitTermination()
}
```

- [ ] **Step 3: Write the failing integration test**

`frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/EnrichServiceIntegrationTest.kt`:

```kotlin
package com.github.robran.adserver.frequency

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// Tests use unique user-ids per test (user-A, user-B) to avoid cross-test contamination
// without needing FLUSHDB or per-test cleanup.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EnrichServiceIntegrationTest {

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private lateinit var redisClient: RedisClient
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var stub: FrequencyGrpcKt.FrequencyCoroutineStub

    private val serverName = InProcessServerBuilder.generateName()

    @BeforeAll
    fun setupClass() {
        redis.start()
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        redisClient = RedisClient.connect(url)

        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(EnrichService(redisClient))
            .build()
            .start()

        channel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build()
        stub = FrequencyGrpcKt.FrequencyCoroutineStub(channel)
    }

    @AfterAll
    fun tearDownClass() {
        channel.shutdownNow()
        server.shutdownNow()
        redisClient.close()
        redis.stop()
    }

    @Test
    fun `returns empty response for unknown user`() = runTest {
        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("never-seen").addCampaignIds("c1").build(),
        )
        assertThat(resp.freqCountsMap.toMap()).isEmpty()
        assertThat(resp.recentCategoriesList.toList()).isEmpty()
    }

    @Test
    fun `returns counts for campaigns with stored counters`() = runTest {
        redisClient.set("freq:user-A:c1", "5")
        redisClient.set("freq:user-A:c2", "2")
        // c3 has no entry → not in response

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder()
                .setUserId("user-A")
                .addAllCampaignIds(listOf("c1", "c2", "c3"))
                .build(),
        )
        assertThat(resp.freqCountsMap.toMap()).isEqualTo(mapOf("c1" to 5, "c2" to 2))
    }

    @Test
    fun `returns recent categories from winhistory zset`() = runTest {
        redisClient.zadd(
            "winhistory:user-A",
            "c1:IAB13" to 1000.0,
            "c2:IAB19" to 1500.0,
            "c1:IAB13" to 2000.0, // duplicate category — set semantics dedupe
        )

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("user-A").build(),
        )
        assertThat(resp.recentCategoriesList.toSet()).containsExactlyInAnyOrder("IAB13", "IAB19")
    }

    @Test
    fun `combined response: counts AND categories in one RPC`() = runTest {
        redisClient.set("freq:user-B:c1", "3")
        redisClient.zadd("winhistory:user-B", "c1:IAB1" to 1.0)

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("user-B").addCampaignIds("c1").build(),
        )
        assertThat(resp.freqCountsMap["c1"]).isEqualTo(3)
        assertThat(resp.recentCategoriesList.toSet()).containsExactlyInAnyOrder("IAB1")
    }

    @Test
    fun `empty user_id is rejected with INVALID_ARGUMENT`() = runTest {
        val ex = runCatching {
            stub.enrichForAuction(EnrichRequest.newBuilder().setUserId("").build())
        }.exceptionOrNull()
        // require() throws IllegalArgumentException, which gRPC surfaces as a StatusRuntimeException
        // with code UNKNOWN by default. We accept any throwable here as proof the call was rejected.
        assertThat(ex != null).isEqualTo(true)
    }
}
```

- [ ] **Step 4: Run the test to confirm it fails**

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.EnrichServiceIntegrationTest"`
Expected: FAIL until Application/server wiring is correct. (The `Application.kt` from Step 2 isn't actually invoked by this test — the test uses `InProcessServerBuilder` to run the service directly. But compilation of the Application is necessary.)

- [ ] **Step 5: Run the test to confirm it passes**

(Application.kt and EnrichService should already compile. The test should pass directly after writing it.)

Run: `./gradlew :frequency-service:test --tests "com.github.robran.adserver.frequency.EnrichServiceIntegrationTest"`
Expected: PASS, 5 tests green.

- [ ] **Step 6: Commit**

```bash
git add frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/AppConfig.kt \
        frequency-service/src/main/kotlin/com/github/robran/adserver/frequency/Application.kt \
        frequency-service/src/test/kotlin/com/github/robran/adserver/frequency/EnrichServiceIntegrationTest.kt
git commit -m "Phase 2 task 7: gRPC server bootstrap + Testcontainers integration test"
```

---

## Task 8: ad-server gRPC Client Dependencies

**Files:**
- Modify: `ad-server/build.gradle.kts`

- [ ] **Step 1: Add client dependencies**

In `ad-server/build.gradle.kts`, in the `dependencies { ... }` block, add the following lines in the production `implementation` section (alongside the existing Ktor/config/postgres lines):

```kotlin
    // Phase 2: gRPC client to frequency-service
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.kotlin.stub)
```

(No additions needed in `testImplementation` — `kotlinx-coroutines-test` is already there from Task 13 of Phase 1.)

- [ ] **Step 2: Verify it builds**

Run: `./gradlew :ad-server:build -x test`
Expected: BUILD SUCCESSFUL. The new dependencies resolve, but no source changes yet.

- [ ] **Step 3: Commit**

```bash
git add ad-server/build.gradle.kts
git commit -m "Phase 2 task 8: ad-server gRPC client dependencies"
```

---

## Task 9: `GrpcFrequencyClient` (real gRPC client with timeout + fail-open)

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`

The client implements the existing `FrequencyClient` interface. It enforces an 8 ms timeout (per spec) and fails open on any error: timeout, connection refused, gRPC status errors. Fail-open returns an empty `EnrichResult`, which means freq-cap and competitive-separation stages effectively become no-ops for that request — latency wins over freshness.

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt`:

```kotlin
package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.EnrichResponse
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.Server
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GrpcFrequencyClientTest {

    private lateinit var server: Server
    private lateinit var serverName: String
    private var fakeBehavior: suspend (EnrichRequest) -> EnrichResponse = { EnrichResponse.getDefaultInstance() }

    @BeforeEach
    fun setUp() {
        serverName = InProcessServerBuilder.generateName()
        val service = object : FrequencyGrpcKt.FrequencyCoroutineImplBase() {
            override suspend fun enrichForAuction(request: EnrichRequest): EnrichResponse =
                fakeBehavior(request)
        }
        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdownNow()
    }

    private fun newClient(timeoutMs: Long = 8L): GrpcFrequencyClient {
        val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        return GrpcFrequencyClient(channel, timeoutMs)
    }

    @Test
    fun `returns mapped EnrichResult on a successful RPC`() = runTest {
        fakeBehavior = { _ ->
            EnrichResponse.newBuilder()
                .putFreqCounts("c1", 3)
                .putFreqCounts("c2", 7)
                .addRecentCategories("IAB1")
                .addRecentCategories("IAB13-1")
                .build()
        }
        val client = newClient(timeoutMs = 5_000L) // generous for the success path

        val result = client.enrich("u1", listOf("c1", "c2"))

        assertThat(result.freqCounts).isEqualTo(mapOf("c1" to 3, "c2" to 7))
        assertThat(result.recentCategories).containsExactlyInAnyOrder("IAB1", "IAB13-1")
    }

    @Test
    fun `fails open on slow server (timeout exceeded)`() = runTest {
        fakeBehavior = { _ ->
            delay(50) // longer than the 8ms timeout
            EnrichResponse.newBuilder().putFreqCounts("c1", 99).build()
        }
        val client = newClient(timeoutMs = 8L)

        val result = client.enrich("u1", listOf("c1"))

        // Fail-open returns empty.
        assertThat(result.freqCounts).isEmpty()
        assertThat(result.recentCategories).isEmpty()
    }

    @Test
    fun `fails open on server error`() = runTest {
        fakeBehavior = { _ ->
            throw StatusRuntimeException(Status.UNAVAILABLE.withDescription("redis down"))
        }
        val client = newClient(timeoutMs = 5_000L)

        val result = client.enrich("u1", listOf("c1"))

        assertThat(result.freqCounts).isEmpty()
        assertThat(result.recentCategories).isEmpty()
    }

    @Test
    fun `passes user_id and campaign_ids through to the server`() = runTest {
        var captured: EnrichRequest? = null
        fakeBehavior = { req ->
            captured = req
            EnrichResponse.getDefaultInstance()
        }
        val client = newClient(timeoutMs = 5_000L)

        client.enrich("user-zalia", listOf("camp-001", "camp-002", "camp-003"))

        val req = captured!!
        assertThat(req.userId).isEqualTo("user-zalia")
        assertThat(req.campaignIdsList.toList()).containsExactlyInAnyOrder("camp-001", "camp-002", "camp-003")
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.GrpcFrequencyClientTest"`
Expected: FAIL — `GrpcFrequencyClient` does not exist.

- [ ] **Step 3: Write the implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt`:

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.ManagedChannel
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

/**
 * Real gRPC implementation of [FrequencyClient]. Calls the standalone frequency-service.
 * Enforces an 8 ms timeout (configurable for tests) and falls back to an empty [EnrichResult]
 * on any error (timeout, server error, channel failure). Per spec section 5.4: latency wins,
 * freshness loses.
 */
class GrpcFrequencyClient(
    channel: ManagedChannel,
    private val timeoutMs: Long = 8L,
) : FrequencyClient {

    private val log = LoggerFactory.getLogger(javaClass)
    private val stub = FrequencyGrpcKt.FrequencyCoroutineStub(channel)

    override suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult {
        val request = EnrichRequest.newBuilder()
            .setUserId(userId)
            .addAllCampaignIds(campaignIds)
            .build()
        return try {
            val response = withTimeout(timeoutMs) { stub.enrichForAuction(request) }
            EnrichResult(
                freqCounts = response.freqCountsMap.toMap(),
                recentCategories = response.recentCategoriesList.toSet(),
            )
        } catch (e: TimeoutCancellationException) {
            log.debug("frequency.fail_open: timeout after {}ms", timeoutMs)
            EnrichResult(freqCounts = emptyMap(), recentCategories = emptySet())
        } catch (e: Throwable) {
            log.debug("frequency.fail_open: {}", e.javaClass.simpleName)
            EnrichResult(freqCounts = emptyMap(), recentCategories = emptySet())
        }
    }
}
```

- [ ] **Step 4: Run the test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.GrpcFrequencyClientTest"`
Expected: PASS, 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClient.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/GrpcFrequencyClientTest.kt
git commit -m "Phase 2 task 9: GrpcFrequencyClient with 8ms timeout and fail-open"
```

---

## Task 10: Wire `GrpcFrequencyClient` into `Application.kt`

**Files:**
- Modify: `ad-server/src/main/resources/application.conf`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`

`buildPipeline` is already a top-level function in Phase 1's `Application.kt` (the integration test calls it). We parameterize it to accept a `FrequencyClient`, so production code passes a `GrpcFrequencyClient` and tests can keep using `FakeFrequencyClient`.

- [ ] **Step 1: Update `application.conf`**

Append a new `frequency` block to `ad-server/src/main/resources/application.conf`:

```hocon
adserver {
    server {
        host = "0.0.0.0"
        port = 8080
        port = ${?ADSERVER_PORT}
    }
    inventory {
        jdbcUrl = "jdbc:postgresql://localhost:5432/kotlin_ad_server"
        jdbcUrl = ${?INVENTORY_JDBC_URL}
        user = "kotlin_ad_server"
        user = ${?INVENTORY_DB_USER}
        password = "kotlin_ad_server"
        password = ${?INVENTORY_DB_PASSWORD}
        skipMigrate = false
        skipMigrate = ${?INVENTORY_SKIP_MIGRATE}
    }
    frequency {
        host = "localhost"
        host = ${?FREQ_SERVICE_HOST}
        port = 9090
        port = ${?FREQ_SERVICE_PORT}
        timeoutMs = 8
        timeoutMs = ${?FREQ_SERVICE_TIMEOUT_MS}
    }
}
```

- [ ] **Step 2: Update `AppConfig.kt`**

In `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`, add a `FrequencyConfig` data class and include it in `AppConfig`:

```kotlin
package com.github.robran.adserver

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(
    val host: String,
    val port: Int,
)

data class InventoryConfig(
    val jdbcUrl: String,
    val user: String,
    val password: String,
    val skipMigrate: Boolean,
)

data class FrequencyConfig(
    val host: String,
    val port: Int,
    val timeoutMs: Long,
)

data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
    val frequency: FrequencyConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig =
            raw.extract("adserver")
    }
}
```

- [ ] **Step 3: Update `Application.kt`**

Replace the entire contents of `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt` with:

```kotlin
package com.github.robran.adserver

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.CandidateBuilder
import com.github.robran.adserver.auction.FrequencyClient
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.http.HealthState
import com.github.robran.adserver.http.bidRoutes
import com.github.robran.adserver.http.healthRoutes
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.random.Random

private val log = LoggerFactory.getLogger("com.github.robran.adserver.Application")

fun main() {
    val config = AppConfig.load()

    val snapshot = InventoryLoader.pooledDataSource(
        config.inventory.jdbcUrl,
        config.inventory.user,
        config.inventory.password,
    ).use { ds ->
        if (!config.inventory.skipMigrate) {
            InventoryLoader.migrate(
                config.inventory.jdbcUrl,
                config.inventory.user,
                config.inventory.password,
            )
        }
        InventoryLoader(ds).load()
    }

    val frequencyChannel = NettyChannelBuilder
        .forAddress(config.frequency.host, config.frequency.port)
        .usePlaintext()
        .build()
    val frequencyClient = GrpcFrequencyClient(frequencyChannel, timeoutMs = config.frequency.timeoutMs)
    val pipeline = buildPipeline(snapshot, frequencyClient)

    log.info(
        "ad-server starting: {} campaigns loaded, frequency-service @ {}:{}",
        snapshot.size,
        config.frequency.host,
        config.frequency.port,
    )

    val healthState = HealthState().apply { ready.set(true) }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down ad-server")
            frequencyChannel.shutdown()
        },
    )

    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, pipeline)
    }.start(wait = true)
}

/**
 * Builds the auction pipeline with a caller-supplied [FrequencyClient].
 * Production wires a [GrpcFrequencyClient]; tests wire a fake.
 */
fun buildPipeline(snapshot: InventorySnapshot, frequencyClient: FrequencyClient): AuctionPipeline =
    AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages = listOf(
            BlockingPolicyStage(),
            FrequencyAndCompsepStage(frequencyClient),
            FloorPriceStage(),
            SelectionStage(Random.Default),
        ),
    )

fun Application.adServerModule(
    healthState: HealthState,
    pipeline: AuctionPipeline,
) {
    install(ContentNegotiation) {
        json(
            Json {
                ignoreUnknownKeys = true
                encodeDefaults = false
                explicitNulls = false
            },
        )
    }
    install(CallLogging)
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(
                io.ktor.http.HttpStatusCode.BadRequest,
                mapOf("error" to "invalid_request", "message" to (cause.message ?: "bad request")),
            )
        }
    }

    routing {
        healthRoutes(healthState)
        bidRoutes(pipeline)
    }
}
```

- [ ] **Step 4: Update `BidRouteIntegrationTest.kt` to pass a fake client**

Phase 1's test already calls `buildPipeline(snapshot)`. The signature change requires passing a fake. Find this block in `ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt`:

```kotlin
        InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                val snapshot = InventoryLoader(ds).load()
                pipeline = buildPipeline(snapshot)
            }
```

Replace the `buildPipeline(snapshot)` line with:

```kotlin
                pipeline = buildPipeline(snapshot, com.github.robran.adserver.auction.FakeFrequencyClient())
```

(Or, to keep it readable, add an `import com.github.robran.adserver.auction.FakeFrequencyClient` near the other `com.github.robran.adserver.*` imports and use the unqualified name.)

- [ ] **Step 5: Run all ad-server tests**

Run: `./gradlew :ad-server:test`
Expected: BUILD SUCCESSFUL — all unit and integration tests still pass with the new wiring.

- [ ] **Step 6: Run a `:ad-server:build` to make sure ktlint and the main artifact assemble**

Run: `./gradlew :ad-server:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add ad-server/src/main/resources/application.conf \
        ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt
git commit -m "Phase 2 task 10: wire GrpcFrequencyClient into Application + frequency config"
```

---

## Task 11: End-to-End Smoke Test (ad-server + frequency-service + Postgres + Redis)

**Files:**
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase2EndToEndTest.kt`

This test brings up the entire Phase-1+Phase-2 stack: real Postgres (Testcontainers), real Redis (Testcontainers), a real frequency-service running in-process, and the ad-server Ktor app via `testApplication`. It then verifies that pre-loading Redis with a frequency cap actually filters that campaign out of the auction.

The new module dependency direction: `ad-server` test depends on `frequency-service` so we can construct an in-process gRPC server with the real `EnrichService`. Add this dependency in Step 1.

- [ ] **Step 1: Add a test dependency on `frequency-service`**

In `ad-server/build.gradle.kts`, in the `testImplementation` section, add:

```kotlin
    testImplementation(project(":frequency-service"))
```

- [ ] **Step 2: Write the test**

`ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase2EndToEndTest.kt`:

```kotlin
package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.frequency.EnrichService
import com.github.robran.adserver.frequency.RedisClient
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.User
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Phase2EndToEndTest {

    private val postgres: PostgreSQLContainer<*> =
        PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("kotlin_ad_server_test")
            .withUsername("test")
            .withPassword("test")

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private lateinit var redisClient: RedisClient
    private lateinit var freqServer: Server
    private lateinit var freqChannel: ManagedChannel
    private lateinit var pipeline: AuctionPipeline

    private val serverName = InProcessServerBuilder.generateName()

    @BeforeAll
    fun setup() {
        postgres.start()
        redis.start()

        // Hydrate inventory.
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        val snapshot = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                InventoryLoader(ds).load()
            }

        // Start a real frequency-service backed by the test Redis container.
        val redisUrl = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        redisClient = RedisClient.connect(redisUrl)
        freqServer = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(EnrichService(redisClient))
            .build()
            .start()

        // Wire ad-server to the in-process frequency service via a real gRPC channel.
        freqChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        val grpcClient = GrpcFrequencyClient(freqChannel, timeoutMs = 1_000L) // generous for tests
        pipeline = buildPipeline(snapshot, grpcClient)
    }

    @AfterAll
    fun tearDown() {
        freqChannel.shutdownNow()
        freqServer.shutdownNow()
        redisClient.close()
        redis.stop()
        postgres.stop()
    }

    private fun banner300x250Request(userId: String) = BidRequest(
        id = "req-${System.nanoTime()}",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
        user = User(id = userId),
    )

    @Test
    fun `auction returns a winner when no caps are set`() = testApplication {
        application { adServerModule(HealthState().apply { ready.set(true) }, pipeline) }
        val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request(userId = "fresh-user"))
        }
        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(1)
        assertThat(body.seatbid[0].bid[0].cid).isNotNull()
    }

    @Test
    fun `auction skips a campaign whose freq cap is hit`() = testApplication {
        application { adServerModule(HealthState().apply { ready.set(true) }, pipeline) }
        val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

        // Determine the user, then pre-populate Redis with caps for ALL campaigns so the auction
        // ends in a no-fill at the freq+compsep stage.
        val userId = "capped-user"
        // Seed inventory has 50 campaigns; their IDs follow camp-001 .. camp-050.
        runBlocking {
            for (i in 1..50) {
                val cid = "camp-%03d".format(i)
                redisClient.set("freq:$userId:$cid", "999")
            }
        }

        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request(userId = userId))
        }
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(0)
        assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
    }

    @Test
    fun `competitive separation drops candidates whose category is in winhistory`() = testApplication {
        application { adServerModule(HealthState().apply { ready.set(true) }, pipeline) }
        val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

        // Populate winhistory with every IAB category in the seed: this should starve every
        // candidate at the competitive-separation step and produce a no-fill.
        val userId = "saturated-user"
        val seedCategories = listOf(
            "IAB13", "IAB13-1", "IAB13-2", "IAB13-3", "IAB13-7", "IAB13-9",
            "IAB19", "IAB19-6", "IAB19-15", "IAB19-30",
            "IAB8", "IAB8-1", "IAB8-5", "IAB8-9",
            "IAB2", "IAB2-2", "IAB2-5",
            "IAB20", "IAB20-1", "IAB20-3", "IAB20-26",
            "IAB5", "IAB5-1", "IAB5-3", "IAB5-13",
            "IAB16", "IAB16-1", "IAB16-7",
        )
        runBlocking {
            redisClient.zadd(
                "winhistory:$userId",
                *seedCategories.mapIndexed { i, c -> "fake-camp-$i:$c" to i.toDouble() }.toTypedArray(),
            )
        }

        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request(userId = userId))
        }
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(0)
        assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
    }
}
```

- [ ] **Step 3: Run the test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.http.Phase2EndToEndTest"`
Expected: PASS, 3 tests green. (Requires Docker daemon for Postgres + Redis containers.)

- [ ] **Step 4: Run the full suite to confirm no regressions**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL across all four modules (`common-protocol`, `inventory-loader`, `ad-server`, `frequency-service`).

- [ ] **Step 5: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase2EndToEndTest.kt
git commit -m "Phase 2 task 11: end-to-end test with real gRPC + Redis + Postgres"
```

---

## Task 12: README Update + Smoke-Test Script Update

**Files:**
- Modify: `README.md`
- Modify: `scripts/smoke-test.sh`

- [ ] **Step 1: Update `README.md`**

In the `## Status` section, change Phase 2 from `⏳` to `✅`:

```markdown
## Status

- ✅ **Phase 1 — Skeleton + hot path**
- ✅ **Phase 2 — Frequency service + Redis** (this commit)
- ⏳ Phase 3 — Kafka + Flink aggregator
- ⏳ Phase 4 — Observability (Micrometer, OpenTelemetry, Jaeger, Prometheus, Grafana)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README
```

In the `## Modules` section, add the new module:

```markdown
- `frequency-service` — standalone gRPC service (port 9090) backed by Lettuce → Redis. Owns the per-user impression counters and recent-win history. Read-only on the gRPC layer in Phase 2; Phase 3 adds Flink-driven increments.
```

In the `## Run` section, update the prerequisites and the run instructions:

```markdown
## Run

```bash
# Postgres (inventory)
docker run -d --name kotlin-ad-pg \
    -e POSTGRES_USER=kotlin_ad_server \
    -e POSTGRES_PASSWORD=kotlin_ad_server \
    -e POSTGRES_DB=kotlin_ad_server \
    -p 5432:5432 \
    postgres:16-alpine

# Redis (frequency counters + winhistory)
docker run -d --name kotlin-ad-redis -p 6379:6379 redis:7-alpine

# Run the frequency service first (ad-server tries to connect on boot)
./gradlew :frequency-service:run &

# Then run the ad-server
./gradlew :ad-server:run
```
```

- [ ] **Step 2: Smoke-test script unchanged**

The existing `scripts/smoke-test.sh` runs `./gradlew test`, which now includes the new `frequency-service` module's tests + the `Phase2EndToEndTest`. No edits needed.

- [ ] **Step 3: Run the smoke test**

Run: `./scripts/smoke-test.sh`
Expected: "Phase 1 smoke test PASSED" (the script's success line is generic — keep it as-is, or update to `Phase 1+2 smoke test PASSED` if you want).

If you want to update the success line, edit `scripts/smoke-test.sh` and change the final `echo` line from:

```bash
echo "==> Phase 1 smoke test PASSED. Inventory loaded, BidRequest → BidResponse round-trip verified."
```

to:

```bash
echo "==> Phase 1+2 smoke test PASSED. Inventory + frequency-service + auction round-trip verified."
```

- [ ] **Step 4: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 2 task 12: README + smoke-test script update for Phase 2"
```

---

## Phase 2 Done

Working software at the end of Phase 2:

- **`frequency-service`** — standalone gRPC server on port 9090 backed by Lettuce → Redis
- **`common-protocol`** — generates Java + Kotlin gRPC stubs from `frequency.proto`
- **`ad-server`** — `GrpcFrequencyClient` with 8 ms timeout + fail-open replaces the Phase 1 stub
- **End-to-end demo** — `redis-cli SET freq:user-X:camp-001 99` → that user no longer wins `camp-001` in subsequent auctions
- **Tests:** ~6 new RedisClient tests, 5 new EnrichService integration tests, 4 new GrpcFrequencyClient unit tests, 3 new Phase 2 e2e tests. Phase 1 tests untouched.

**Next:** Phase 3 — Kafka + Avro + Flink impression aggregator. The Flink job consumes `impression-events`, windows by user+campaign, and writes increments back to Redis (the same Redis that frequency-service reads). Will need its own plan.
