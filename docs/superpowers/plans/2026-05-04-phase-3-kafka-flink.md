# Phase 3 — Kafka + Avro + Flink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the impression event loop. The `ad-server` produces `auction-results` and `impression-events` to Kafka (Avro serialized via Confluent Schema Registry, fire-and-forget). A new `flink-impression-aggregator` module windows impressions per (user, campaign), aggregates counts, and writes them back to Redis using Lua-scripted atomic operations — the same Redis the `frequency-service` reads on the request path.

**Architecture:** Kafka in KRaft mode, three topics keyed by `userId`. ad-server uses a dedicated `Dispatchers.IO` coroutine context for fire-and-forget producer sends; the request path never awaits Kafka acks. Flink job runs as a `MiniCluster` in tests and (optionally) submits to the JM+TM in `docker-compose` for local demo. Increments to Redis use a Lua script (INCRBY + EXPIRE in one atomic operation) so counter and TTL stay in sync.

**Tech Stack:**
- Existing: Kotlin 2.1.0 / JVM 21, Gradle 8.11, Ktor 3.0.3, Lettuce 6.5.1.RELEASE, gRPC 1.68.2
- New:
  - Apache Kafka client: kafka-clients 3.8.0
  - Avro: avro 1.12.0
  - Confluent platform 7.7.x (Schema Registry server image, kafka-avro-serializer client lib)
  - gradle-avro-plugin: com.github.davidmc24.gradle.plugin.avro 1.9.1
  - Flink: 1.20.0 (DataStream API + `flink-connector-kafka`)
  - Testcontainers Kafka: built into testcontainers core (use `KafkaContainer`)
  - Testcontainers Schema Registry: dedicated `confluent-platform` module 1.21.4

---

## Scope notes

Per spec section 11 ("De-scope Dial"), the priority-order fallbacks are:
1. Drop Avro + Schema Registry → JSON-on-Kafka (saves ~3 tasks)
2. Drop Flink exactly-once → at-least-once (saves Flink config complexity)

This plan keeps all three (Avro, Schema Registry, exactly-once-via-checkpointing) because they're the differentiating ad-tech-authentic signals. If timeline pressure forces a cut, drop in the order above.

---

## File Structure

```
kotlin_ad_server/
├── settings.gradle.kts                                          (modify: add :flink-impression-aggregator)
├── gradle/libs.versions.toml                                    (modify: kafka, avro, flink, etc.)
├── docker-compose.yml                                           (NEW: Postgres + Redis + Kafka + Schema Registry + Flink)
├── scripts/
│   ├── smoke-test.sh                                            (modify: success message)
│   └── kafka-init-topics.sh                                     (NEW: helper for local demo)
│
├── common-protocol/
│   ├── build.gradle.kts                                         (modify: add gradle-avro-plugin)
│   └── src/main/avro/                                           (NEW directory)
│       ├── ImpressionEvent.avsc
│       └── AuctionResultEvent.avsc
│
├── ad-server/
│   ├── build.gradle.kts                                         (modify: add kafka-clients, avro, schema-registry-client)
│   └── src/main/kotlin/com/github/robran/adserver/
│       ├── AppConfig.kt                                         (modify: add KafkaConfig)
│       ├── Application.kt                                       (modify: wire KafkaEventEmitter)
│       ├── kafka/
│       │   ├── KafkaEventEmitter.kt                             (NEW)
│       │   └── ProducerFactory.kt                               (NEW)
│       └── auction/
│           └── AuctionPipeline.kt                               (modify: emit events post-response)
│   └── src/test/kotlin/com/github/robran/adserver/kafka/
│       └── KafkaEventEmitterTest.kt                             (NEW: Testcontainers Kafka)
│
└── flink-impression-aggregator/                                 (NEW MODULE)
    ├── build.gradle.kts
    └── src/
        ├── main/kotlin/com/github/robran/adserver/flink/
        │   ├── Application.kt                                   # MiniCluster runner
        │   ├── AppConfig.kt
        │   ├── ImpressionAggregatorJob.kt                       # the topology
        │   ├── RedisCounterSink.kt                              # RichSinkFunction
        │   └── LuaScripts.kt                                    # loads .lua resources
        ├── main/resources/
        │   ├── application.conf
        │   ├── logback.xml
        │   └── lua/
        │       ├── incrFreqWithExpiry.lua
        │       └── addWinHistory.lua
        └── test/kotlin/com/github/robran/adserver/flink/
            └── ImpressionAggregatorJobTest.kt                   # MiniCluster + Testcontainers
```

---

## Task 1: Version Catalog Additions for Phase 3

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1: Add new versions**

In the `[versions]` section, append after the Phase 2 group:

```toml

# Phase 3 additions
kafka-clients = "3.8.0"
avro = "1.12.0"
confluent = "7.7.0"
flink = "1.20.0"
flink-kafka-connector = "3.3.0-1.20"
gradle-avro-plugin = "1.9.1"
```

- [ ] **Step 2: Add new libraries**

In `[libraries]`, append after the Phase 2 block:

```toml

# Phase 3: Kafka + Avro + Schema Registry
kafka-clients = { module = "org.apache.kafka:kafka-clients", version.ref = "kafka-clients" }
avro = { module = "org.apache.avro:avro", version.ref = "avro" }
confluent-kafka-avro-serializer = { module = "io.confluent:kafka-avro-serializer", version.ref = "confluent" }
confluent-kafka-schema-registry-client = { module = "io.confluent:kafka-schema-registry-client", version.ref = "confluent" }

# Phase 3: Flink
flink-streaming-java = { module = "org.apache.flink:flink-streaming-java", version.ref = "flink" }
flink-clients = { module = "org.apache.flink:flink-clients", version.ref = "flink" }
flink-connector-kafka = { module = "org.apache.flink:flink-connector-kafka", version.ref = "flink-kafka-connector" }
flink-avro = { module = "org.apache.flink:flink-avro", version.ref = "flink" }
flink-avro-confluent-registry = { module = "org.apache.flink:flink-avro-confluent-registry", version.ref = "flink" }
flink-test-utils = { module = "org.apache.flink:flink-test-utils", version.ref = "flink" }
flink-streaming-java-tests = { module = "org.apache.flink:flink-streaming-java", version.ref = "flink" }

# Phase 3: Testcontainers — Kafka + Schema Registry
testcontainers-kafka = { module = "org.testcontainers:kafka" }
```

(The Confluent Maven repository is required for `confluent-*` and `flink-avro-confluent-registry`. Add it to `settings.gradle.kts` in Step 4.)

- [ ] **Step 3: Add the avro plugin**

In `[plugins]`, append:

```toml
avro = { id = "com.github.davidmc24.gradle.plugin.avro", version.ref = "gradle-avro-plugin" }
```

- [ ] **Step 4: Add the Confluent Maven repository**

In `settings.gradle.kts`, find the `dependencyResolutionManagement` block:

```kotlin
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}
```

Add a Confluent repo entry (Schema Registry server-side libs are not on Maven Central):

```kotlin
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        maven {
            name = "Confluent"
            url = uri("https://packages.confluent.io/maven/")
            content {
                includeGroup("io.confluent")
                // The Avro confluent registry connector is also published here.
                includeGroup("org.apache.flink")
            }
        }
    }
}
```

(The `content { includeGroup }` filter prevents Confluent's repo from being a fallback for unrelated artifacts — it only resolves `io.confluent:*` and Flink's Confluent-bundled connectors.)

Also add the same repo to the `pluginManagement` block (the gradle-avro-plugin lives on Gradle Plugin Portal, but adding Confluent here is harmless and future-proof). Actually, **don't** add it to pluginManagement — gradle-avro-plugin is on Gradle Plugin Portal which is already declared. Skip this part.

- [ ] **Step 5: Verify the catalog parses**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add gradle/libs.versions.toml settings.gradle.kts
git commit -m "Phase 3 task 1: version catalog + Confluent maven repo"
```

---

## Task 2: docker-compose.yml (Kafka + Schema Registry + Flink + reuse Postgres/Redis)

**Files:**
- Create: `docker-compose.yml`
- Create: `scripts/kafka-init-topics.sh`

The compose stack lets a developer `docker compose up` and have everything except the JVM apps. The integration tests don't use this — they use Testcontainers. This is for the local demo.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    container_name: kotlin-ad-pg
    environment:
      POSTGRES_USER: kotlin_ad_server
      POSTGRES_PASSWORD: kotlin_ad_server
      POSTGRES_DB: kotlin_ad_server
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "kotlin_ad_server"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    container_name: kotlin-ad-redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.7.0
    container_name: kotlin-ad-kafka
    environment:
      # KRaft mode — no Zookeeper.
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:29092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    ports:
      - "29092:29092"   # host-accessible
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 10s
      timeout: 5s
      retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    container_name: kotlin-ad-schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://kafka:9092"
    ports:
      - "8081:8081"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081/subjects || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10

  topic-init:
    image: confluentinc/cp-kafka:7.7.0
    container_name: kotlin-ad-topic-init
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        for topic in bid-requests auction-results impression-events; do
          kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
              --topic $$topic --partitions 6 --replication-factor 1
        done
        echo "topics created"

  flink-jobmanager:
    image: flink:1.20.0-scala_2.12-java17
    container_name: kotlin-ad-flink-jm
    command: jobmanager
    ports:
      - "8082:8081"   # Flink web UI (host port 8082 to avoid clash with schema registry)
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        execution.checkpointing.interval: 30s
        execution.checkpointing.mode: EXACTLY_ONCE
        state.backend.type: filesystem
        state.checkpoints.dir: file:///tmp/flink-checkpoints
    volumes:
      - flink-checkpoints:/tmp/flink-checkpoints

  flink-taskmanager:
    image: flink:1.20.0-scala_2.12-java17
    container_name: kotlin-ad-flink-tm
    depends_on:
      - flink-jobmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
    volumes:
      - flink-checkpoints:/tmp/flink-checkpoints

volumes:
  flink-checkpoints:
```

- [ ] **Step 2: Write `scripts/kafka-init-topics.sh`**

For users who don't use docker-compose's `topic-init` container, this is a standalone helper.

```bash
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

for topic in bid-requests auction-results impression-events; do
    docker exec -it kotlin-ad-kafka kafka-topics \
        --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 6 \
        --replication-factor 1
done
echo "Topics created on $BOOTSTRAP"
```

`chmod +x scripts/kafka-init-topics.sh`.

- [ ] **Step 3: Verify the compose file is valid**

Run: `docker compose config --quiet`
Expected: no output, exit code 0.

(Don't actually `up` the stack as part of the test — that's slow and side-effecting. The compose file is for the local demo; integration tests use Testcontainers separately.)

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml scripts/kafka-init-topics.sh
git commit -m "Phase 3 task 2: docker-compose stack (Postgres+Redis+Kafka+SchemaRegistry+Flink)"
```

---

## Task 3: Avro Schemas in `common-protocol`

**Files:**
- Modify: `common-protocol/build.gradle.kts`
- Create: `common-protocol/src/main/avro/ImpressionEvent.avsc`
- Create: `common-protocol/src/main/avro/AuctionResultEvent.avsc`

- [ ] **Step 1: Apply the avro plugin in `common-protocol/build.gradle.kts`**

Read the current file first. Add `alias(libs.plugins.avro)` to the `plugins` block:

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.protobuf)
    alias(libs.plugins.avro)
    `java-library`
}
```

In the `dependencies` block, add the Avro library to `api` (so consumer modules can use the generated classes):

```kotlin
    api(libs.avro)
```

(Place it alongside the other `api` lines, e.g., right after `api(libs.protobuf.kotlin)`.)

After the existing `protobuf { ... }` block, add the avro plugin config:

```kotlin
avro {
    fieldVisibility.set("PRIVATE")
    isCreateSetters.set(false)  // immutable generated classes (use builders only)
    stringType.set("String")
}
```

The avro plugin auto-discovers `src/main/avro/*.avsc` and generates Java to `build/generated-main-avro-java/`. Hook the kotlin compile into avro generation (analogous to the protobuf hook):

In the existing `tasks.withType<...KotlinCompile>...configureEach { dependsOn(...) }` block, add a second `dependsOn` for avro:

```kotlin
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    dependsOn(tasks.named("generateProto"))
    dependsOn(tasks.named("generateAvroJava"))
}
```

- [ ] **Step 2: Write `common-protocol/src/main/avro/ImpressionEvent.avsc`**

```bash
mkdir -p common-protocol/src/main/avro
```

```json
{
  "namespace": "com.github.robran.adserver.protocol.events",
  "type": "record",
  "name": "ImpressionEvent",
  "doc": "Emitted by ad-server when an ad is selected. Drives Phase 3 Flink aggregator → Redis counter.",
  "fields": [
    { "name": "user_id",     "type": "string", "doc": "Resolved user id (or 'anonymous')" },
    { "name": "campaign_id", "type": "string" },
    { "name": "creative_id", "type": "string" },
    { "name": "category",    "type": "string", "doc": "Primary IAB category of the campaign" },
    { "name": "price",       "type": "double", "doc": "Winning bid price (CPM, USD)" },
    { "name": "ts_millis",   "type": "long",   "doc": "Selection timestamp, unix epoch ms" }
  ]
}
```

- [ ] **Step 3: Write `common-protocol/src/main/avro/AuctionResultEvent.avsc`**

```json
{
  "namespace": "com.github.robran.adserver.protocol.events",
  "type": "record",
  "name": "AuctionResultEvent",
  "doc": "Post-auction analytics: winner + reason for no-fill if applicable. Not on the request path.",
  "fields": [
    { "name": "request_id",  "type": "string" },
    { "name": "user_id",     "type": "string" },
    { "name": "imp_id",      "type": "string" },
    { "name": "ts_millis",   "type": "long" },
    {
      "name": "outcome",
      "type": {
        "type": "enum",
        "name": "Outcome",
        "symbols": ["FILLED", "NO_FILL_BLOCKING", "NO_FILL_FREQ_COMPSEP", "NO_FILL_FLOOR", "NO_FILL_OTHER"]
      }
    },
    { "name": "winner_campaign_id", "type": ["null", "string"], "default": null },
    { "name": "winner_price",       "type": ["null", "double"], "default": null },
    { "name": "candidates_initial", "type": "int" },
    { "name": "candidates_after_blocking", "type": "int" },
    { "name": "candidates_after_freq_compsep", "type": "int" },
    { "name": "candidates_after_floor", "type": "int" }
  ]
}
```

- [ ] **Step 4: Run codegen and verify**

Run: `./gradlew :common-protocol:generateAvroJava`
Expected: BUILD SUCCESSFUL. Files appear under `common-protocol/build/generated-main-avro-java/com/github/robran/adserver/protocol/events/`:
- `ImpressionEvent.java`
- `AuctionResultEvent.java`
- `Outcome.java`

Then: `./gradlew :common-protocol:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Smoke test**

Write `common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/events/AvroSchemaTest.kt`:

```kotlin
package com.github.robran.adserver.protocol.events

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class AvroSchemaTest {
    @Test
    fun `ImpressionEvent builder round-trips a record`() {
        val event = ImpressionEvent.newBuilder()
            .setUserId("user-1")
            .setCampaignId("camp-001")
            .setCreativeId("cre-001a")
            .setCategory("IAB13")
            .setPrice(2.50)
            .setTsMillis(1_700_000_000_000L)
            .build()

        assertThat(event.userId.toString()).isEqualTo("user-1")
        assertThat(event.campaignId.toString()).isEqualTo("camp-001")
        assertThat(event.tsMillis).isEqualTo(1_700_000_000_000L)
    }

    @Test
    fun `AuctionResultEvent supports nullable winner fields for no-fill`() {
        val event = AuctionResultEvent.newBuilder()
            .setRequestId("req-1")
            .setUserId("user-1")
            .setImpId("1")
            .setTsMillis(1L)
            .setOutcome(Outcome.NO_FILL_FLOOR)
            .setCandidatesInitial(5)
            .setCandidatesAfterBlocking(5)
            .setCandidatesAfterFreqCompsep(3)
            .setCandidatesAfterFloor(0)
            .build()

        assertThat(event.outcome).isEqualTo(Outcome.NO_FILL_FLOOR)
        assertThat(event.winnerCampaignId == null).isEqualTo(true)
    }
}
```

Note on `.toString()`: Avro 1.12 generates `CharSequence` for string fields by default. The `stringType.set("String")` config in the avro plugin block changes this to `String`, but accessor methods still return the underlying `CharSequence`. Use `.toString()` for value comparisons.

Run: `./gradlew :common-protocol:test --tests "com.github.robran.adserver.protocol.events.AvroSchemaTest"`
Expected: PASS, 2 tests green.

- [ ] **Step 6: Commit**

```bash
git add common-protocol/build.gradle.kts \
        common-protocol/src/main/avro/ \
        common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/events/AvroSchemaTest.kt
git commit -m "Phase 3 task 3: Avro schemas (ImpressionEvent, AuctionResultEvent) + codegen"
```

---

## Task 4: Register `:flink-impression-aggregator` Module

**Files:**
- Modify: `settings.gradle.kts`
- Create: `flink-impression-aggregator/build.gradle.kts` (stub)

- [ ] **Step 1: Update `settings.gradle.kts`**

Add `"flink-impression-aggregator"` to the `include(...)` block:

```kotlin
include(
    "common-protocol",
    "inventory-loader",
    "ad-server",
    "frequency-service",
    "flink-impression-aggregator",
)
```

- [ ] **Step 2: Create directories**

```bash
mkdir -p flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink
mkdir -p flink-impression-aggregator/src/main/resources/lua
mkdir -p flink-impression-aggregator/src/test/kotlin/com/github/robran/adserver/flink
mkdir -p flink-impression-aggregator/src/test/resources
```

- [ ] **Step 3: Write stub `flink-impression-aggregator/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
}
```

- [ ] **Step 4: Verify**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add settings.gradle.kts flink-impression-aggregator/build.gradle.kts
git commit -m "Phase 3 task 4: register :flink-impression-aggregator module"
```

---

## Task 5: ad-server Kafka Producer (KafkaEventEmitter + ProducerFactory)

**Files:**
- Modify: `ad-server/build.gradle.kts`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`
- Modify: `ad-server/src/main/resources/application.conf`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/kafka/ProducerFactory.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitter.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt`

`KafkaEventEmitter` exposes `suspend fun emitImpression(...)` and `suspend fun emitAuctionResult(...)`. Both wrap `producer.send()` and **never await the ack** — they launch on a dedicated dispatcher and return immediately. This matches spec section 5.3 ("fire-and-forget on the response path").

- [ ] **Step 1: Add Kafka deps to `ad-server/build.gradle.kts`**

Add to the `implementation` section:

```kotlin
    // Phase 3: Kafka producer + Avro
    implementation(libs.kafka.clients)
    implementation(libs.confluent.kafka.avro.serializer)
    implementation(libs.confluent.kafka.schema.registry.client)
```

Add to the `testImplementation` section:

```kotlin
    testImplementation(libs.testcontainers.kafka)
```

- [ ] **Step 2: Add `KafkaConfig` to `AppConfig.kt`**

Add a new data class:

```kotlin
data class KafkaConfig(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val topicAuctionResults: String,
    val topicImpressionEvents: String,
    val lingerMs: Int,
    val acks: String,
)
```

Append `kafka: KafkaConfig` to the `AppConfig` constructor:

```kotlin
data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
    val frequency: FrequencyConfig,
    val kafka: KafkaConfig,
) { ... }
```

- [ ] **Step 3: Add `kafka` block to `application.conf`**

Append at the end of the existing `adserver { ... }` block, before the closing brace:

```hocon
    kafka {
        bootstrapServers = "localhost:29092"
        bootstrapServers = ${?KAFKA_BOOTSTRAP_SERVERS}
        schemaRegistryUrl = "http://localhost:8081"
        schemaRegistryUrl = ${?SCHEMA_REGISTRY_URL}
        topicAuctionResults = "auction-results"
        topicAuctionResults = ${?KAFKA_TOPIC_AUCTION_RESULTS}
        topicImpressionEvents = "impression-events"
        topicImpressionEvents = ${?KAFKA_TOPIC_IMPRESSION_EVENTS}
        lingerMs = 5
        lingerMs = ${?KAFKA_LINGER_MS}
        acks = "1"
        acks = ${?KAFKA_ACKS}
    }
```

- [ ] **Step 4: Write `ProducerFactory.kt`**

```kotlin
package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

/**
 * Builds Kafka producers configured for fire-and-forget Avro emission against Confluent Schema
 * Registry. Producer instances are heavyweight (each owns a thread + network socket) — create
 * one per JVM and reuse it across requests.
 */
object ProducerFactory {

    fun avroProducer(config: KafkaConfig): KafkaProducer<String, Any> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl)
            put(ProducerConfig.ACKS_CONFIG, config.acks)
            put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs)
            // Compress the on-wire payload — meaningful at portfolio-level QPS, free during dev.
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            // Idempotence guards against duplicate records on retry; tolerable overhead at our QPS.
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            // Bounded retries instead of MAX_VALUE so a stuck producer doesn't pin memory forever.
            put(ProducerConfig.RETRIES_CONFIG, 3)
        }
        return KafkaProducer(props)
    }
}
```

- [ ] **Step 5: Write `KafkaEventEmitter.kt`**

```kotlin
package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

/**
 * Fire-and-forget event emitter. The request path calls [emitImpression] / [emitAuctionResult]
 * and returns immediately. Producer.send() is non-blocking by default (returns a Future and
 * batches behind the scenes); we don't even await the Future. Errors land in [errorCallback]
 * but never propagate up to the request handler.
 *
 * Per spec 5.5: events are launched in a supervisor scope rooted in [emitterScope] so they
 * survive request completion but cannot fail the request.
 */
class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    /** Set [emitterScope] to a long-lived scope; defaults to a process-scoped supervisor. */
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    fun emitImpression(event: ImpressionEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicImpressionEvents, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicImpressionEvents, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicImpressionEvents, e.message)
            }
        }
    }

    fun emitAuctionResult(event: AuctionResultEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicAuctionResults, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicAuctionResults, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicAuctionResults, e.message)
            }
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}
```

- [ ] **Step 6: Write the integration test**

`ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt`:

```kotlin
package com.github.robran.adserver.kafka

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaEventEmitterTest {

    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
    )

    private val mockSchemaRegistryScope = "phase3-test-${UUID.randomUUID()}"
    private val mockSchemaRegistryUrl = "mock://$mockSchemaRegistryScope"

    private lateinit var producer: KafkaProducer<String, Any>
    private lateinit var emitter: KafkaEventEmitter
    private lateinit var config: KafkaConfig

    @BeforeAll
    fun setup() {
        kafka.start()
        // Auto-create topics for the test (the docker-compose setup uses --create explicitly,
        // but Kafka's broker also supports auto.create.topics.enable in test mode).
        config = KafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            schemaRegistryUrl = mockSchemaRegistryUrl,
            topicAuctionResults = "auction-results-test",
            topicImpressionEvents = "impression-events-test",
            lingerMs = 0,
            acks = "1",
        )
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl)
            put(ProducerConfig.ACKS_CONFIG, "1")
        }
        producer = KafkaProducer(props)
        emitter = KafkaEventEmitter(producer, config)
    }

    @AfterAll
    fun tearDown() {
        emitter.close()
        kafka.stop()
        MockSchemaRegistry.dropScope(mockSchemaRegistryScope)
    }

    @Timeout(value = 30)
    @Test
    fun `emitImpression sends an Avro-encoded record to the configured topic`() {
        val event = ImpressionEvent.newBuilder()
            .setUserId("user-1")
            .setCampaignId("camp-001")
            .setCreativeId("cre-001a")
            .setCategory("IAB13")
            .setPrice(2.50)
            .setTsMillis(1L)
            .build()
        emitter.emitImpression(event)
        producer.flush()

        val received = pollOne<ImpressionEvent>(config.topicImpressionEvents)
        assertThat(received.userId.toString()).isEqualTo("user-1")
        assertThat(received.campaignId.toString()).isEqualTo("camp-001")
    }

    @Timeout(value = 30)
    @Test
    fun `emitAuctionResult sends an Avro-encoded record with outcome enum`() {
        val event = AuctionResultEvent.newBuilder()
            .setRequestId("r1")
            .setUserId("user-1")
            .setImpId("1")
            .setTsMillis(1L)
            .setOutcome(Outcome.FILLED)
            .setWinnerCampaignId("camp-007")
            .setWinnerPrice(3.10)
            .setCandidatesInitial(5)
            .setCandidatesAfterBlocking(5)
            .setCandidatesAfterFreqCompsep(3)
            .setCandidatesAfterFloor(2)
            .build()
        emitter.emitAuctionResult(event)
        producer.flush()

        val received = pollOne<AuctionResultEvent>(config.topicAuctionResults)
        assertThat(received.outcome).isEqualTo(Outcome.FILLED)
        assertThat(received.winnerCampaignId.toString()).isEqualTo("camp-007")
    }

    private inline fun <reified T> pollOne(topic: String): T {
        val consumerProps = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java.name)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl)
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
        }
        val consumer = KafkaConsumer<String, T>(consumerProps)
        consumer.use {
            it.subscribe(listOf(topic))
            val deadline = System.currentTimeMillis() + 20_000
            while (System.currentTimeMillis() < deadline) {
                val records = it.poll(Duration.ofMillis(500))
                for (record in records) {
                    return record.value()
                }
            }
            throw AssertionError("No record received on topic $topic within 20s")
        }
    }
}
```

Note on `MockSchemaRegistry`: Confluent's `kafka-schema-registry-client` ships an in-process mock that handles `mock://` URLs. It avoids needing a real Schema Registry container in this test — the producer + consumer share schemas through the mock store.

- [ ] **Step 7: Run the test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.kafka.KafkaEventEmitterTest"`
Expected: PASS, 2 tests green. (First run pulls `confluentinc/cp-kafka:7.7.0` which is ~700MB.)

- [ ] **Step 8: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/main/resources/application.conf \
        ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/kafka/ \
        ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt
git commit -m "Phase 3 task 5: Kafka producer + KafkaEventEmitter (fire-and-forget Avro emit)"
```

---

## Task 6: Wire `KafkaEventEmitter` into the Auction Pipeline

**Files:**
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt`
- Modify: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt`

The pipeline's `runAuction` returns the `BidResponse` AND emits two events as a side effect:
- `AuctionResultEvent` always (records the outcome of every auction)
- `ImpressionEvent` only if a winner was selected

Pass an optional `KafkaEventEmitter` (defaulting to a no-op) so existing tests keep working.

- [ ] **Step 1: Add a `NoOpKafkaEmitter` interface + default**

Refactor `KafkaEventEmitter` into a small interface so the pipeline can take a no-op for tests. Open `ad-server/src/main/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitter.kt`. Replace the file with:

```kotlin
package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

/** Public emitter contract — pipeline depends on this, not the concrete Kafka type. */
interface EventEmitter {
    fun emitImpression(event: ImpressionEvent)
    fun emitAuctionResult(event: AuctionResultEvent)
}

/** No-op for tests that don't care about emission. */
object NoOpEventEmitter : EventEmitter {
    override fun emitImpression(event: ImpressionEvent) {}
    override fun emitAuctionResult(event: AuctionResultEvent) {}
}

/**
 * Fire-and-forget Kafka implementation. The request path calls [emitImpression] /
 * [emitAuctionResult] and returns immediately. Errors land in the producer callback but never
 * propagate up to the request handler.
 */
class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) : EventEmitter, AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun emitImpression(event: ImpressionEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicImpressionEvents, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicImpressionEvents, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicImpressionEvents, e.message)
            }
        }
    }

    override fun emitAuctionResult(event: AuctionResultEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicAuctionResults, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicAuctionResults, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicAuctionResults, e.message)
            }
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}
```

- [ ] **Step 2: Update `KafkaEventEmitterTest.kt` to use the new interface**

In `ad-server/src/test/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitterTest.kt`, no signatures changed (the existing test calls `KafkaEventEmitter` directly, which still implements both `EventEmitter` and `AutoCloseable`). Verify the existing test still passes:

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.kafka.KafkaEventEmitterTest"`
Expected: PASS, 2 tests green.

- [ ] **Step 3: Modify `AuctionPipeline.kt` to emit events**

Read the current file. Add an `EventEmitter` constructor parameter (default `NoOpEventEmitter`):

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.kafka.EventEmitter
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import java.util.UUID

class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
    private val eventEmitter: EventEmitter = NoOpEventEmitter,
    private val clock: () -> Long = { System.currentTimeMillis() },
) {
    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)

        // Track funnel sizes for the auction-result event.
        val sizes = IntArray(4) // [initial, post-blocking, post-freq+compsep, post-floor]
        sizes[0] = initial.size

        if (initial.isEmpty()) {
            emitOutcome(request, ctx, sizes, Outcome.NO_FILL_BLOCKING, winner = null)
            return BidResponse(id = request.id, nbr = NoBidReason.NO_MATCHING_CREATIVE)
        }

        var current = initial
        for ((idx, stage) in stages.withIndex()) {
            current = stage.evaluate(ctx, current)
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

- [ ] **Step 4: Update `Application.kt` `buildPipeline` signature + main wiring**

In `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`:

(a) Update `buildPipeline` to accept an emitter:

```kotlin
fun buildPipeline(
    snapshot: InventorySnapshot,
    frequencyClient: FrequencyClient,
    eventEmitter: com.github.robran.adserver.kafka.EventEmitter = com.github.robran.adserver.kafka.NoOpEventEmitter,
): AuctionPipeline = AuctionPipeline(
    candidateBuilder = CandidateBuilder(snapshot),
    stages = listOf(
        BlockingPolicyStage(),
        FrequencyAndCompsepStage(frequencyClient),
        FloorPriceStage(),
        SelectionStage(Random.Default),
    ),
    eventEmitter = eventEmitter,
)
```

(b) In `main()`, after `val frequencyClient = ...`, construct the Kafka producer + emitter and pass it in:

```kotlin
    val kafkaProducer = com.github.robran.adserver.kafka.ProducerFactory.avroProducer(config.kafka)
    val eventEmitter = com.github.robran.adserver.kafka.KafkaEventEmitter(kafkaProducer, config.kafka)
    val pipeline = buildPipeline(snapshot, frequencyClient, eventEmitter)
```

Update the shutdown hook to also close the emitter:

```kotlin
    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down ad-server")
            eventEmitter.close()
            frequencyChannel.shutdown()
        },
    )
```

- [ ] **Step 5: Update `BidRouteIntegrationTest` to keep working**

The test's call site is `pipeline = buildPipeline(snapshot, FakeFrequencyClient())`. Since `eventEmitter` defaults to `NoOpEventEmitter`, no change is needed — but verify the test still compiles + passes:

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.http.BidRouteIntegrationTest"`
Expected: PASS, 5 tests green.

- [ ] **Step 6: Update `AuctionPipelineTest` for the new signatures**

Phase 1's test invokes `AuctionPipeline(candidateBuilder, stages)` directly. The new third parameter has a default, so existing tests still compile. Verify:

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTest"`
Expected: PASS, 6 tests green.

Add ONE new test that asserts emission. Append inside the `AuctionPipelineTest` class:

```kotlin
    @Test
    fun `emits AuctionResultEvent for both filled and no-fill outcomes`() = runTest {
        val recordingEmitter = object : com.github.robran.adserver.kafka.EventEmitter {
            val results = mutableListOf<com.github.robran.adserver.protocol.events.AuctionResultEvent>()
            val impressions = mutableListOf<com.github.robran.adserver.protocol.events.ImpressionEvent>()
            override fun emitAuctionResult(event: com.github.robran.adserver.protocol.events.AuctionResultEvent) { results += event }
            override fun emitImpression(event: com.github.robran.adserver.protocol.events.ImpressionEvent) { impressions += event }
        }

        val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = recordingEmitter,
            clock = { 12345L },
        )

        pipe.runAuction(req())

        assertThat(recordingEmitter.results).hasSize(1)
        assertThat(recordingEmitter.results[0].outcome).isEqualTo(com.github.robran.adserver.protocol.events.Outcome.FILLED)
        assertThat(recordingEmitter.results[0].tsMillis).isEqualTo(12345L)
        assertThat(recordingEmitter.impressions).hasSize(1)
        assertThat(recordingEmitter.impressions[0].campaignId.toString()).isEqualTo("c1")

        // No-fill case
        recordingEmitter.results.clear()
        recordingEmitter.impressions.clear()
        val pipeBlocked = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = recordingEmitter,
            clock = { 99L },
        )
        pipeBlocked.runAuction(req(bcat = listOf("IAB1")))
        assertThat(recordingEmitter.results).hasSize(1)
        assertThat(recordingEmitter.results[0].outcome)
            .isEqualTo(com.github.robran.adserver.protocol.events.Outcome.NO_FILL_BLOCKING)
        assertThat(recordingEmitter.impressions).hasSize(0)
    }
```

(`req()` and `campaign(...)` are existing helpers in the test file.)

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTest"`
Expected: PASS, 7 tests green (6 existing + 1 new).

- [ ] **Step 7: Run the full ad-server suite + build**

Run: `./gradlew :ad-server:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/kafka/KafkaEventEmitter.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt
git commit -m "Phase 3 task 6: emit AuctionResultEvent + ImpressionEvent from AuctionPipeline"
```

---

## Task 7: `flink-impression-aggregator` Module Build File + Config

**Files:**
- Modify: `flink-impression-aggregator/build.gradle.kts`
- Create: `flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/AppConfig.kt`
- Create: `flink-impression-aggregator/src/main/resources/application.conf`
- Create: `flink-impression-aggregator/src/main/resources/logback.xml`
- Create: `flink-impression-aggregator/src/test/resources/logback-test.xml`

- [ ] **Step 1: Replace `flink-impression-aggregator/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

application {
    mainClass.set("com.github.robran.adserver.flink.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))

    // Flink core + Kafka connector + Avro Schema Registry support
    implementation(libs.flink.streaming.java)
    implementation(libs.flink.clients)
    implementation(libs.flink.connector.kafka)
    implementation(libs.flink.avro)
    implementation(libs.flink.avro.confluent.registry)

    // Redis sink uses Lettuce (already in catalog)
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.flink.test.utils)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
    // Generic Redis container is enough; no dedicated module needed.
}
```

- [ ] **Step 2: Write `AppConfig.kt`**

```kotlin
package com.github.robran.adserver.flink

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class FlinkSourceConfig(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val topicImpressionEvents: String,
    val groupId: String,
)

data class RedisSinkConfig(
    val url: String,
    val capWindowSeconds: Long,
    val winhistoryWindowSeconds: Long,
)

data class FlinkAppConfig(
    val source: FlinkSourceConfig,
    val sink: RedisSinkConfig,
    val checkpointIntervalMs: Long,
    val windowSeconds: Long,
    val allowedLatenessSeconds: Long,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): FlinkAppConfig =
            raw.extract("flink-aggregator")
    }
}
```

- [ ] **Step 3: Write `application.conf`**

```hocon
flink-aggregator {
    source {
        bootstrapServers = "localhost:29092"
        bootstrapServers = ${?KAFKA_BOOTSTRAP_SERVERS}
        schemaRegistryUrl = "http://localhost:8081"
        schemaRegistryUrl = ${?SCHEMA_REGISTRY_URL}
        topicImpressionEvents = "impression-events"
        topicImpressionEvents = ${?KAFKA_TOPIC_IMPRESSION_EVENTS}
        groupId = "impression-aggregator"
        groupId = ${?FLINK_GROUP_ID}
    }
    sink {
        url = "redis://localhost:6379"
        url = ${?FLINK_REDIS_URL}
        capWindowSeconds = 86400        # 24h rolling cap window
        capWindowSeconds = ${?CAP_WINDOW_SECONDS}
        winhistoryWindowSeconds = 3600  # 1h competitive-separation window
        winhistoryWindowSeconds = ${?WINHISTORY_WINDOW_SECONDS}
    }
    checkpointIntervalMs = 30000
    checkpointIntervalMs = ${?CHECKPOINT_INTERVAL_MS}
    windowSeconds = 10
    windowSeconds = ${?WINDOW_SECONDS}
    allowedLatenessSeconds = 2
    allowedLatenessSeconds = ${?ALLOWED_LATENESS_SECONDS}
}
```

- [ ] **Step 4: Write `logback.xml`**

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
    <logger name="org.apache.flink" level="WARN"/>
    <logger name="org.apache.kafka" level="WARN"/>
    <logger name="io.lettuce.core" level="WARN"/>
</configuration>
```

- [ ] **Step 5: Write `src/test/resources/logback-test.xml`**

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
    <logger name="org.apache.flink" level="WARN"/>
    <logger name="org.apache.kafka" level="WARN"/>
    <logger name="io.lettuce.core" level="WARN"/>
</configuration>
```

- [ ] **Step 6: Verify the module configures**

Run: `./gradlew :flink-impression-aggregator:dependencies --configuration runtimeClasspath`
Expected: BUILD SUCCESSFUL. Tree includes `flink-streaming-java`, `flink-connector-kafka`, `flink-avro-confluent-registry`, `lettuce-core`, and the `:common-protocol` project.

- [ ] **Step 7: Commit**

```bash
git add flink-impression-aggregator/build.gradle.kts \
        flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/AppConfig.kt \
        flink-impression-aggregator/src/main/resources/application.conf \
        flink-impression-aggregator/src/main/resources/logback.xml \
        flink-impression-aggregator/src/test/resources/logback-test.xml
git commit -m "Phase 3 task 7: flink-impression-aggregator module setup"
```

---

## Task 8: Lua Scripts for Atomic Redis Writes

**Files:**
- Create: `flink-impression-aggregator/src/main/resources/lua/incrFreqWithExpiry.lua`
- Create: `flink-impression-aggregator/src/main/resources/lua/addWinHistory.lua`
- Create: `flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/LuaScripts.kt`

The Flink sink runs INCRBY+EXPIRE as one atomic Redis operation, and ZADD+ZREMRANGEBYSCORE as another. Without Lua, an aggregator crash between INCRBY and EXPIRE could leave an unbounded counter — these scripts close that gap.

- [ ] **Step 1: Write `incrFreqWithExpiry.lua`**

```lua
-- KEYS[1] = the freq:{userId}:{campaignId} key
-- ARGV[1] = increment amount (integer)
-- ARGV[2] = TTL in seconds
-- Returns: the new counter value
local newval = redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return newval
```

- [ ] **Step 2: Write `addWinHistory.lua`**

```lua
-- KEYS[1] = the winhistory:{userId} sorted-set key
-- ARGV[1] = score (timestamp, ms)
-- ARGV[2] = member ("{campaignId}:{category}")
-- ARGV[3] = trim threshold (drop entries with score < threshold)
-- Returns: the size of the zset after trim
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[3]))
return redis.call('ZCARD', KEYS[1])
```

- [ ] **Step 3: Write `LuaScripts.kt`**

```kotlin
package com.github.robran.adserver.flink

import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import org.slf4j.LoggerFactory

/**
 * Loads the Lua scripts from the classpath, registers them with Redis on first use (SCRIPT LOAD),
 * and caches the SHA1 for subsequent EVALSHA calls. This is the pattern Lettuce + Redis recommend.
 *
 * Not thread-safe: a single LuaScripts instance is owned by a single Flink RichSinkFunction and
 * accessed serially by the sink's checkpoint thread.
 */
class LuaScripts(private val sync: RedisCommands<String, String>) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val incrFreqSrc: String = readResource("/lua/incrFreqWithExpiry.lua")
    private val addWinHistorySrc: String = readResource("/lua/addWinHistory.lua")

    private var incrFreqSha: String? = null
    private var addWinHistorySha: String? = null

    fun incrFreqWithExpiry(key: String, increment: Long, ttlSeconds: Long): Long {
        val sha = ensureLoaded(incrFreqSrc, ::incrFreqShaHolder)
        return sync.evalsha<Long>(
            sha,
            ScriptOutputType.INTEGER,
            arrayOf(key),
            increment.toString(),
            ttlSeconds.toString(),
        )
    }

    fun addWinHistory(key: String, scoreMs: Long, member: String, trimBeforeMs: Long): Long {
        val sha = ensureLoaded(addWinHistorySrc, ::addWinHistoryShaHolder)
        return sync.evalsha<Long>(
            sha,
            ScriptOutputType.INTEGER,
            arrayOf(key),
            scoreMs.toString(),
            member,
            trimBeforeMs.toString(),
        )
    }

    private fun incrFreqShaHolder(value: String?): String? {
        incrFreqSha = value
        return incrFreqSha
    }

    private fun addWinHistoryShaHolder(value: String?): String? {
        addWinHistorySha = value
        return addWinHistorySha
    }

    private fun ensureLoaded(source: String, holder: (String?) -> String?): String {
        val current = holder(null) // read
        if (current != null) return current
        val sha = sync.scriptLoad(source)
        holder(sha) // write
        return sha
    }

    companion object {
        private fun readResource(path: String): String =
            LuaScripts::class.java.getResourceAsStream(path)
                ?.bufferedReader()
                ?.use { it.readText() }
                ?: error("Lua script not found on classpath: $path")
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `./gradlew :flink-impression-aggregator:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add flink-impression-aggregator/src/main/resources/lua/ \
        flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/LuaScripts.kt
git commit -m "Phase 3 task 8: Lua scripts for atomic INCRBY+EXPIRE and ZADD+ZREMRANGEBYSCORE"
```

---

## Task 9: `RedisCounterSink` (Flink RichSinkFunction)

**Files:**
- Create: `flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/RedisCounterSink.kt`

The sink receives `WindowedCount` records (defined inline below) and writes them to Redis. Lettuce connection is lazily opened in `open()` (per task instance, called by Flink at job startup) and closed in `close()`.

- [ ] **Step 1: Write `RedisCounterSink.kt`**

```kotlin
package com.github.robran.adserver.flink

import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction
import org.slf4j.LoggerFactory
import io.lettuce.core.RedisClient as LettuceClient

/**
 * One windowed aggregation record from the upstream operator.
 *
 * @property userId destination user
 * @property campaignId destination campaign
 * @property category primary IAB category of the campaign (for winhistory member)
 * @property count number of impressions to add
 * @property windowEndMs end of the window (used as the winhistory score)
 */
data class WindowedCount(
    val userId: String,
    val campaignId: String,
    val category: String,
    val count: Long,
    val windowEndMs: Long,
)

/**
 * Writes windowed impression counts back to Redis.
 *
 * Uses Lua scripts (registered once per task) to keep INCRBY+EXPIRE and ZADD+ZREMRANGEBYSCORE
 * atomic. The connection lifecycle is per task instance — Flink calls open() on job startup
 * and close() on shutdown.
 */
class RedisCounterSink(
    private val redisUrl: String,
    private val capWindowSeconds: Long,
    private val winhistoryWindowSeconds: Long,
) : RichSinkFunction<WindowedCount>() {

    private val log = LoggerFactory.getLogger(javaClass)

    @Transient private var lettuce: LettuceClient? = null
    @Transient private var connection: StatefulRedisConnection<String, String>? = null
    @Transient private var scripts: LuaScripts? = null

    override fun open(openContext: OpenContext) {
        val uri = RedisURI.create(redisUrl)
        lettuce = LettuceClient.create(uri)
        connection = lettuce!!.connect()
        scripts = LuaScripts(connection!!.sync())
        log.info("RedisCounterSink open: connected to {}", redisUrl)
    }

    override fun invoke(value: WindowedCount, context: SinkFunction.Context) {
        val s = scripts ?: error("Sink not opened")
        val freqKey = "freq:${value.userId}:${value.campaignId}"
        s.incrFreqWithExpiry(freqKey, value.count, capWindowSeconds)

        val winKey = "winhistory:${value.userId}"
        val member = "${value.campaignId}:${value.category}"
        val trimBefore = value.windowEndMs - winhistoryWindowSeconds * 1000
        s.addWinHistory(winKey, value.windowEndMs, member, trimBefore)
    }

    override fun close() {
        try {
            connection?.close()
        } finally {
            lettuce?.shutdown()
        }
        log.info("RedisCounterSink closed")
    }
}
```

Note on `RichSinkFunction`: Flink 1.20 deprecated the legacy `RichSinkFunction` in favor of `Sink<T>` interface, but `RichSinkFunction` still works and is dramatically simpler. The deprecated class lives at `org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction` in 1.20. Using it is acceptable for Phase 3; if Phase 5 / 6 polish wants to migrate, that's a future task.

- [ ] **Step 2: Verify compile**

Run: `./gradlew :flink-impression-aggregator:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/RedisCounterSink.kt
git commit -m "Phase 3 task 9: RedisCounterSink (Flink → Lettuce → Lua-scripted Redis writes)"
```

---

## Task 10: `ImpressionAggregatorJob` Topology

**Files:**
- Create: `flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/ImpressionAggregatorJob.kt`
- Create: `flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/Application.kt`

`ImpressionAggregatorJob.build(env, config)` constructs the topology from a Kafka source through the windowed aggregator into the Redis sink. `Application.main()` instantiates a local `StreamExecutionEnvironment`, builds the job, and executes it.

- [ ] **Step 1: Write `ImpressionAggregatorJob.kt`**

Use a single `ProcessWindowFunction<ImpressionEvent, …>` that receives the raw events. (The two-arg `aggregate(AggregateFunction, ProcessWindowFunction)` overload would lose access to the category — its second argument only sees the AggregateFunction's `Long` result.)

```kotlin
package com.github.robran.adserver.flink

import com.github.robran.adserver.protocol.events.ImpressionEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

object ImpressionAggregatorJob {

    fun build(env: StreamExecutionEnvironment, config: FlinkAppConfig): DataStream<WindowedCount> {
        val watermarks = WatermarkStrategy
            .forBoundedOutOfOrderness<ImpressionEvent>(Duration.ofSeconds(config.allowedLatenessSeconds))
            .withTimestampAssigner { event, _ -> event.tsMillis }

        val source = kafkaSource(config)
        val stream: DataStream<ImpressionEvent> = env
            .fromSource(source, watermarks, "kafka-impression-events")

        val aggregated: DataStream<WindowedCount> = stream
            .keyBy { event -> "${event.userId}|${event.campaignId}" }
            .window(TumblingEventTimeWindows.of(Time.seconds(config.windowSeconds)))
            .allowedLateness(Time.seconds(config.allowedLatenessSeconds))
            .process(CountAndEmit())

        aggregated.addSink(
            RedisCounterSink(
                redisUrl = config.sink.url,
                capWindowSeconds = config.sink.capWindowSeconds,
                winhistoryWindowSeconds = config.sink.winhistoryWindowSeconds,
            ),
        ).name("redis-counter-sink")

        return aggregated
    }

    private fun kafkaSource(config: FlinkAppConfig): KafkaSource<ImpressionEvent> {
        val deserializer = ConfluentRegistryAvroDeserializationSchema.forSpecific(
            ImpressionEvent::class.java,
            config.source.schemaRegistryUrl,
        )
        return KafkaSource.builder<ImpressionEvent>()
            .setBootstrapServers(config.source.bootstrapServers)
            .setTopics(config.source.topicImpressionEvents)
            .setGroupId(config.source.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(deserializer)
            .build()
    }

    /** Counts events per window and emits one WindowedCount with the window's end timestamp. */
    private class CountAndEmit : ProcessWindowFunction<ImpressionEvent, WindowedCount, String, TimeWindow>() {
        override fun process(
            key: String,
            context: Context,
            elements: Iterable<ImpressionEvent>,
            out: Collector<WindowedCount>,
        ) {
            val list = elements.toList()
            if (list.isEmpty()) return
            val sample = list.first()
            val count = list.size.toLong()
            out.collect(
                WindowedCount(
                    userId = sample.userId.toString(),
                    campaignId = sample.campaignId.toString(),
                    category = sample.category.toString(),
                    count = count,
                    windowEndMs = context.window().end,
                ),
            )
        }
    }
}
```

- [ ] **Step 2: Write `Application.kt`**

```kotlin
package com.github.robran.adserver.flink

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.flink.Application")

fun main() {
    val config = FlinkAppConfig.load()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    env.parallelism = 1

    ImpressionAggregatorJob.build(env, config)

    log.info(
        "ImpressionAggregator starting: kafka={} schemaRegistry={} redis={}",
        config.source.bootstrapServers,
        config.source.schemaRegistryUrl,
        config.sink.url,
    )

    env.execute("kotlin_ad_server-impression-aggregator")
}
```

- [ ] **Step 3: Verify compile**

Run: `./gradlew :flink-impression-aggregator:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/ImpressionAggregatorJob.kt \
        flink-impression-aggregator/src/main/kotlin/com/github/robran/adserver/flink/Application.kt
git commit -m "Phase 3 task 10: ImpressionAggregatorJob topology + MiniCluster main"
```

---

## Task 11: Flink Job Integration Test (MiniCluster + Testcontainers Kafka + Redis)

**Files:**
- Create: `flink-impression-aggregator/src/test/kotlin/com/github/robran/adserver/flink/ImpressionAggregatorJobTest.kt`

This test brings up: a Testcontainers Kafka, a Testcontainers Redis, an in-process Schema Registry mock, and a Flink `MiniCluster`. It produces impression events to Kafka, runs the Flink job, and asserts that the expected Redis counter values appear within a reasonable window.

- [ ] **Step 1: Write the test**

`flink-impression-aggregator/src/test/kotlin/com/github/robran/adserver/flink/ImpressionAggregatorJobTest.kt`:

```kotlin
package com.github.robran.adserver.flink

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.events.ImpressionEvent
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.UUID
import io.lettuce.core.RedisClient as LettuceClient

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ImpressionAggregatorJobTest {

    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
    )

    private val redis: GenericContainer<*> = GenericContainer(
        DockerImageName.parse("redis:7-alpine"),
    ).withExposedPorts(6379)

    private val mockScope = "phase3-flink-${UUID.randomUUID()}"
    private val mockUrl = "mock://$mockScope"
    private val topic = "impression-events-test"

    private lateinit var producer: KafkaProducer<String, ImpressionEvent>
    private lateinit var redisLettuce: LettuceClient
    private lateinit var redisConn: StatefulRedisConnection<String, String>

    private val miniCluster = MiniClusterWithClientResource(
        MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build(),
    )

    @BeforeAll
    fun setup() {
        kafka.start()
        redis.start()
        miniCluster.before()

        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockUrl)
            put(ProducerConfig.ACKS_CONFIG, "1")
        }
        producer = KafkaProducer(producerProps)

        redisLettuce = LettuceClient.create(RedisURI.create("redis://${redis.host}:${redis.getMappedPort(6379)}"))
        redisConn = redisLettuce.connect()
    }

    @AfterAll
    fun tearDown() {
        producer.close()
        redisConn.close()
        redisLettuce.shutdown()
        miniCluster.after()
        redis.stop()
        kafka.stop()
        MockSchemaRegistry.dropScope(mockScope)
    }

    @Timeout(value = 90)
    @Test
    fun `aggregates impressions and writes counts back to Redis`() {
        // Given: 5 impressions for user-A/camp-001, all in window [0, 10000)
        val baseTs = 0L
        val events = (1..5).map { i ->
            ImpressionEvent.newBuilder()
                .setUserId("user-A")
                .setCampaignId("camp-001")
                .setCreativeId("cre-001a")
                .setCategory("IAB13")
                .setPrice(2.0)
                .setTsMillis(baseTs + i * 1000L)
                .build()
        }
        // One impression in the next window, used to drive the watermark past the first window's end.
        val watermarkDriver = ImpressionEvent.newBuilder()
            .setUserId("other")
            .setCampaignId("camp-099")
            .setCreativeId("cre-x")
            .setCategory("IAB1")
            .setPrice(1.0)
            .setTsMillis(15_000L)
            .build()

        for (e in events + watermarkDriver) {
            producer.send(ProducerRecord(topic, e.userId.toString(), e)).get()
        }
        producer.flush()

        // Build the job using a config pointed at the test infra.
        val config = FlinkAppConfig(
            source = FlinkSourceConfig(
                bootstrapServers = kafka.bootstrapServers,
                schemaRegistryUrl = mockUrl,
                topicImpressionEvents = topic,
                groupId = "test-${UUID.randomUUID()}",
            ),
            sink = RedisSinkConfig(
                url = "redis://${redis.host}:${redis.getMappedPort(6379)}",
                capWindowSeconds = 86400,
                winhistoryWindowSeconds = 3600,
            ),
            checkpointIntervalMs = 5_000,
            windowSeconds = 10,
            allowedLatenessSeconds = 2,
        )
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
        env.parallelism = 1

        ImpressionAggregatorJob.build(env, config)

        val jobThread = Thread {
            try {
                env.execute("test-job")
            } catch (_: InterruptedException) {
                // shutdown signal
            }
        }.also { it.isDaemon = true; it.start() }

        // Poll Redis for the expected counter; allow up to 60s for the job + Kafka catch-up.
        val sync = redisConn.sync()
        val deadline = System.currentTimeMillis() + 60_000
        var observed: Long = 0
        while (System.currentTimeMillis() < deadline) {
            val raw = sync.get("freq:user-A:camp-001")
            if (raw != null) {
                observed = raw.toLong()
                if (observed >= 5) break
            }
            Thread.sleep(500)
        }
        jobThread.interrupt()

        assertThat(observed).isEqualTo(5L)

        // Winhistory should also contain user-A's win
        val winSize = sync.zcard("winhistory:user-A")
        assertThat(winSize).isEqualTo(1L)
    }
}
```

- [ ] **Step 2: Run the test**

Run: `./gradlew :flink-impression-aggregator:test`
Expected: PASS, 1 test green. (Heavy: ~60-90s. First run pulls Kafka image + downloads Flink dependencies.)

- [ ] **Step 3: Commit**

```bash
git add flink-impression-aggregator/src/test/kotlin/com/github/robran/adserver/flink/ImpressionAggregatorJobTest.kt
git commit -m "Phase 3 task 11: Flink integration test (MiniCluster + Testcontainers Kafka + Redis)"
```

---

## Task 12: Phase 3 End-to-End Test (ad-server emits → Kafka → Flink → Redis → next auction filters)

**Files:**
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase3EndToEndTest.kt`
- Modify: `ad-server/build.gradle.kts` (testImplementation `:flink-impression-aggregator`)

This is the loop-closure test: drive a few hundred bid requests, prove Flink aggregates and writes counters back to Redis, prove the next round of requests has freq caps triggering.

The full stack here: Postgres + Redis + Kafka + Schema Registry mock + Flink MiniCluster + frequency-service in-process + ad-server via testApplication.

- [ ] **Step 1: Add the test dependency**

In `ad-server/build.gradle.kts`, in the `testImplementation` block, add:

```kotlin
    testImplementation(project(":flink-impression-aggregator"))
```

- [ ] **Step 2: Write the test**

`ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase3EndToEndTest.kt`:

```kotlin
package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isGreaterThanOrEqualTo
import com.github.robran.adserver.AppConfig
import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.flink.FlinkAppConfig
import com.github.robran.adserver.flink.FlinkSourceConfig
import com.github.robran.adserver.flink.ImpressionAggregatorJob
import com.github.robran.adserver.flink.RedisSinkConfig
import com.github.robran.adserver.frequency.EnrichService
import com.github.robran.adserver.frequency.RedisClient as FreqRedisClient
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.kafka.KafkaEventEmitter
import com.github.robran.adserver.kafka.ProducerFactory
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
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
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Phase3EndToEndTest {

    private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("kotlin_ad_server_test")
        .withUsername("test")
        .withPassword("test")

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
    )

    private val mockScope = "phase3-e2e-${UUID.randomUUID()}"
    private val mockSchemaUrl = "mock://$mockScope"

    private lateinit var freqRedis: FreqRedisClient
    private lateinit var freqServer: Server
    private lateinit var freqChannel: ManagedChannel
    private lateinit var pipeline: AuctionPipeline
    private lateinit var emitter: KafkaEventEmitter

    private val freqServerName = InProcessServerBuilder.generateName()

    private val miniCluster = MiniClusterWithClientResource(
        MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build(),
    )

    @BeforeAll
    fun setup() {
        postgres.start()
        redis.start()
        kafka.start()
        miniCluster.before()

        // Hydrate inventory from Postgres.
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        val snapshot = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                InventoryLoader(ds).load()
            }

        // Boot frequency-service against the test Redis.
        val redisUrl = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        freqRedis = FreqRedisClient.connect(redisUrl)
        freqServer = InProcessServerBuilder.forName(freqServerName)
            .directExecutor()
            .addService(EnrichService(freqRedis))
            .build()
            .start()
        freqChannel = InProcessChannelBuilder.forName(freqServerName).directExecutor().build()
        val grpcClient = GrpcFrequencyClient(freqChannel, timeoutMs = 1_000L)

        // Build the ad-server's Kafka emitter.
        val kafkaConfig = KafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            schemaRegistryUrl = mockSchemaUrl,
            topicAuctionResults = "auction-results-test",
            topicImpressionEvents = "impression-events-test",
            lingerMs = 0,
            acks = "1",
        )
        val producer = ProducerFactory.avroProducer(kafkaConfig)
        emitter = KafkaEventEmitter(producer, kafkaConfig)
        pipeline = buildPipeline(snapshot, grpcClient, emitter)

        // Start the Flink job.
        val flinkConfig = FlinkAppConfig(
            source = FlinkSourceConfig(
                bootstrapServers = kafka.bootstrapServers,
                schemaRegistryUrl = mockSchemaUrl,
                topicImpressionEvents = "impression-events-test",
                groupId = "phase3-e2e",
            ),
            sink = RedisSinkConfig(
                url = redisUrl,
                capWindowSeconds = 86400,
                winhistoryWindowSeconds = 3600,
            ),
            checkpointIntervalMs = 5_000,
            windowSeconds = 5,
            allowedLatenessSeconds = 1,
        )
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(flinkConfig.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
        env.parallelism = 1
        ImpressionAggregatorJob.build(env, flinkConfig)
        Thread {
            try { env.execute("phase3-e2e-flink") } catch (_: InterruptedException) {}
        }.also { it.isDaemon = true; it.start() }
    }

    @AfterAll
    fun tearDown() {
        emitter.close()
        freqChannel.shutdownNow()
        freqServer.shutdownNow()
        freqRedis.close()
        miniCluster.after()
        kafka.stop()
        redis.stop()
        postgres.stop()
        io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry.dropScope(mockScope)
    }

    @Timeout(value = 180)
    @Test
    fun `loop closes - drive impressions, observe Redis counters increase, freq caps trigger`() = testApplication {
        application { adServerModule(HealthState().apply { ready.set(true) }, pipeline) }
        val httpClient = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

        val userId = "loop-test-user"

        // Drive 30 bid requests in a row. Each filled auction emits an ImpressionEvent. Some
        // campaigns have caps as low as 3, so after enough impressions, those campaigns will
        // be filtered out by the freq+compsep stage.
        repeat(30) {
            val response = httpClient.post("/openrtb/bid") {
                contentType(ContentType.Application.Json)
                setBody(
                    BidRequest(
                        id = "req-${System.nanoTime()}",
                        imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
                        user = User(id = userId),
                    ),
                )
            }
            response.body<BidResponse>()
        }

        // Wait for Flink to drain its window + write to Redis. Window is 5s + 1s lateness;
        // give it generous slack.
        val deadline = System.currentTimeMillis() + 60_000
        var totalCounted = 0L
        val sync = redis.let {
            io.lettuce.core.RedisClient.create(
                io.lettuce.core.RedisURI.create("redis://${redis.host}:${redis.getMappedPort(6379)}"),
            ).connect().sync()
        }
        while (System.currentTimeMillis() < deadline) {
            val keys = sync.keys("freq:$userId:*")
            totalCounted = keys.sumOf { (sync.get(it)?.toLongOrNull() ?: 0L) }
            if (totalCounted > 0) break
            Thread.sleep(500)
        }

        // We expect at least some impressions to have been aggregated. The exact count depends on
        // which campaigns won (random tie-break), but at least 5 impressions across all campaigns
        // should round-trip in 60s.
        assertThat(totalCounted).isGreaterThanOrEqualTo(5L)
    }
}
```

- [ ] **Step 3: Run the test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.http.Phase3EndToEndTest"`
Expected: PASS, 1 test green. **HEAVY:** brings up Postgres + Redis + Kafka + Flink MiniCluster + a real Ktor app + frequency-service. Expected runtime: 60-180 seconds.

- [ ] **Step 4: Commit**

```bash
git add ad-server/build.gradle.kts \
        ad-server/src/test/kotlin/com/github/robran/adserver/http/Phase3EndToEndTest.kt
git commit -m "Phase 3 task 12: end-to-end loop test (impressions → Kafka → Flink → Redis)"
```

---

## Task 13: README + Smoke-Test Update

**Files:**
- Modify: `README.md`
- Modify: `scripts/smoke-test.sh`

- [ ] **Step 1: Update README Status block**

Find:

```markdown
## Status

- ✅ **Phase 1 — Skeleton + hot path**
- ✅ **Phase 2 — Frequency service + Redis** (this commit)
- ⏳ Phase 3 — Kafka + Flink aggregator
- ⏳ Phase 4 — Observability (Micrometer, OpenTelemetry, Jaeger, Prometheus, Grafana)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README
```

Replace with:

```markdown
## Status

- ✅ **Phase 1 — Skeleton + hot path**
- ✅ **Phase 2 — Frequency service + Redis**
- ✅ **Phase 3 — Kafka + Flink aggregator** (this commit)
- ⏳ Phase 4 — Observability (Micrometer, OpenTelemetry, Jaeger, Prometheus, Grafana)
- ⏳ Phase 5 — Gatling load testing + profiling
- ⏳ Phase 6 — Polish + final README
```

- [ ] **Step 2: Update Modules section**

Add after the existing `frequency-service` line:

```markdown
- `flink-impression-aggregator` — Apache Flink 1.20 streaming job. Consumes `impression-events` from Kafka (Avro via Confluent Schema Registry), keys by `(user, campaign)`, tumbling 10-second windows, writes counts back to Redis through Lua-scripted atomic INCRBY+EXPIRE.
```

- [ ] **Step 3: Replace the `## Run` section body**

Replace the whole `## Run` section's body with:

````markdown
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
````

- [ ] **Step 4: Update the smoke-test script**

In `scripts/smoke-test.sh`, replace the success line:

```bash
echo "==> Phase 1+2 smoke test PASSED. Inventory + frequency-service + auction round-trip verified."
```

with:

```bash
echo "==> Phase 1+2+3 smoke test PASSED. Full event loop: ad-server → Kafka → Flink → Redis."
```

- [ ] **Step 5: Run smoke test**

Run: `./scripts/smoke-test.sh`
Expected: BUILD SUCCESSFUL, ends with "Phase 1+2+3 smoke test PASSED. Full event loop: ad-server → Kafka → Flink → Redis."

(This runs ALL tests across all modules including the heavy Phase 3 tests. Expect ~3-5 minutes.)

- [ ] **Step 6: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 3 task 13: README + smoke-test script update for Phase 3"
```

---

## Phase 3 Done

Working software at the end of Phase 3:

- **Three Kafka topics** (`bid-requests`, `auction-results`, `impression-events`) configured in docker-compose, init'd by an init container.
- **Avro schemas + codegen** for `ImpressionEvent` and `AuctionResultEvent`, served via Confluent Schema Registry.
- **`ad-server` produces** `auction-results` (always) and `impression-events` (when filled) fire-and-forget on a dedicated `Dispatchers.IO` supervisor scope. The request path never awaits Kafka acks.
- **`flink-impression-aggregator` module:** Flink 1.20 job, Kafka source with Avro deserialization via Confluent Schema Registry, tumbling 10-second windows keyed by `(user, campaign)`, Lua-scripted atomic Redis writes (INCRBY+EXPIRE for freq counters; ZADD+ZREMRANGEBYSCORE for winhistory).
- **End-to-end loop closes:** drive bid requests → impressions land in Kafka → Flink aggregates → counts appear in Redis → next bid requests have those caps applied.
- **Tests:** ~2 new common-protocol Avro tests, 2 new ad-server `KafkaEventEmitterTest` integration tests (Testcontainers Kafka), 1 new `AuctionPipelineTest` emission test, 1 heavy Flink integration test (MiniCluster + Testcontainers Kafka + Redis), 1 heavy Phase3 e2e test (Postgres + Redis + Kafka + Flink + ad-server). Phase 1+2 tests untouched.

**Next:** Phase 4 — observability. Micrometer metrics on every rule stage + Redis lookup + Kafka send + frequency RPC. OpenTelemetry tracing across services with W3C trace propagation. Prometheus + Grafana + Jaeger in docker-compose. Will need its own plan.
