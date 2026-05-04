# Phase 1 — Skeleton + Hot Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a Gradle multi-module Kotlin project with an OpenRTB 2.6-subset Ktor service that hydrates a Postgres-backed ad inventory at boot and runs a 5-stage rule engine (blocking → frequency+compsep [stubbed] → floor → selection) end-to-end. By the end of this plan, `curl POST http://localhost:8080/openrtb/bid` with a sample BidRequest returns a winning Bid.

**Architecture:** Single Gradle monorepo. Three modules in this phase: `common-protocol` (OpenRTB DTOs), `inventory-loader` (Postgres → in-memory snapshot), `ad-server` (Ktor service + rule engine). Frequency stage uses a `FakeFrequencyClient` that returns empty results (real gRPC client lands in Phase 2). All decisions per `docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`.

**Tech Stack:**
- Kotlin 2.1.0 / JVM target 21
- Gradle 8.11 + Kotlin DSL + version catalog
- Ktor 3.0.3 (Netty engine)
- kotlinx.serialization 1.7.3
- Flyway 11.0.0
- HikariCP 6.2.1
- Postgres JDBC 42.7.4
- Testcontainers 1.20.4 (Postgres module)
- JUnit Jupiter 5.11.3
- ktlint Gradle plugin 12.1.1
- Logback 1.5.12 + logstash-logback-encoder 8.0

---

## File Structure

```
kotlin_ad_server/
├── settings.gradle.kts
├── build.gradle.kts
├── gradle.properties
├── gradle/
│   ├── libs.versions.toml          # version catalog
│   └── wrapper/                    # gradle wrapper
├── .editorconfig
├── .gitignore
├── README.md                       # placeholder
│
├── common-protocol/
│   ├── build.gradle.kts
│   └── src/
│       ├── main/kotlin/com/github/robran/adserver/protocol/openrtb/
│       │   ├── BidRequest.kt       # BidRequest top-level
│       │   ├── Imp.kt              # Imp + Banner
│       │   ├── Context.kt          # Site, Device, User
│       │   └── BidResponse.kt      # BidResponse + SeatBid + Bid
│       └── test/kotlin/com/github/robran/adserver/protocol/openrtb/
│           └── OpenRtbSerializationTest.kt
│
├── inventory-loader/
│   ├── build.gradle.kts
│   └── src/
│       ├── main/kotlin/com/github/robran/adserver/inventory/
│       │   ├── Campaign.kt
│       │   ├── Creative.kt
│       │   ├── InventorySnapshot.kt
│       │   └── InventoryLoader.kt
│       ├── main/resources/
│       │   ├── db/migration/V1__init.sql
│       │   └── seed/campaigns.csv
│       └── test/kotlin/com/github/robran/adserver/inventory/
│           └── InventoryLoaderTest.kt
│
└── ad-server/
    ├── build.gradle.kts
    └── src/
        ├── main/kotlin/com/github/robran/adserver/
        │   ├── Application.kt              # Ktor entry point
        │   ├── AppConfig.kt
        │   ├── http/BidRoute.kt
        │   ├── http/HealthRoute.kt
        │   └── auction/
        │       ├── AuctionContext.kt
        │       ├── Candidate.kt
        │       ├── CandidateBuilder.kt
        │       ├── RuleStage.kt
        │       ├── AuctionPipeline.kt
        │       ├── FrequencyClient.kt
        │       ├── FakeFrequencyClient.kt
        │       └── stages/
        │           ├── BlockingPolicyStage.kt
        │           ├── FrequencyAndCompsepStage.kt
        │           ├── FloorPriceStage.kt
        │           └── SelectionStage.kt
        ├── main/resources/
        │   ├── application.conf
        │   └── logback.xml
        └── test/kotlin/com/github/robran/adserver/
            ├── auction/CandidateBuilderTest.kt
            ├── auction/stages/BlockingPolicyStageTest.kt
            ├── auction/stages/FrequencyAndCompsepStageTest.kt
            ├── auction/stages/FloorPriceStageTest.kt
            ├── auction/stages/SelectionStageTest.kt
            ├── auction/AuctionPipelineTest.kt
            └── http/BidRouteIntegrationTest.kt
```

---

## Task 1: Repo Scaffolding (Gradle wrapper, settings, version catalog)

**Files:**
- Create: `settings.gradle.kts`
- Create: `build.gradle.kts`
- Create: `gradle.properties`
- Create: `gradle/libs.versions.toml`
- Create: `.gitignore`
- Create: `.editorconfig`
- Create: `README.md`
- Create: `gradle/wrapper/gradle-wrapper.properties` (via `gradle wrapper`)

**Prerequisite:** Gradle ≥ 8.11 installed locally (`brew install gradle` or sdkman). Required only to bootstrap the wrapper; CI and dev workflow use `./gradlew` thereafter.

- [ ] **Step 1: Write `.gitignore`**

```gitignore
# Gradle
.gradle/
build/
!gradle-wrapper.jar

# IDE
.idea/
*.iml
*.ipr
*.iws
.vscode/

# Kotlin
*.class

# Logs
*.log

# OS
.DS_Store
Thumbs.db

# Local env
.env
.env.local

# Flyway local cache
flyway/

# Testcontainers
.testcontainers/
```

- [ ] **Step 2: Write `.editorconfig`**

```editorconfig
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
indent_style = space
indent_size = 4
trim_trailing_whitespace = true

[*.{kt,kts}]
ij_kotlin_imports_layout = *,java.**,javax.**,kotlin.**,^
ktlint_standard_no-wildcard-imports = enabled
ktlint_standard_filename = enabled
max_line_length = 140

[*.{yml,yaml,json,md}]
indent_size = 2
```

- [ ] **Step 3: Write `gradle.properties`**

```properties
org.gradle.jvmargs=-Xmx2g -XX:+UseG1GC -Dfile.encoding=UTF-8
org.gradle.parallel=true
org.gradle.caching=true
org.gradle.configuration-cache=true
kotlin.code.style=official
```

- [ ] **Step 4: Write `gradle/libs.versions.toml`**

```toml
[versions]
kotlin = "2.1.0"
kotlinx-serialization = "1.7.3"
kotlinx-coroutines = "1.9.0"
ktor = "3.0.3"
logback = "1.5.12"
logstash-logback = "8.0"
postgres-jdbc = "42.7.4"
hikaricp = "6.2.1"
flyway = "11.0.0"
testcontainers = "1.20.4"
junit = "5.11.3"
assertk = "0.28.1"
ktlint-plugin = "12.1.1"
config4k = "0.7.0"

[libraries]
# Kotlin
kotlinx-serialization-json = { module = "org.jetbrains.kotlinx:kotlinx-serialization-json", version.ref = "kotlinx-serialization" }
kotlinx-coroutines-core = { module = "org.jetbrains.kotlinx:kotlinx-coroutines-core", version.ref = "kotlinx-coroutines" }

# Ktor server
ktor-server-core = { module = "io.ktor:ktor-server-core", version.ref = "ktor" }
ktor-server-netty = { module = "io.ktor:ktor-server-netty", version.ref = "ktor" }
ktor-server-content-negotiation = { module = "io.ktor:ktor-server-content-negotiation", version.ref = "ktor" }
ktor-serialization-kotlinx-json = { module = "io.ktor:ktor-serialization-kotlinx-json", version.ref = "ktor" }
ktor-server-status-pages = { module = "io.ktor:ktor-server-status-pages", version.ref = "ktor" }
ktor-server-call-logging = { module = "io.ktor:ktor-server-call-logging", version.ref = "ktor" }
ktor-server-test-host = { module = "io.ktor:ktor-server-test-host", version.ref = "ktor" }
ktor-client-content-negotiation = { module = "io.ktor:ktor-client-content-negotiation", version.ref = "ktor" }

# Config (typesafe)
config4k = { module = "io.github.config4k:config4k", version.ref = "config4k" }

# Logging
logback-classic = { module = "ch.qos.logback:logback-classic", version.ref = "logback" }
logstash-logback-encoder = { module = "net.logstash.logback:logstash-logback-encoder", version.ref = "logstash-logback" }

# DB
postgres-jdbc = { module = "org.postgresql:postgresql", version.ref = "postgres-jdbc" }
hikaricp = { module = "com.zaxxer:HikariCP", version.ref = "hikaricp" }
flyway-core = { module = "org.flywaydb:flyway-core", version.ref = "flyway" }
flyway-postgres = { module = "org.flywaydb:flyway-database-postgresql", version.ref = "flyway" }

# Test
junit-bom = { module = "org.junit:junit-bom", version.ref = "junit" }
junit-jupiter = { module = "org.junit.jupiter:junit-jupiter" }
junit-jupiter-engine = { module = "org.junit.jupiter:junit-jupiter-engine" }
assertk = { module = "com.willowtreeapps.assertk:assertk", version.ref = "assertk" }
testcontainers-bom = { module = "org.testcontainers:testcontainers-bom", version.ref = "testcontainers" }
testcontainers-junit = { module = "org.testcontainers:junit-jupiter" }
testcontainers-postgres = { module = "org.testcontainers:postgresql" }

[plugins]
kotlin-jvm = { id = "org.jetbrains.kotlin.jvm", version.ref = "kotlin" }
kotlin-serialization = { id = "org.jetbrains.kotlin.plugin.serialization", version.ref = "kotlin" }
ktor = { id = "io.ktor.plugin", version.ref = "ktor" }
ktlint = { id = "org.jlleitschuh.gradle.ktlint", version.ref = "ktlint-plugin" }
```

- [ ] **Step 5: Write root `settings.gradle.kts`**

```kotlin
rootProject.name = "kotlin_ad_server"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

include(
    "common-protocol",
    "inventory-loader",
    "ad-server",
)
```

- [ ] **Step 6: Write root `build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.ktlint) apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = rootProject.libs.plugins.ktlint.get().pluginId)

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(21)
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            freeCompilerArgs.addAll(
                "-Xjsr305=strict",
                "-opt-in=kotlin.RequiresOptIn",
            )
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
        }
    }
}
```

- [ ] **Step 7: Write `README.md` placeholder**

```markdown
# kotlin_ad_server

Kotlin-based ad serving runtime. Design: `docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md`.

## Status

Phase 1 (skeleton + hot path) — in progress.

## Build

```bash
./gradlew build
```

## Run (after Phase 1)

```bash
./gradlew :ad-server:run
```
```

- [ ] **Step 8: Bootstrap Gradle wrapper**

Run: `gradle wrapper --gradle-version 8.11 --distribution-type bin`
Expected: creates `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties`.

- [ ] **Step 9: Verify build configures**

Run: `./gradlew help`
Expected: BUILD SUCCESSFUL. Gradle resolves the version catalog with no errors. (Three module directories don't exist yet — Gradle will warn but succeed because they're declared in settings.gradle.kts; if it fails on missing dirs, create empty placeholders `mkdir -p common-protocol inventory-loader ad-server` first.)

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "Phase 1 task 1: Gradle scaffolding with version catalog"
```

---

## Task 2: `common-protocol` Module Setup

**Files:**
- Create: `common-protocol/build.gradle.kts`
- Create: `common-protocol/src/main/kotlin/.gitkeep`
- Create: `common-protocol/src/test/kotlin/.gitkeep`

- [ ] **Step 1: Write `common-protocol/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
}

dependencies {
    api(libs.kotlinx.serialization.json)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
}
```

- [ ] **Step 2: Create source dirs with placeholder files**

Run:
```bash
mkdir -p common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb
mkdir -p common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/openrtb
touch common-protocol/src/main/kotlin/.gitkeep
touch common-protocol/src/test/kotlin/.gitkeep
```

- [ ] **Step 3: Verify module configures**

Run: `./gradlew :common-protocol:dependencies --configuration runtimeClasspath`
Expected: BUILD SUCCESSFUL. Output lists `kotlinx-serialization-json` in the resolved tree.

- [ ] **Step 4: Commit**

```bash
git add common-protocol/
git commit -m "Phase 1 task 2: common-protocol module setup"
```

---

## Task 3: OpenRTB BidRequest + Imp + Context Types

**Files:**
- Create: `common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/BidRequest.kt`
- Create: `common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/Imp.kt`
- Create: `common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/Context.kt`

OpenRTB 2.6 subset per spec: single-imp, banner only, no app/PMP/deals. Field names match the spec exactly so a reviewer recognizes them.

- [ ] **Step 1: Write `BidRequest.kt`**

```kotlin
package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

/**
 * OpenRTB 2.6 BidRequest — subset for kotlin_ad_server: single-imp, banner only.
 * Field names match the OpenRTB 2.6 spec (lowercased) so the schema is recognizable.
 */
@Serializable
data class BidRequest(
    val id: String,
    val imp: List<Imp>,
    val site: Site? = null,
    val device: Device? = null,
    val user: User? = null,
    val tmax: Int? = null,            // max auction time in ms
    val cur: List<String> = listOf("USD"),
    val bcat: List<String> = emptyList(),  // blocked IAB categories
    val badv: List<String> = emptyList(),  // blocked advertiser domains
    val bapp: List<String> = emptyList(),  // blocked app bundle IDs
)
```

- [ ] **Step 2: Write `Imp.kt`**

```kotlin
package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

@Serializable
data class Imp(
    val id: String,
    val banner: Banner,
    val bidfloor: Double = 0.0,
    val bidfloorcur: String = "USD",
    val tagid: String? = null,        // ad slot identifier on the publisher side
    val secure: Int = 0,              // 1 if HTTPS required
)

@Serializable
data class Banner(
    val w: Int,
    val h: Int,
    val pos: Int? = null,
)
```

- [ ] **Step 3: Write `Context.kt`**

```kotlin
package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

@Serializable
data class Site(
    val id: String,
    val domain: String? = null,
    val cat: List<String> = emptyList(),
    val page: String? = null,
)

@Serializable
data class Device(
    val ua: String? = null,
    val ip: String? = null,
    val devicetype: Int? = null,
    val os: String? = null,
    val geo: Geo? = null,
)

@Serializable
data class Geo(
    val country: String? = null,
    val region: String? = null,
    val city: String? = null,
)

@Serializable
data class User(
    val id: String? = null,
    val buyeruid: String? = null,
    val yob: Int? = null,
    val gender: String? = null,
)
```

- [ ] **Step 4: Verify it compiles**

Run: `./gradlew :common-protocol:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/
git commit -m "Phase 1 task 3: OpenRTB BidRequest, Imp, Context DTOs"
```

---

## Task 4: OpenRTB BidResponse Types

**Files:**
- Create: `common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/BidResponse.kt`

- [ ] **Step 1: Write `BidResponse.kt`**

```kotlin
package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

/**
 * OpenRTB 2.6 BidResponse. `nbr` (no-bid reason) is set when no winner found.
 */
@Serializable
data class BidResponse(
    val id: String,                       // matches BidRequest.id
    val seatbid: List<SeatBid> = emptyList(),
    val cur: String = "USD",
    val nbr: Int? = null,                 // no-bid reason; 0 = unknown error / no fill
)

@Serializable
data class SeatBid(
    val seat: String,                     // advertiser/seat id
    val bid: List<Bid>,
)

@Serializable
data class Bid(
    val id: String,                       // unique bid id
    val impid: String,                    // matches Imp.id
    val price: Double,                    // CPM, in BidResponse.cur
    val adid: String? = null,             // creative id
    val nurl: String? = null,             // win notice URL (out of scope this phase, kept for shape parity)
    val adm: String? = null,              // creative markup (out of scope; demo returns null)
    val cid: String? = null,              // campaign id
    val crid: String? = null,             // creative id (alternative to adid)
    val cat: List<String> = emptyList(),  // IAB categories
    val w: Int? = null,
    val h: Int? = null,
)

/**
 * Standard no-bid reasons per OpenRTB 2.6.
 */
object NoBidReason {
    const val UNKNOWN_ERROR = 0
    const val TECHNICAL_ERROR = 1
    const val INVALID_REQUEST = 2
    const val KNOWN_WEB_SPIDER = 3
    const val SUSPECTED_NON_HUMAN_TRAFFIC = 4
    const val UNSUPPORTED_DEVICE = 8
    const val BLOCKED_PUBLISHER = 9
    const val UNMATCHED_USER = 10
    // 100+ reserved for exchange-specific. We use 200+ for our stage rejections (debug-only).
    const val NO_CANDIDATES_AFTER_BLOCKING = 200
    const val NO_CANDIDATES_AFTER_FREQ_COMPSEP = 201
    const val NO_CANDIDATES_AFTER_FLOOR = 202
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :common-protocol:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add common-protocol/src/main/kotlin/com/github/robran/adserver/protocol/openrtb/BidResponse.kt
git commit -m "Phase 1 task 4: OpenRTB BidResponse DTOs"
```

---

## Task 5: Serialization Round-Trip Test (Golden BidRequest)

**Files:**
- Create: `common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/openrtb/OpenRtbSerializationTest.kt`
- Create: `common-protocol/src/test/resources/golden/bidrequest-banner.json`

- [ ] **Step 1: Write golden BidRequest JSON**

Create `common-protocol/src/test/resources/golden/bidrequest-banner.json`:

```json
{
  "id": "req-001",
  "imp": [
    {
      "id": "1",
      "banner": { "w": 300, "h": 250 },
      "bidfloor": 1.5,
      "bidfloorcur": "USD",
      "tagid": "slot-homepage-top",
      "secure": 1
    }
  ],
  "site": {
    "id": "site-42",
    "domain": "example.com",
    "cat": ["IAB1"],
    "page": "https://example.com/home"
  },
  "device": {
    "ua": "Mozilla/5.0",
    "ip": "203.0.113.1",
    "devicetype": 2,
    "os": "macOS"
  },
  "user": {
    "id": "user-zalia"
  },
  "tmax": 100,
  "cur": ["USD"],
  "bcat": ["IAB7-39"],
  "badv": ["bad.example.com"],
  "bapp": []
}
```

- [ ] **Step 2: Write the failing test**

`common-protocol/src/test/kotlin/com/github/robran/adserver/protocol/openrtb/OpenRtbSerializationTest.kt`:

```kotlin
package com.github.robran.adserver.protocol.openrtb

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

class OpenRtbSerializationTest {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = false
        explicitNulls = false
    }

    @Test
    fun `parses golden BidRequest banner fixture`() {
        val raw = this::class.java.getResource("/golden/bidrequest-banner.json")!!.readText()
        val req = json.decodeFromString<BidRequest>(raw)

        assertThat(req.id).isEqualTo("req-001")
        assertThat(req.imp).hasSize(1)
        assertThat(req.imp[0].banner.w).isEqualTo(300)
        assertThat(req.imp[0].banner.h).isEqualTo(250)
        assertThat(req.imp[0].bidfloor).isEqualTo(1.5)
        assertThat(req.user?.id).isEqualTo("user-zalia")
        assertThat(req.bcat).isEqualTo(listOf("IAB7-39"))
    }

    @Test
    fun `BidResponse round-trips through JSON`() {
        val resp = BidResponse(
            id = "req-001",
            seatbid = listOf(
                SeatBid(
                    seat = "advertiser-7",
                    bid = listOf(
                        Bid(
                            id = "bid-1",
                            impid = "1",
                            price = 2.50,
                            cid = "campaign-9",
                            crid = "creative-3",
                            cat = listOf("IAB3"),
                            w = 300,
                            h = 250,
                        ),
                    ),
                ),
            ),
            cur = "USD",
        )

        val encoded = json.encodeToString(BidResponse.serializer(), resp)
        val decoded = json.decodeFromString<BidResponse>(encoded)

        assertThat(decoded).isEqualTo(resp)
    }
}
```

Note: import for `hasSize` — adjust if your assertk version uses a different import path. The standard is `import assertk.assertions.hasSize`.

Add it:
```kotlin
import assertk.assertions.hasSize
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `./gradlew :common-protocol:test --tests "com.github.robran.adserver.protocol.openrtb.OpenRtbSerializationTest"`
Expected: PASS, both tests green.

- [ ] **Step 4: Commit**

```bash
git add common-protocol/src/test/
git commit -m "Phase 1 task 5: OpenRTB serialization round-trip tests"
```

---

## Task 6: `inventory-loader` Module + Domain Types

**Files:**
- Create: `inventory-loader/build.gradle.kts`
- Create: `inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/Campaign.kt`
- Create: `inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/Creative.kt`
- Create: `inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/InventorySnapshot.kt`

- [ ] **Step 1: Write `inventory-loader/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

dependencies {
    api(project(":common-protocol"))
    implementation(libs.postgres.jdbc)
    implementation(libs.hikaricp)
    implementation(libs.flyway.core)
    implementation(libs.flyway.postgres)
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgres)
    testImplementation(libs.logback.classic)
}
```

- [ ] **Step 2: Write `Campaign.kt`**

```kotlin
package com.github.robran.adserver.inventory

/**
 * A served-side campaign — distinct from an advertiser. One campaign owns many creatives.
 * Held in the in-memory snapshot; never read from Postgres on the request path.
 */
data class Campaign(
    val id: String,
    val advertiserId: String,
    val advertiserDomain: String,
    val category: String,             // primary IAB category, e.g., "IAB3"
    val bidPrice: Double,             // CPM the campaign will pay if it wins, USD
    val frequencyCap: Int,            // per-user cap over the cap window (24h)
    val active: Boolean,
    val creatives: List<Creative>,
)
```

- [ ] **Step 3: Write `Creative.kt`**

```kotlin
package com.github.robran.adserver.inventory

data class Creative(
    val id: String,
    val campaignId: String,
    val width: Int,
    val height: Int,
    val markup: String,               // demo: opaque string
)

/** Helper: does this creative match the requested banner size? */
fun Creative.matches(bannerW: Int, bannerH: Int): Boolean =
    width == bannerW && height == bannerH
```

- [ ] **Step 4: Write `InventorySnapshot.kt`**

```kotlin
package com.github.robran.adserver.inventory

import java.time.Instant

/**
 * Immutable snapshot of all active campaigns + creatives. Hydrated at boot from Postgres
 * by [InventoryLoader] and held in memory for the lifetime of the process. Reload is a future
 * concern (admin endpoint). The request path never touches the DB — only this snapshot.
 */
class InventorySnapshot(
    val campaigns: List<Campaign>,
    val loadedAt: Instant,
) {
    /** Index for fast lookup; built once at construction. */
    val byCampaignId: Map<String, Campaign> = campaigns.associateBy { it.id }

    val size: Int get() = campaigns.size

    fun activeCampaigns(): Sequence<Campaign> = campaigns.asSequence().filter { it.active }
}
```

- [ ] **Step 5: Verify compile**

Run: `./gradlew :inventory-loader:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add inventory-loader/
git commit -m "Phase 1 task 6: inventory-loader module + Campaign/Creative/Snapshot types"
```

---

## Task 7: Postgres Schema (Flyway V1)

**Files:**
- Create: `inventory-loader/src/main/resources/db/migration/V1__init.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V1__init.sql: initial inventory schema for kotlin_ad_server

CREATE TABLE advertisers (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    domain          TEXT NOT NULL UNIQUE
);

CREATE TABLE campaigns (
    id              TEXT PRIMARY KEY,
    advertiser_id   TEXT NOT NULL REFERENCES advertisers(id),
    category        TEXT NOT NULL,             -- IAB primary category
    bid_price       NUMERIC(10, 4) NOT NULL,   -- USD CPM
    frequency_cap   INTEGER NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_campaigns_active ON campaigns (active) WHERE active = TRUE;

CREATE TABLE creatives (
    id              TEXT PRIMARY KEY,
    campaign_id     TEXT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    width           INTEGER NOT NULL,
    height          INTEGER NOT NULL,
    markup          TEXT NOT NULL
);

CREATE INDEX idx_creatives_campaign ON creatives (campaign_id);
```

- [ ] **Step 2: Verify it's at the right path**

Run: `ls inventory-loader/src/main/resources/db/migration/`
Expected: `V1__init.sql`

- [ ] **Step 3: Commit**

```bash
git add inventory-loader/src/main/resources/db/migration/
git commit -m "Phase 1 task 7: Postgres schema (Flyway V1)"
```

---

## Task 8: Seed Data CSV

**Files:**
- Create: `inventory-loader/src/main/resources/seed/campaigns.csv`
- Create: `inventory-loader/src/main/resources/seed/creatives.csv`
- Create: `inventory-loader/src/main/resources/seed/advertisers.csv`

These are loaded into Postgres in tests and at boot to populate the in-memory snapshot. ~50 campaigns spread across 8 advertisers, with IAB-category overlap (so competitive separation has work to do in Phase 2).

- [ ] **Step 1: Write `advertisers.csv`**

```csv
id,name,domain
adv-001,AcmeBank,acmebank.example.com
adv-002,RocketTech,rockettech.example.com
adv-003,GreenBites,greenbites.example.com
adv-004,FastFleet,fastfleet.example.com
adv-005,NovaPay,novapay.example.com
adv-006,WanderHotels,wander.example.com
adv-007,EduPath,edupath.example.com
adv-008,FitForge,fitforge.example.com
```

- [ ] **Step 2: Write `campaigns.csv`**

50 campaigns, IAB categories chosen so several share a category (drives competitive separation in Phase 2). Bid prices vary 0.5–8.0 USD CPM. Frequency caps vary 3–10 per 24h.

```csv
id,advertiser_id,category,bid_price,frequency_cap,active
camp-001,adv-001,IAB13,4.50,5,true
camp-002,adv-001,IAB13,3.80,5,true
camp-003,adv-001,IAB13-1,2.10,3,true
camp-004,adv-002,IAB19,5.20,7,true
camp-005,adv-002,IAB19-6,4.10,7,true
camp-006,adv-002,IAB19,3.30,5,true
camp-007,adv-002,IAB19-30,2.50,4,true
camp-008,adv-003,IAB8,1.90,8,true
camp-009,adv-003,IAB8-1,2.40,6,true
camp-010,adv-003,IAB8-5,1.60,5,true
camp-011,adv-003,IAB8,3.10,5,true
camp-012,adv-004,IAB2,2.20,6,true
camp-013,adv-004,IAB2-2,3.40,5,true
camp-014,adv-004,IAB2,1.80,8,true
camp-015,adv-005,IAB13,6.10,3,true
camp-016,adv-005,IAB13-3,5.30,3,true
camp-017,adv-005,IAB13-7,4.40,4,true
camp-018,adv-006,IAB20,3.70,5,true
camp-019,adv-006,IAB20-1,2.60,7,true
camp-020,adv-006,IAB20-3,2.10,7,true
camp-021,adv-007,IAB5,1.40,10,true
camp-022,adv-007,IAB5-1,1.20,10,true
camp-023,adv-007,IAB5-3,1.80,8,true
camp-024,adv-008,IAB16,2.90,6,true
camp-025,adv-008,IAB16-7,3.20,5,true
camp-026,adv-008,IAB16,2.30,7,true
camp-027,adv-001,IAB13,4.10,5,true
camp-028,adv-002,IAB19,3.60,6,true
camp-029,adv-003,IAB8,2.70,6,true
camp-030,adv-004,IAB2,2.00,7,true
camp-031,adv-005,IAB13,5.80,3,true
camp-032,adv-006,IAB20,3.10,6,true
camp-033,adv-007,IAB5,1.50,9,true
camp-034,adv-008,IAB16,3.00,6,true
camp-035,adv-001,IAB13-2,3.20,5,true
camp-036,adv-002,IAB19-15,4.70,5,true
camp-037,adv-003,IAB8-9,1.70,7,true
camp-038,adv-004,IAB2-5,2.80,6,true
camp-039,adv-005,IAB13-9,5.10,4,true
camp-040,adv-006,IAB20-26,2.40,7,true
camp-041,adv-007,IAB5-13,1.30,10,true
camp-042,adv-008,IAB16-1,2.60,7,true
camp-043,adv-001,IAB13,3.50,5,true
camp-044,adv-002,IAB19,4.20,6,true
camp-045,adv-003,IAB8,2.00,7,true
camp-046,adv-004,IAB2,2.50,6,true
camp-047,adv-005,IAB13,4.80,4,true
camp-048,adv-006,IAB20,3.40,5,true
camp-049,adv-007,IAB5,1.60,9,true
camp-050,adv-008,IAB16,2.80,6,true
```

- [ ] **Step 3: Write `creatives.csv`**

Each campaign gets 1–2 creatives in standard IAB sizes (300×250, 728×90, 160×600, 300×600). Most campaigns have 300×250 — the dominant slot size.

```csv
id,campaign_id,width,height,markup
cre-001a,camp-001,300,250,<acmebank-300x250>
cre-001b,camp-001,728,90,<acmebank-728x90>
cre-002a,camp-002,300,250,<acmebank-2-300x250>
cre-003a,camp-003,300,250,<acmebank-3-300x250>
cre-004a,camp-004,300,250,<rockettech-300x250>
cre-004b,camp-004,160,600,<rockettech-160x600>
cre-005a,camp-005,300,250,<rockettech-2-300x250>
cre-006a,camp-006,728,90,<rockettech-3-728x90>
cre-007a,camp-007,300,600,<rockettech-4-300x600>
cre-008a,camp-008,300,250,<greenbites-300x250>
cre-009a,camp-009,300,250,<greenbites-2-300x250>
cre-010a,camp-010,728,90,<greenbites-3-728x90>
cre-011a,camp-011,300,250,<greenbites-4-300x250>
cre-012a,camp-012,300,250,<fastfleet-300x250>
cre-012b,camp-012,160,600,<fastfleet-160x600>
cre-013a,camp-013,300,250,<fastfleet-2-300x250>
cre-014a,camp-014,728,90,<fastfleet-3-728x90>
cre-015a,camp-015,300,250,<novapay-300x250>
cre-015b,camp-015,300,600,<novapay-300x600>
cre-016a,camp-016,300,250,<novapay-2-300x250>
cre-017a,camp-017,728,90,<novapay-3-728x90>
cre-018a,camp-018,300,250,<wander-300x250>
cre-019a,camp-019,300,250,<wander-2-300x250>
cre-020a,camp-020,728,90,<wander-3-728x90>
cre-021a,camp-021,300,250,<edupath-300x250>
cre-022a,camp-022,300,250,<edupath-2-300x250>
cre-023a,camp-023,160,600,<edupath-3-160x600>
cre-024a,camp-024,300,250,<fitforge-300x250>
cre-024b,camp-024,300,600,<fitforge-300x600>
cre-025a,camp-025,300,250,<fitforge-2-300x250>
cre-026a,camp-026,728,90,<fitforge-3-728x90>
cre-027a,camp-027,300,250,<acmebank-x-300x250>
cre-028a,camp-028,300,250,<rockettech-x-300x250>
cre-029a,camp-029,300,250,<greenbites-x-300x250>
cre-030a,camp-030,300,250,<fastfleet-x-300x250>
cre-031a,camp-031,300,250,<novapay-x-300x250>
cre-032a,camp-032,300,250,<wander-x-300x250>
cre-033a,camp-033,300,250,<edupath-x-300x250>
cre-034a,camp-034,300,250,<fitforge-x-300x250>
cre-035a,camp-035,300,250,<acmebank-y-300x250>
cre-036a,camp-036,300,250,<rockettech-y-300x250>
cre-037a,camp-037,300,250,<greenbites-y-300x250>
cre-038a,camp-038,300,250,<fastfleet-y-300x250>
cre-039a,camp-039,300,250,<novapay-y-300x250>
cre-040a,camp-040,300,250,<wander-y-300x250>
cre-041a,camp-041,300,250,<edupath-y-300x250>
cre-042a,camp-042,300,250,<fitforge-y-300x250>
cre-043a,camp-043,300,250,<acmebank-z-300x250>
cre-044a,camp-044,300,250,<rockettech-z-300x250>
cre-045a,camp-045,300,250,<greenbites-z-300x250>
cre-046a,camp-046,300,250,<fastfleet-z-300x250>
cre-047a,camp-047,300,250,<novapay-z-300x250>
cre-048a,camp-048,300,250,<wander-z-300x250>
cre-049a,camp-049,300,250,<edupath-z-300x250>
cre-050a,camp-050,300,250,<fitforge-z-300x250>
```

- [ ] **Step 4: Commit**

```bash
git add inventory-loader/src/main/resources/seed/
git commit -m "Phase 1 task 8: seed CSVs (8 advertisers, 50 campaigns, 56 creatives)"
```

---

## Task 9: InventoryLoader (Postgres → Snapshot) + Testcontainers Test

**Files:**
- Create: `inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/InventoryLoader.kt`
- Create: `inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/SeedLoader.kt`
- Create: `inventory-loader/src/test/kotlin/com/github/robran/adserver/inventory/InventoryLoaderTest.kt`
- Create: `inventory-loader/src/test/resources/logback-test.xml`

- [ ] **Step 1: Write `InventoryLoader.kt`**

```kotlin
package com.github.robran.adserver.inventory

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.time.Instant
import javax.sql.DataSource

class InventoryLoader(private val dataSource: DataSource) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Read all campaigns + their creatives from Postgres and assemble an immutable [InventorySnapshot].
     * Returns active campaigns only (active=true).
     */
    fun load(): InventorySnapshot {
        val started = System.nanoTime()
        val campaigns = readCampaigns()
        val creativesByCampaign = readCreativesGroupedByCampaign()
        val advertiserDomainsById = readAdvertiserDomains()

        val assembled = campaigns
            .filter { it.active }
            .map { c ->
                c.copy(
                    advertiserDomain = advertiserDomainsById[c.advertiserId]
                        ?: error("orphan campaign ${c.id}: advertiser ${c.advertiserId} not found"),
                    creatives = creativesByCampaign[c.id].orEmpty(),
                )
            }

        val durMs = (System.nanoTime() - started) / 1_000_000
        log.info("Inventory snapshot loaded: {} campaigns ({} ms)", assembled.size, durMs)
        return InventorySnapshot(assembled, Instant.now())
    }

    private fun readCampaigns(): List<Campaign> = dataSource.connection.use { conn ->
        conn.prepareStatement(
            """
            SELECT id, advertiser_id, category, bid_price, frequency_cap, active
            FROM campaigns
            """.trimIndent(),
        ).use { ps ->
            ps.executeQuery().use { rs ->
                buildList {
                    while (rs.next()) {
                        add(
                            Campaign(
                                id = rs.getString("id"),
                                advertiserId = rs.getString("advertiser_id"),
                                advertiserDomain = "",  // filled in by load()
                                category = rs.getString("category"),
                                bidPrice = rs.getBigDecimal("bid_price").toDouble(),
                                frequencyCap = rs.getInt("frequency_cap"),
                                active = rs.getBoolean("active"),
                                creatives = emptyList(),  // filled in by load()
                            ),
                        )
                    }
                }
            }
        }
    }

    private fun readCreativesGroupedByCampaign(): Map<String, List<Creative>> =
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT id, campaign_id, width, height, markup
                FROM creatives
                """.trimIndent(),
            ).use { ps ->
                ps.executeQuery().use { rs ->
                    val acc = mutableMapOf<String, MutableList<Creative>>()
                    while (rs.next()) {
                        val campaignId = rs.getString("campaign_id")
                        acc.getOrPut(campaignId) { mutableListOf() }.add(
                            Creative(
                                id = rs.getString("id"),
                                campaignId = campaignId,
                                width = rs.getInt("width"),
                                height = rs.getInt("height"),
                                markup = rs.getString("markup"),
                            ),
                        )
                    }
                    acc
                }
            }
        }

    private fun readAdvertiserDomains(): Map<String, String> = dataSource.connection.use { conn ->
        conn.prepareStatement("SELECT id, domain FROM advertisers").use { ps ->
            ps.executeQuery().use { rs ->
                buildMap { while (rs.next()) put(rs.getString("id"), rs.getString("domain")) }
            }
        }
    }

    companion object {
        fun migrate(jdbcUrl: String, user: String, password: String) {
            Flyway.configure()
                .dataSource(jdbcUrl, user, password)
                .locations("classpath:db/migration")
                .load()
                .migrate()
        }

        fun pooledDataSource(jdbcUrl: String, user: String, password: String): HikariDataSource {
            val cfg = HikariConfig().apply {
                this.jdbcUrl = jdbcUrl
                this.username = user
                this.password = password
                this.maximumPoolSize = 4   // boot-time only, not on hot path
                this.poolName = "inventory-pool"
            }
            return HikariDataSource(cfg)
        }
    }
}
```

- [ ] **Step 2: Write `SeedLoader.kt`**

```kotlin
package com.github.robran.adserver.inventory

import org.slf4j.LoggerFactory
import javax.sql.DataSource

/**
 * Loads the seed CSVs from classpath into a freshly migrated database.
 * Use in tests and (optionally) at boot in dev.
 */
object SeedLoader {
    private val log = LoggerFactory.getLogger(javaClass)

    fun seed(dataSource: DataSource) {
        seedTable(dataSource, "advertisers", "/seed/advertisers.csv", listOf("id", "name", "domain"))
        seedTable(
            dataSource,
            "campaigns",
            "/seed/campaigns.csv",
            listOf("id", "advertiser_id", "category", "bid_price", "frequency_cap", "active"),
        )
        seedTable(
            dataSource,
            "creatives",
            "/seed/creatives.csv",
            listOf("id", "campaign_id", "width", "height", "markup"),
        )
    }

    private fun seedTable(dataSource: DataSource, table: String, resource: String, cols: List<String>) {
        val rows = parseCsv(resource)
        val placeholders = cols.joinToString(",") { "?" }
        val sql = "INSERT INTO $table (${cols.joinToString(",")}) VALUES ($placeholders) ON CONFLICT (id) DO NOTHING"
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(sql).use { ps ->
                for (row in rows) {
                    cols.forEachIndexed { i, col ->
                        val v = row[col] ?: error("missing column $col in $resource: $row")
                        when (col) {
                            "active" -> ps.setBoolean(i + 1, v.toBoolean())
                            "frequency_cap", "width", "height" -> ps.setInt(i + 1, v.toInt())
                            "bid_price" -> ps.setBigDecimal(i + 1, java.math.BigDecimal(v))
                            else -> ps.setString(i + 1, v)
                        }
                    }
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        }
        log.info("Seeded {} rows into {} from {}", rows.size, table, resource)
    }

    private fun parseCsv(resource: String): List<Map<String, String>> {
        val text = SeedLoader::class.java.getResourceAsStream(resource)
            ?.bufferedReader()
            ?.readText()
            ?: error("resource not found: $resource")
        val lines = text.lines().filter { it.isNotBlank() }
        if (lines.size < 2) return emptyList()
        val header = lines.first().split(",")
        return lines.drop(1).map { line ->
            val cells = line.split(",")
            require(cells.size == header.size) { "malformed CSV row: $line" }
            header.zip(cells).toMap()
        }
    }
}
```

- [ ] **Step 3: Write `logback-test.xml`**

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
</configuration>
```

- [ ] **Step 4: Write the failing test**

`inventory-loader/src/test/kotlin/com/github/robran/adserver/inventory/InventoryLoaderTest.kt`:

```kotlin
package com.github.robran.adserver.inventory

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InventoryLoaderTest {

    @Container
    val postgres: PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("kotlin_ad_server_test")
        .withUsername("test")
        .withPassword("test")

    @BeforeAll
    fun migrateAndSeed() {
        postgres.start()
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds -> SeedLoader.seed(ds) }
    }

    @AfterAll
    fun stop() {
        postgres.stop()
    }

    @Test
    fun `loads 50 campaigns from seed data`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            assertThat(snapshot.size).isEqualTo(50)
        }
    }

    @Test
    fun `each campaign has at least one creative and an advertiser domain`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            for (c in snapshot.campaigns) {
                assertThat(c.creatives.size).isGreaterThan(0)
                assertThat(c.advertiserDomain).isNotNull()
                assertThat(c.advertiserDomain).contains("example.com")
            }
        }
    }

    @Test
    fun `byCampaignId index has every campaign`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            assertThat(snapshot.byCampaignId["camp-001"]).isNotNull()
            assertThat(snapshot.byCampaignId["camp-050"]).isNotNull()
            assertThat(snapshot.byCampaignId.size).isEqualTo(snapshot.size)
        }
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :inventory-loader:test`
Expected: PASS, 3 tests green. (Requires Docker daemon running for Testcontainers.)

- [ ] **Step 6: Commit**

```bash
git add inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/InventoryLoader.kt \
        inventory-loader/src/main/kotlin/com/github/robran/adserver/inventory/SeedLoader.kt \
        inventory-loader/src/test/kotlin/com/github/robran/adserver/inventory/InventoryLoaderTest.kt \
        inventory-loader/src/test/resources/logback-test.xml
git commit -m "Phase 1 task 9: InventoryLoader with Testcontainers integration test"
```

---

## Task 10: ad-server Module Scaffolding + Health Endpoint

**Files:**
- Create: `ad-server/build.gradle.kts`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/AppConfig.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/http/HealthRoute.kt`
- Create: `ad-server/src/main/resources/application.conf`
- Create: `ad-server/src/main/resources/logback.xml`

- [ ] **Step 1: Write `ad-server/build.gradle.kts`**

```kotlin
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.ktor)
    application
}

application {
    mainClass.set("com.github.robran.adserver.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))
    implementation(project(":inventory-loader"))

    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.status.pages)
    implementation(libs.ktor.server.call.logging)

    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation(libs.logstash.logback.encoder)
    implementation(libs.postgres.jdbc)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.content.negotiation)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgres)
}
```

- [ ] **Step 2: Write `application.conf`**

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
}
```

- [ ] **Step 3: Write `logback.xml`**

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
    <logger name="io.netty" level="WARN"/>
    <logger name="com.zaxxer.hikari" level="WARN"/>
    <logger name="org.flywaydb" level="INFO"/>
</configuration>
```

- [ ] **Step 4: Write `AppConfig.kt`**

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

data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig =
            raw.extract("adserver")
    }
}
```

- [ ] **Step 5: Write `HealthRoute.kt`**

```kotlin
package com.github.robran.adserver.http

import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Liveness flips to true once the process is up. Readiness flips to true after inventory hydration.
 */
class HealthState {
    val ready = AtomicBoolean(false)
}

fun Route.healthRoutes(state: HealthState) {
    get("/healthz") {
        call.respondText("ok", status = HttpStatusCode.OK)
    }
    get("/readyz") {
        if (state.ready.get()) {
            call.respondText("ready", status = HttpStatusCode.OK)
        } else {
            call.respondText("not ready", status = HttpStatusCode.ServiceUnavailable)
        }
    }
}
```

- [ ] **Step 6: Write `Application.kt`**

Per the spec ("Healthcheck blocks on snapshot load"), the server only starts accepting traffic *after* the inventory snapshot is hydrated. This avoids the need for any "is the snapshot ready yet?" guard on the request path.

```kotlin
package com.github.robran.adserver

import com.github.robran.adserver.http.HealthState
import com.github.robran.adserver.http.healthRoutes
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.Application")

fun main() {
    val config = AppConfig.load()

    if (!config.inventory.skipMigrate) {
        InventoryLoader.migrate(
            config.inventory.jdbcUrl,
            config.inventory.user,
            config.inventory.password,
        )
    }
    val ds = InventoryLoader.pooledDataSource(
        config.inventory.jdbcUrl,
        config.inventory.user,
        config.inventory.password,
    )
    val snapshot = InventoryLoader(ds).load()
    log.info("ad-server starting: {} campaigns loaded", snapshot.size)

    val healthState = HealthState().apply { ready.set(true) }

    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, snapshot)
    }.start(wait = true)
}

fun Application.adServerModule(
    healthState: HealthState,
    @Suppress("UNUSED_PARAMETER") snapshot: InventorySnapshot,
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

    routing {
        healthRoutes(healthState)
        // bid route wired in Task 19
    }
}
```

- [ ] **Step 7: Verify it builds**

Run: `./gradlew :ad-server:build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: Commit**

```bash
git add ad-server/
git commit -m "Phase 1 task 10: ad-server Ktor scaffolding + health endpoints"
```

---

## Task 11: AuctionContext, Candidate, CandidateBuilder

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionContext.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/Candidate.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/CandidateBuilder.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/CandidateBuilderTest.kt`

- [ ] **Step 1: Write `Candidate.kt`**

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative

/**
 * One eligible (campaign, creative) pair for an impression. The auction works on a list of these.
 * Filtered down by each [RuleStage]; the [SelectionStage] picks one (or none).
 */
data class Candidate(
    val campaign: Campaign,
    val creative: Creative,
    val bidPrice: Double = campaign.bidPrice,
)
```

- [ ] **Step 2: Write `AuctionContext.kt`**

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.openrtb.BidRequest

/**
 * Per-request immutable context. Carries the inbound request, the resolved user id, and
 * the imp under auction (Phase 1 supports single-imp only).
 *
 * Future extensions (later phases): MeterRegistry, OpenTelemetry Span, gRPC client handles.
 */
data class AuctionContext(
    val request: BidRequest,
    val userId: String,
    val impIndex: Int = 0,
) {
    val imp get() = request.imp[impIndex]
}
```

- [ ] **Step 3: Write `CandidateBuilder.kt`**

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.inventory.matches

/**
 * Builds the initial candidate list by matching the imp's banner size against each campaign's creatives.
 * A campaign contributes one [Candidate] per creative that matches the requested size; campaigns
 * with no matching creative are excluded entirely. No I/O, pure function over the snapshot.
 */
class CandidateBuilder(private val snapshot: InventorySnapshot) {

    fun build(ctx: AuctionContext): List<Candidate> {
        val w = ctx.imp.banner.w
        val h = ctx.imp.banner.h
        return buildList {
            for (campaign in snapshot.activeCampaigns()) {
                for (creative in campaign.creatives) {
                    if (creative.matches(w, h)) {
                        add(Candidate(campaign, creative))
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 4: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/CandidateBuilderTest.kt`:

```kotlin
package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import org.junit.jupiter.api.Test
import java.time.Instant

class CandidateBuilderTest {

    private fun campaign(id: String, active: Boolean = true, vararg sizes: Pair<Int, Int>): Campaign {
        val creatives = sizes.mapIndexed { i, (w, h) ->
            Creative(id = "$id-cre-$i", campaignId = id, width = w, height = h, markup = "<m>")
        }
        return Campaign(
            id = id,
            advertiserId = "adv-x",
            advertiserDomain = "x.example.com",
            category = "IAB1",
            bidPrice = 1.0,
            frequencyCap = 5,
            active = active,
            creatives = creatives,
        )
    }

    private fun snapshot(vararg campaigns: Campaign) =
        InventorySnapshot(campaigns.toList(), Instant.now())

    private fun request(w: Int, h: Int) = BidRequest(
        id = "req",
        imp = listOf(Imp(id = "1", banner = Banner(w = w, h = h))),
        user = null,
    )

    @Test
    fun `includes only creatives whose size matches the imp banner`() {
        val s = snapshot(
            campaign("c1", true, 300 to 250, 728 to 90),
            campaign("c2", true, 300 to 250),
            campaign("c3", true, 160 to 600),
        )
        val ctx = AuctionContext(request = request(300, 250), userId = "u")
        val candidates = CandidateBuilder(s).build(ctx)

        val ids = candidates.map { it.creative.id }
        assertThat(ids).containsExactlyInAnyOrder("c1-cre-0", "c2-cre-0")
    }

    @Test
    fun `excludes inactive campaigns`() {
        val s = snapshot(
            campaign("c1", active = false, 300 to 250),
            campaign("c2", active = true, 300 to 250),
        )
        val candidates = CandidateBuilder(s).build(
            AuctionContext(request = request(300, 250), userId = "u"),
        )
        assertThat(candidates).hasSize(1)
    }

    @Test
    fun `returns empty when no creative matches`() {
        val s = snapshot(campaign("c1", true, 300 to 250))
        val candidates = CandidateBuilder(s).build(
            AuctionContext(request = request(728, 90), userId = "u"),
        )
        assertThat(candidates).isEmpty()
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.CandidateBuilderTest"`
Expected: PASS, 3 tests green.

- [ ] **Step 6: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/ \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/CandidateBuilderTest.kt
git commit -m "Phase 1 task 11: AuctionContext, Candidate, CandidateBuilder"
```

---

## Task 12: RuleStage Interface

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/RuleStage.kt`

- [ ] **Step 1: Write `RuleStage.kt`**

```kotlin
package com.github.robran.adserver.auction

/**
 * One step in the auction rule pipeline. Each stage reads the input candidate list and returns
 * the surviving subset (possibly empty). Stages are run in declaration order:
 *
 *   blocking → frequency+compsep → floor → selection
 *
 * `suspend` because Phase 2 introduces a gRPC call inside [stages.FrequencyAndCompsepStage].
 * Phase 1 stages are CPU-only but signed `suspend` for forward compatibility.
 */
fun interface RuleStage {
    suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate>
}
```

- [ ] **Step 2: Verify compile**

Run: `./gradlew :ad-server:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/RuleStage.kt
git commit -m "Phase 1 task 12: RuleStage suspend interface"
```

---

## Task 13: BlockingPolicyStage + Tests

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStage.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStageTest.kt`

The first stage. In-memory predicate, no I/O. Drops candidates whose campaign matches `BidRequest.bcat` (blocked categories), `badv` (blocked advertiser domains), or whose advertiser is on the blocklist.

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStageTest.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class BlockingPolicyStageTest {

    private fun candidate(id: String, category: String, advertiserDomain: String): Candidate {
        val campaign = Campaign(
            id = id,
            advertiserId = "adv-$id",
            advertiserDomain = advertiserDomain,
            category = category,
            bidPrice = 1.0,
            frequencyCap = 5,
            active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private fun ctx(bcat: List<String> = emptyList(), badv: List<String> = emptyList()) =
        AuctionContext(
            request = BidRequest(
                id = "r",
                imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
                bcat = bcat,
                badv = badv,
            ),
            userId = "u",
        )

    @Test
    fun `passes through when no blocks specified`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "ok.example.com"),
            candidate("c2", "IAB2", "fine.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(), candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c2")
    }

    @Test
    fun `drops candidates whose category is in bcat`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "a.example.com"),
            candidate("c2", "IAB7-39", "b.example.com"),
            candidate("c3", "IAB3", "c.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB7-39")), candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c3")
    }

    @Test
    fun `drops candidates whose advertiser domain is in badv`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "good.example.com"),
            candidate("c2", "IAB2", "blocked.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(badv = listOf("blocked.example.com")), candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }

    @Test
    fun `bcat matching is exact, not prefix`() = runTest {
        // IAB13 must NOT block IAB13-1 in this implementation. OpenRTB allows either; we choose exact.
        // (Documented: this is a design choice for the spec subset.)
        val candidates = listOf(
            candidate("c1", "IAB13-1", "a.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB13")), candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }

    @Test
    fun `returns empty when all candidates are blocked`() = runTest {
        val candidates = listOf(candidate("c1", "IAB1", "x.example.com"))
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB1")), candidates)
        assertThat(out).isEmpty()
    }
}
```

Add the coroutines-test dependency if not already pulled in transitively. In `ad-server/build.gradle.kts` testImplementation block, add:

```kotlin
testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.BlockingPolicyStageTest"`
Expected: FAIL — `BlockingPolicyStage` does not exist.

- [ ] **Step 3: Write the minimal implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStage.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.RuleStage

/**
 * Drops candidates whose campaign category is in [BidRequest.bcat] or whose advertiser
 * domain is in [BidRequest.badv]. Exact-match category check (not prefix); see test for rationale.
 * Pure in-memory predicate, no I/O.
 */
class BlockingPolicyStage : RuleStage {
    override suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate> {
        val bcat = ctx.request.bcat.toSet()
        val badv = ctx.request.badv.toSet()
        if (bcat.isEmpty() && badv.isEmpty()) return candidates
        return candidates.filter { c ->
            c.campaign.category !in bcat && c.campaign.advertiserDomain !in badv
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.BlockingPolicyStageTest"`
Expected: PASS, 5 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStage.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/BlockingPolicyStageTest.kt \
        ad-server/build.gradle.kts
git commit -m "Phase 1 task 13: BlockingPolicyStage with bcat/badv filtering"
```

---

## Task 14: FrequencyClient Interface + FakeFrequencyClient

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/FrequencyClient.kt`
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/FakeFrequencyClient.kt`

The interface that the real gRPC client (Phase 2) will implement. Phase 1 uses an in-memory fake.

- [ ] **Step 1: Write `FrequencyClient.kt`**

```kotlin
package com.github.robran.adserver.auction

/**
 * Single combined call: returns per-campaign impression counts AND the IAB categories
 * recently served to this user, in one round-trip. Phase 2 swaps the implementation for a
 * gRPC client; Phase 1 uses [FakeFrequencyClient]. Suspending so the real impl can be async.
 */
interface FrequencyClient {
    suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult
}

data class EnrichResult(
    /** Map of campaignId → current count over the cap window. Missing campaign means count=0. */
    val freqCounts: Map<String, Int>,
    /** Distinct IAB categories served to the user within the competitive-separation window. */
    val recentCategories: Set<String>,
)
```

- [ ] **Step 2: Write `FakeFrequencyClient.kt`**

```kotlin
package com.github.robran.adserver.auction

/**
 * Phase 1 stub: every user is "fresh" — no caps hit, no recent categories.
 * Configurable via constructor for tests that want to exercise the freq+compsep stage logic
 * without a real Redis-backed service.
 */
class FakeFrequencyClient(
    private val counts: Map<String, Int> = emptyMap(),
    private val recentCategories: Set<String> = emptySet(),
) : FrequencyClient {
    override suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult =
        EnrichResult(freqCounts = counts, recentCategories = recentCategories)
}
```

- [ ] **Step 3: Verify compile**

Run: `./gradlew :ad-server:compileKotlin`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/FrequencyClient.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/auction/FakeFrequencyClient.kt
git commit -m "Phase 1 task 14: FrequencyClient interface + FakeFrequencyClient stub"
```

---

## Task 15: FrequencyAndCompsepStage + Tests

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStage.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStageTest.kt`

Two filters fed by one [FrequencyClient.enrich] call:
- Drop candidates where `freqCount[campaignId] >= campaign.frequencyCap`
- Drop candidates where `campaign.category ∈ recentCategories`

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStageTest.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.FakeFrequencyClient
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class FrequencyAndCompsepStageTest {

    private fun candidate(id: String, category: String, cap: Int = 5): Candidate {
        val campaign = Campaign(
            id = id, advertiserId = "adv", advertiserDomain = "x.example.com",
            category = category, bidPrice = 1.0, frequencyCap = cap, active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private val ctx = AuctionContext(
        request = BidRequest(id = "r", imp = listOf(Imp(id = "1", banner = Banner(300, 250)))),
        userId = "u",
    )

    @Test
    fun `passes through when no caps hit and no recent categories`() = runTest {
        val candidates = listOf(candidate("c1", "IAB1"), candidate("c2", "IAB2"))
        val out = FrequencyAndCompsepStage(FakeFrequencyClient()).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c2")
    }

    @Test
    fun `drops candidates at or above their freq cap`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", cap = 5),    // count 5 → exactly at cap → drop
            candidate("c2", "IAB2", cap = 5),    // count 4 → below cap → keep
            candidate("c3", "IAB3", cap = 3),    // count 0 (missing) → keep
        )
        val client = FakeFrequencyClient(counts = mapOf("c1" to 5, "c2" to 4))
        val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c2", "c3")
    }

    @Test
    fun `drops candidates whose category was recently served`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1"),
            candidate("c2", "IAB2"),
            candidate("c3", "IAB3"),
        )
        val client = FakeFrequencyClient(recentCategories = setOf("IAB2"))
        val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c3")
    }

    @Test
    fun `applies both filters in one pass`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", cap = 3),
            candidate("c2", "IAB1", cap = 5),    // category blocked
            candidate("c3", "IAB2", cap = 5),    // count over cap
            candidate("c4", "IAB3", cap = 5),    // survives
        )
        val client = FakeFrequencyClient(
            counts = mapOf("c3" to 7),
            recentCategories = setOf("IAB1"),
        )
        val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c4"))
    }

    @Test
    fun `empty candidate list short-circuits without calling the client`() = runTest {
        var called = false
        val tracker = object : com.github.robran.adserver.auction.FrequencyClient {
            override suspend fun enrich(
                userId: String,
                campaignIds: List<String>,
            ): com.github.robran.adserver.auction.EnrichResult {
                called = true
                return com.github.robran.adserver.auction.EnrichResult(emptyMap(), emptySet())
            }
        }
        val out = FrequencyAndCompsepStage(tracker).evaluate(ctx, emptyList())
        assertThat(out).isEmpty()
        assertThat(called).isEqualTo(false)
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.FrequencyAndCompsepStageTest"`
Expected: FAIL — `FrequencyAndCompsepStage` does not exist.

- [ ] **Step 3: Write the implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStage.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.FrequencyClient
import com.github.robran.adserver.auction.RuleStage

/**
 * Combined stage: one [FrequencyClient.enrich] call per request returns BOTH per-campaign
 * counts and the user's recent categories. Drop a candidate if either:
 *   - its campaign's count >= cap
 *   - its category is in recentCategories
 *
 * Phase 1 uses [FakeFrequencyClient]; Phase 2 swaps in a gRPC client that talks to frequency-service.
 * Empty input short-circuits — no need to call the client.
 */
class FrequencyAndCompsepStage(private val frequencyClient: FrequencyClient) : RuleStage {
    override suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate> {
        if (candidates.isEmpty()) return emptyList()
        val campaignIds = candidates.map { it.campaign.id }
        val enrich = frequencyClient.enrich(ctx.userId, campaignIds)

        return candidates.filter { c ->
            val count = enrich.freqCounts[c.campaign.id] ?: 0
            count < c.campaign.frequencyCap && c.campaign.category !in enrich.recentCategories
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.FrequencyAndCompsepStageTest"`
Expected: PASS, 5 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStage.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FrequencyAndCompsepStageTest.kt
git commit -m "Phase 1 task 15: FrequencyAndCompsepStage (combined freq cap + competitive separation)"
```

---

## Task 16: FloorPriceStage + Tests

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStage.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStageTest.kt`

Drops candidates whose `bidPrice < imp.bidfloor`. USD only for the demo.

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStageTest.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class FloorPriceStageTest {

    private fun candidate(id: String, bid: Double): Candidate {
        val campaign = Campaign(
            id = id, advertiserId = "adv", advertiserDomain = "x.example.com",
            category = "IAB1", bidPrice = bid, frequencyCap = 5, active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private fun ctx(floor: Double) = AuctionContext(
        request = BidRequest(
            id = "r",
            imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
        ),
        userId = "u",
    )

    @Test
    fun `passes through when floor is zero`() = runTest {
        val candidates = listOf(candidate("c1", 0.5), candidate("c2", 1.0))
        val out = FloorPriceStage().evaluate(ctx(floor = 0.0), candidates)
        assertThat(out).hasSize(2)
    }

    @Test
    fun `drops candidates below the floor`() = runTest {
        val candidates = listOf(
            candidate("c1", 0.5),    // below
            candidate("c2", 1.0),    // exactly at floor → keep
            candidate("c3", 2.0),    // above → keep
        )
        val out = FloorPriceStage().evaluate(ctx(floor = 1.0), candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c2", "c3")
    }

    @Test
    fun `returns empty when no candidate meets the floor`() = runTest {
        val candidates = listOf(candidate("c1", 0.5), candidate("c2", 0.9))
        val out = FloorPriceStage().evaluate(ctx(floor = 1.0), candidates)
        assertThat(out).isEmpty()
    }
}
```

- [ ] **Step 2: Run to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.FloorPriceStageTest"`
Expected: FAIL — `FloorPriceStage` does not exist.

- [ ] **Step 3: Write the implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStage.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.RuleStage

/**
 * Drops candidates whose bid price is strictly below the imp's bid floor.
 * "At or above" wins (i.e., bid >= floor → keep). USD only in this phase.
 */
class FloorPriceStage : RuleStage {
    override suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate> {
        val floor = ctx.imp.bidfloor
        if (floor <= 0.0) return candidates
        return candidates.filter { it.bidPrice >= floor }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.FloorPriceStageTest"`
Expected: PASS, 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStage.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/FloorPriceStageTest.kt
git commit -m "Phase 1 task 16: FloorPriceStage"
```

---

## Task 17: SelectionStage + Tests

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/SelectionStage.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/SelectionStageTest.kt`

The terminal stage. Returns the highest-bid candidate; ties broken with an injectable `Random` (so tests are deterministic).

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/SelectionStageTest.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.random.Random

class SelectionStageTest {

    private fun candidate(id: String, bid: Double): Candidate {
        val campaign = Campaign(
            id = id, advertiserId = "adv", advertiserDomain = "x.example.com",
            category = "IAB1", bidPrice = bid, frequencyCap = 5, active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private val ctx = AuctionContext(
        request = BidRequest(id = "r", imp = listOf(Imp(id = "1", banner = Banner(300, 250)))),
        userId = "u",
    )

    @Test
    fun `picks highest bid`() = runTest {
        val candidates = listOf(candidate("c1", 1.0), candidate("c2", 3.0), candidate("c3", 2.0))
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out).hasSize(1)
        assertThat(out[0].campaign.id).isEqualTo("c2")
    }

    @Test
    fun `breaks ties with the injected Random deterministically`() = runTest {
        val candidates = listOf(candidate("c1", 5.0), candidate("c2", 5.0), candidate("c3", 5.0))
        // Random(42).nextInt(3) == 1 (tested separately; the point is determinism)
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out).hasSize(1)
        // Whichever index Random picks, it must be stable across runs of the test.
        val first = out[0].campaign.id
        val second = SelectionStage(Random(42)).evaluate(ctx, candidates)[0].campaign.id
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun `returns empty when input is empty`() = runTest {
        val out = SelectionStage(Random(42)).evaluate(ctx, emptyList())
        assertThat(out).isEmpty()
    }

    @Test
    fun `returns the only candidate without invoking randomness`() = runTest {
        val candidates = listOf(candidate("c1", 1.0))
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }
}
```

- [ ] **Step 2: Run to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.SelectionStageTest"`
Expected: FAIL — `SelectionStage` does not exist.

- [ ] **Step 3: Write the implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/SelectionStage.kt`:

```kotlin
package com.github.robran.adserver.auction.stages

import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.RuleStage
import kotlin.random.Random

/**
 * Terminal stage: pick a winner. Highest bid wins; ties broken by the injected [Random]
 * (default: shared default — override in tests for determinism).
 *
 * Returns a list of size 0 or 1, never larger.
 */
class SelectionStage(private val random: Random = Random.Default) : RuleStage {
    override suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate> {
        if (candidates.isEmpty()) return emptyList()
        if (candidates.size == 1) return candidates
        val maxBid = candidates.maxOf { it.bidPrice }
        val tied = candidates.filter { it.bidPrice == maxBid }
        return if (tied.size == 1) tied else listOf(tied[random.nextInt(tied.size)])
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.stages.SelectionStageTest"`
Expected: PASS, 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/stages/SelectionStage.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/stages/SelectionStageTest.kt
git commit -m "Phase 1 task 17: SelectionStage with deterministic tie-breaking"
```

---

## Task 18: AuctionPipeline Orchestrator + Tests

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt`
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt`

Wires the four stages together. Exposes `runAuction(ctx) → BidResponse`.

- [ ] **Step 1: Write the failing test**

`ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt`:

```kotlin
package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random

class AuctionPipelineTest {

    private fun campaign(
        id: String,
        category: String = "IAB1",
        domain: String = "x.example.com",
        bid: Double = 1.0,
        cap: Int = 5,
        sizes: List<Pair<Int, Int>> = listOf(300 to 250),
    ): Campaign {
        val creatives = sizes.mapIndexed { i, (w, h) ->
            Creative(id = "$id-cre-$i", campaignId = id, width = w, height = h, markup = "<m>")
        }
        return Campaign(id, "adv-$id", domain, category, bid, cap, true, creatives)
    }

    private fun pipeline(
        snapshot: InventorySnapshot,
        freq: FrequencyClient = FakeFrequencyClient(),
    ) = AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages = listOf(
            BlockingPolicyStage(),
            FrequencyAndCompsepStage(freq),
            FloorPriceStage(),
            SelectionStage(Random(42)),
        ),
    )

    private fun req(
        userId: String = "u",
        floor: Double = 0.0,
        bcat: List<String> = emptyList(),
        badv: List<String> = emptyList(),
    ) = BidRequest(
        id = "req",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
        user = com.github.robran.adserver.protocol.openrtb.User(id = userId),
        bcat = bcat,
        badv = badv,
    )

    @Test
    fun `returns winner when all stages pass`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0), campaign("c2", bid = 4.5)), Instant.now())
        val resp = pipeline(s).runAuction(req())
        assertThat(resp.seatbid).hasSize(1)
        assertThat(resp.seatbid[0].bid[0].cid).isEqualTo("c2")
        assertThat(resp.seatbid[0].bid[0].price).isEqualTo(4.5)
        assertThat(resp.nbr).isNull()
    }

    @Test
    fun `no-fill when blocked categories eliminate everything`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", category = "IAB7-39")), Instant.now())
        val resp = pipeline(s).runAuction(req(bcat = listOf("IAB7-39")))
        assertThat(resp.seatbid).hasSize(0)
        assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
    }

    @Test
    fun `no-fill when frequency cap eliminates everything`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", cap = 5)), Instant.now())
        val resp = pipeline(s, FakeFrequencyClient(counts = mapOf("c1" to 5))).runAuction(req())
        assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
    }

    @Test
    fun `no-fill when floor eliminates everything`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", bid = 0.5)), Instant.now())
        val resp = pipeline(s).runAuction(req(floor = 1.5))
        assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FLOOR)
    }

    @Test
    fun `winner Bid carries creative size and category`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", category = "IAB13", bid = 3.0)), Instant.now())
        val resp = pipeline(s).runAuction(req())
        val bid = resp.seatbid[0].bid[0]
        assertThat(bid.w).isEqualTo(300)
        assertThat(bid.h).isEqualTo(250)
        assertThat(bid.cat).isEqualTo(listOf("IAB13"))
        assertThat(bid.crid).isNotNull()
    }

    @Test
    fun `falls back to user buyeruid then anonymous when user id is null`() = runTest {
        val s = InventorySnapshot(listOf(campaign("c1", bid = 1.0)), Instant.now())
        val rawNoUser = BidRequest(
            id = "r",
            imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
            user = null,
        )
        val resp = pipeline(s).runAuction(rawNoUser)
        // Anonymous still gets to bid in this phase.
        assertThat(resp.seatbid).hasSize(1)
    }
}
```

- [ ] **Step 2: Run to confirm it fails**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTest"`
Expected: FAIL — `AuctionPipeline` does not exist.

- [ ] **Step 3: Write the implementation**

`ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt`:

```kotlin
package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import java.util.UUID

/**
 * Runs the rule pipeline:
 *
 *   candidates(0) → blocking → freq+compsep → floor → selection
 *
 * Stages are passed in declaration order. If a stage returns empty, the auction terminates and
 * the response is no-fill with a stage-specific [NoBidReason] (Phase 1 debug-only — Phase 4 will
 * also emit observability spans). Pure orchestration; no I/O lives here.
 */
class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
) {
    /**
     * Run a full auction. Always returns a [BidResponse], either with a winning seatbid or with
     * `nbr` set to indicate which stage filtered out the last candidate.
     */
    suspend fun runAuction(request: BidRequest): BidResponse {
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)
        if (initial.isEmpty()) {
            return BidResponse(id = request.id, nbr = NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
        }

        var current = initial
        var stageIndex = 0
        for (stage in stages) {
            current = stage.evaluate(ctx, current)
            if (current.isEmpty()) {
                return BidResponse(id = request.id, nbr = noBidReasonFor(stageIndex))
            }
            stageIndex++
        }

        // Selection always returns 0 or 1 candidate. If we got here, current.size == 1.
        val winner = current.single()
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.auction.AuctionPipelineTest"`
Expected: PASS, 6 tests green.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/auction/AuctionPipeline.kt \
        ad-server/src/test/kotlin/com/github/robran/adserver/auction/AuctionPipelineTest.kt
git commit -m "Phase 1 task 18: AuctionPipeline orchestrator with stage-aware no-fill reasons"
```

---

## Task 19: BidRoute (POST /openrtb/bid) + Wire Into Application

**Files:**
- Create: `ad-server/src/main/kotlin/com/github/robran/adserver/http/BidRoute.kt`
- Modify: `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt`

- [ ] **Step 1: Write `BidRoute.kt`**

```kotlin
package com.github.robran.adserver.http

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.protocol.openrtb.BidRequest
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.post

fun Route.bidRoutes(pipeline: AuctionPipeline) {
    post("/openrtb/bid") {
        val req = call.receive<BidRequest>()
        val resp = pipeline.runAuction(req)
        call.respond(HttpStatusCode.OK, resp)
    }
}
```

- [ ] **Step 2: Update `Application.kt` to build the pipeline and wire the bid route**

Replace the current contents of `ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt` with:

```kotlin
package com.github.robran.adserver

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.CandidateBuilder
import com.github.robran.adserver.auction.FakeFrequencyClient
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.http.HealthState
import com.github.robran.adserver.http.bidRoutes
import com.github.robran.adserver.http.healthRoutes
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.random.Random

private val log = LoggerFactory.getLogger("com.github.robran.adserver.Application")

fun main() {
    val config = AppConfig.load()

    if (!config.inventory.skipMigrate) {
        InventoryLoader.migrate(
            config.inventory.jdbcUrl,
            config.inventory.user,
            config.inventory.password,
        )
    }
    val ds = InventoryLoader.pooledDataSource(
        config.inventory.jdbcUrl,
        config.inventory.user,
        config.inventory.password,
    )
    val snapshot = InventoryLoader(ds).load()
    val pipeline = buildPipeline(snapshot)
    log.info("ad-server starting: {} campaigns loaded", snapshot.size)

    val healthState = HealthState().apply { ready.set(true) }

    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, pipeline)
    }.start(wait = true)
}

/**
 * Builds the auction pipeline. Phase 1 uses [FakeFrequencyClient] (always returns empty);
 * Phase 2 swaps it for a real gRPC client.
 */
fun buildPipeline(snapshot: InventorySnapshot): AuctionPipeline =
    AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages = listOf(
            BlockingPolicyStage(),
            FrequencyAndCompsepStage(FakeFrequencyClient()),
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

    routing {
        healthRoutes(healthState)
        bidRoutes(pipeline)
    }
}
```

Compared to Task 10, the `adServerModule` signature changed: instead of `(HealthState, InventorySnapshot)` it now takes `(HealthState, AuctionPipeline)`. The pipeline is constructed once in `main` from the loaded snapshot and held for the process lifetime — no per-request allocation, no `AtomicReference`, no inheritance gymnastics. `CandidateBuilder` stays as a regular `class`.

- [ ] **Step 3: Verify it builds**

Run: `./gradlew :ad-server:build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Run all unit tests to confirm nothing regressed**

Run: `./gradlew :ad-server:test`
Expected: PASS — all stages + pipeline tests still green. (No integration test yet; that's Task 20.)

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/main/kotlin/com/github/robran/adserver/Application.kt \
        ad-server/src/main/kotlin/com/github/robran/adserver/http/BidRoute.kt
git commit -m "Phase 1 task 19: POST /openrtb/bid wired into Application"
```

---

## Task 20: End-to-End Integration Test (Testcontainers Postgres + Ktor)

**Files:**
- Create: `ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt`
- Create: `ad-server/src/test/resources/logback-test.xml`

The defining test of Phase 1: bring up Postgres, seed it, hydrate a snapshot, mount the Ktor app, POST a real BidRequest, assert a winning Bid comes back.

- [ ] **Step 1: Write `logback-test.xml` for ad-server tests**

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
    <logger name="io.netty" level="WARN"/>
</configuration>
```

- [ ] **Step 2: Write the integration test**

`ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt`:

```kotlin
package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThanOrEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.User
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BidRouteIntegrationTest {

    @Container
    val postgres: PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("kotlin_ad_server_test")
        .withUsername("test")
        .withPassword("test")

    private lateinit var pipeline: AuctionPipeline

    @BeforeAll
    fun setup() {
        postgres.start()
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                val snapshot = InventoryLoader(ds).load()
                pipeline = buildPipeline(snapshot)
            }
    }

    @AfterAll
    fun tearDown() {
        postgres.stop()
    }

    private fun banner300x250Request(
        userId: String = "user-zalia",
        floor: Double = 0.0,
        bcat: List<String> = emptyList(),
    ) = BidRequest(
        id = "req-it-${System.nanoTime()}",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
        user = User(id = userId),
        bcat = bcat,
    )

    @Test
    fun `POST openrtb bid returns a winning Bid for a 300x250 request`() = testApplication {
        application {
            adServerModule(HealthState().apply { ready.set(true) }, pipeline)
        }
        val client = createClient {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }
        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request())
        }
        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(1)
        val bid = body.seatbid[0].bid[0]
        assertThat(bid.cid).isNotNull()
        assertThat(bid.crid).isNotNull()
        assertThat(bid.price).isGreaterThanOrEqualTo(0.0)
        assertThat(bid.w).isEqualTo(300)
        assertThat(bid.h).isEqualTo(250)
        assertThat(body.nbr).isNull()
    }

    @Test
    fun `POST openrtb bid honors bcat blocking — empty result when all categories blocked`() = testApplication {
        application {
            adServerModule(HealthState().apply { ready.set(true) }, pipeline)
        }
        val client = createClient {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }
        // Block ALL categories present in the seed data.
        val allCategories = listOf(
            "IAB13", "IAB13-1", "IAB13-2", "IAB13-3", "IAB13-7", "IAB13-9",
            "IAB19", "IAB19-6", "IAB19-15", "IAB19-30",
            "IAB8", "IAB8-1", "IAB8-5", "IAB8-9",
            "IAB2", "IAB2-2", "IAB2-5",
            "IAB20", "IAB20-1", "IAB20-3", "IAB20-26",
            "IAB5", "IAB5-1", "IAB5-3", "IAB5-13",
            "IAB16", "IAB16-1", "IAB16-7",
        )
        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request(bcat = allCategories))
        }
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(0)
        assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
    }

    @Test
    fun `POST openrtb bid honors floor price`() = testApplication {
        application {
            adServerModule(HealthState().apply { ready.set(true) }, pipeline)
        }
        val client = createClient {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }
        // Set floor higher than every campaign's bid (max in seed is 6.10).
        val response = client.post("/openrtb/bid") {
            contentType(ContentType.Application.Json)
            setBody(banner300x250Request(floor = 99.0))
        }
        val body: BidResponse = response.body()
        assertThat(body.seatbid).hasSize(0)
        assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FLOOR)
    }

    @Test
    fun `healthz returns 200`() = testApplication {
        application {
            adServerModule(HealthState().apply { ready.set(true) }, pipeline)
        }
        val response = client.get("/healthz")
        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
    }
}
```

- [ ] **Step 3: Run the integration test**

Run: `./gradlew :ad-server:test --tests "com.github.robran.adserver.http.BidRouteIntegrationTest"`
Expected: PASS, 4 tests green. Requires Docker daemon running.

- [ ] **Step 4: Run the full test suite to make sure nothing else broke**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL across all three modules.

- [ ] **Step 5: Commit**

```bash
git add ad-server/src/test/kotlin/com/github/robran/adserver/http/BidRouteIntegrationTest.kt \
        ad-server/src/test/resources/logback-test.xml
git commit -m "Phase 1 task 20: end-to-end Ktor + Postgres integration test"
```

---

## Task 21: README + Smoke-Test Script

**Files:**
- Modify: `README.md`
- Create: `scripts/smoke-test.sh`

Wraps the phase: gives a future you (or a recruiter) a one-paragraph summary and a single command to run everything end-to-end.

- [ ] **Step 1: Update `README.md`**

```markdown
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
```

- [ ] **Step 2: Write `scripts/smoke-test.sh`**

Run: `mkdir -p scripts`

Write `scripts/smoke-test.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Running full test suite (unit + Testcontainers integration)"
./gradlew test

echo "==> Phase 1 smoke test PASSED. Inventory loaded, BidRequest → BidResponse round-trip verified."
```

Then: `chmod +x scripts/smoke-test.sh`

- [ ] **Step 3: Run smoke test**

Run: `./scripts/smoke-test.sh`
Expected: All tests pass; final line "Phase 1 smoke test PASSED" appears.

- [ ] **Step 4: Commit**

```bash
git add README.md scripts/smoke-test.sh
git commit -m "Phase 1 task 21: README status block + smoke-test script"
```

---

## Phase 1 Done

Working software:
- 3 Gradle modules building cleanly with shared version catalog.
- 50-campaign inventory hydrated from Postgres into an immutable in-memory snapshot at boot.
- Ktor service serving `POST /openrtb/bid` with full 5-stage rule pipeline (blocking, freq+compsep with fake client, floor, selection).
- 25+ unit tests + 4 Testcontainers integration tests, all green.

You can now `curl POST http://localhost:8080/openrtb/bid` with a valid BidRequest JSON and get a winning Bid response.

**Next:** Phase 2 — frequency-service + Redis (gRPC server, Lettuce reactive client, Lua-scripted atomic operations). Will need its own plan.
