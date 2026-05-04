# Graph Report - .  (2026-05-04)

## Corpus Check
- Corpus is ~18,340 words - fits in a single context window. You may not need a graph.

## Summary
- 208 nodes · 181 edges · 43 communities detected
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 4 edges (avg confidence: 0.74)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Project Structure & Phases|Project Structure & Phases]]
- [[_COMMUNITY_Phase 2-3 Frequency Service & Kafka|Phase 2-3: Frequency Service & Kafka]]
- [[_COMMUNITY_AuctionPipeline Tests|AuctionPipeline Tests]]
- [[_COMMUNITY_Bid Route Integration Tests|Bid Route Integration Tests]]
- [[_COMMUNITY_Blocking Policy Stage Tests|Blocking Policy Stage Tests]]
- [[_COMMUNITY_Candidate Builder Tests|Candidate Builder Tests]]
- [[_COMMUNITY_Frequency+Compsep Stage Tests|Frequency+Compsep Stage Tests]]
- [[_COMMUNITY_Inventory Loader (Postgres - Snapshot)|Inventory Loader (Postgres -> Snapshot)]]
- [[_COMMUNITY_Floor Price Stage Tests|Floor Price Stage Tests]]
- [[_COMMUNITY_Selection Stage Tests|Selection Stage Tests]]
- [[_COMMUNITY_Inventory Loader Tests|Inventory Loader Tests]]
- [[_COMMUNITY_Observability Stack|Observability Stack]]
- [[_COMMUNITY_OpenRTB Protocol Module|OpenRTB Protocol Module]]
- [[_COMMUNITY_OpenRTB Context DTOs|OpenRTB Context DTOs]]
- [[_COMMUNITY_OpenRTB BidResponse Types|OpenRTB BidResponse Types]]
- [[_COMMUNITY_Application Configuration|Application Configuration]]
- [[_COMMUNITY_Auction Pipeline Orchestrator|Auction Pipeline Orchestrator]]
- [[_COMMUNITY_Seed Data Loader|Seed Data Loader]]
- [[_COMMUNITY_OpenRTB Serialization Tests|OpenRTB Serialization Tests]]
- [[_COMMUNITY_Application Bootstrap|Application Bootstrap]]
- [[_COMMUNITY_Frequency Client Interface|Frequency Client Interface]]
- [[_COMMUNITY_OpenRTB Impression DTOs|OpenRTB Impression DTOs]]
- [[_COMMUNITY_Candidate Builder|Candidate Builder]]
- [[_COMMUNITY_Rule Stage Interface|Rule Stage Interface]]
- [[_COMMUNITY_Fake Frequency Client (Phase 1)|Fake Frequency Client (Phase 1)]]
- [[_COMMUNITY_Blocking Policy Stage|Blocking Policy Stage]]
- [[_COMMUNITY_Selection Stage|Selection Stage]]
- [[_COMMUNITY_Frequency+Compsep Stage|Frequency+Compsep Stage]]
- [[_COMMUNITY_Floor Price Stage|Floor Price Stage]]
- [[_COMMUNITY_Health Endpoints|Health Endpoints]]
- [[_COMMUNITY_Creative Domain Type|Creative Domain Type]]
- [[_COMMUNITY_Inventory Snapshot|Inventory Snapshot]]
- [[_COMMUNITY_OpenRTB BidRequest DTO|OpenRTB BidRequest DTO]]
- [[_COMMUNITY_Auction Candidate|Auction Candidate]]
- [[_COMMUNITY_Auction Context|Auction Context]]
- [[_COMMUNITY_Bid Endpoint Route|Bid Endpoint Route]]
- [[_COMMUNITY_Campaign Domain Type|Campaign Domain Type]]
- [[_COMMUNITY_Module Build File|Module Build File]]
- [[_COMMUNITY_Gradle Settings|Gradle Settings]]
- [[_COMMUNITY_Module Build File|Module Build File]]
- [[_COMMUNITY_Module Build File|Module Build File]]
- [[_COMMUNITY_Module Build File|Module Build File]]
- [[_COMMUNITY_Structured Logging|Structured Logging]]

## God Nodes (most connected - your core abstractions)
1. `AuctionPipelineTest` - 10 edges
2. `BidRouteIntegrationTest` - 9 edges
3. `kotlin_ad_server Project` - 9 edges
4. `BlockingPolicyStageTest` - 8 edges
5. `CandidateBuilderTest` - 7 edges
6. `FrequencyAndCompsepStageTest` - 7 edges
7. `5-Stage Rule Engine Pipeline` - 7 edges
8. `FloorPriceStageTest` - 6 edges
9. `SelectionStageTest` - 6 edges
10. `InventoryLoaderTest` - 6 edges

## Surprising Connections (you probably didn't know these)
- `Phase 1 — Skeleton + Hot Path` --references--> `5-Stage Rule Engine Pipeline`  [INFERRED]
  README.md → docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md
- `ad-server Module` --references--> `5-Stage Rule Engine Pipeline`  [EXTRACTED]
  README.md → docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md
- `Task 1: Gradle Scaffolding` --implements--> `Phase 1 — Skeleton + Hot Path`  [EXTRACTED]
  docs/superpowers/plans/2026-05-04-phase-1-skeleton-and-hot-path.md → README.md
- `Phase 2 — Frequency Service + Redis` --references--> `Frequency Service (gRPC, port 9090)`  [EXTRACTED]
  README.md → docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md
- `Phase 3 — Kafka + Flink Aggregator` --references--> `Kafka Topics (bid-requests, auction-results, impression-events)`  [EXTRACTED]
  README.md → docs/superpowers/specs/2026-05-04-kotlin-ad-server-design.md

## Hyperedges (group relationships)
- **5-Stage Rule Engine Pipeline (all stages)** — spec_stage_blocking, spec_stage_freq_compsep, spec_stage_floor, spec_stage_selection, spec_rule_engine [EXTRACTED 1.00]
- **Observability Stack (Micrometer + OpenTelemetry + Structured Logging + Grafana)** — spec_micrometer_metrics, spec_otel_tracing, spec_structured_logging, spec_grafana_dashboard [EXTRACTED 1.00]
- **Six Build Phases of the Project** — readme_phase1, readme_phase2, readme_phase3, readme_phase4, readme_phase5, readme_phase6 [EXTRACTED 1.00]

## Communities

### Community 0 - "Project Structure & Phases"
Cohesion: 0.13
Nodes (18): Task 1: Gradle Scaffolding, Rationale: Load Inventory Before Serving (healthz blocks on snapshot), kotlin_ad_server Project, ad-server Module, inventory-loader Module, Phase 1 — Skeleton + Hot Path, Phase 5 — Gatling Load Testing, Phase 6 — Polish + Final README (+10 more)

### Community 1 - "Phase 2-3: Frequency Service & Kafka"
Cohesion: 0.14
Nodes (16): Rationale: Combined gRPC for Stages 2+3 (one round-trip), Rationale: Fail-Open (latency wins, freshness loses), Rationale: Flink Aggregator Instead of Direct Redis Writes, Rationale: gRPC over REST/JSON (3-5x cheaper per call at 5K-10K QPS), Phase 2 — Frequency Service + Redis, Phase 3 — Kafka + Flink Aggregator, Avro Schemas (ImpressionEvent, AuctionResult), EnrichForAuction gRPC RPC (+8 more)

### Community 2 - "AuctionPipeline Tests"
Cohesion: 0.18
Nodes (1): AuctionPipelineTest

### Community 3 - "Bid Route Integration Tests"
Cohesion: 0.2
Nodes (1): BidRouteIntegrationTest

### Community 4 - "Blocking Policy Stage Tests"
Cohesion: 0.22
Nodes (1): BlockingPolicyStageTest

### Community 5 - "Candidate Builder Tests"
Cohesion: 0.25
Nodes (1): CandidateBuilderTest

### Community 6 - "Frequency+Compsep Stage Tests"
Cohesion: 0.25
Nodes (1): FrequencyAndCompsepStageTest

### Community 7 - "Inventory Loader (Postgres -> Snapshot)"
Cohesion: 0.25
Nodes (1): InventoryLoader

### Community 8 - "Floor Price Stage Tests"
Cohesion: 0.29
Nodes (1): FloorPriceStageTest

### Community 9 - "Selection Stage Tests"
Cohesion: 0.29
Nodes (1): SelectionStageTest

### Community 10 - "Inventory Loader Tests"
Cohesion: 0.29
Nodes (1): InventoryLoaderTest

### Community 11 - "Observability Stack"
Cohesion: 0.43
Nodes (7): Rationale: Kafka Span as Link, Not Parent (avoid latency distortion), Phase 4 — Observability, AuctionContext (BidRequest + OTel span + MeterRegistry), Grafana Dashboard (provisioned JSON), Kafka Fire-and-Forget Post-Response Emission, Micrometer Metrics (Prometheus Registry), OpenTelemetry Tracing (OTLP → Jaeger)

### Community 12 - "OpenRTB Protocol Module"
Cohesion: 0.4
Nodes (6): Task 2: common-protocol Module Setup, Task 3: OpenRTB BidRequest + Imp + Context Types, Task 4: OpenRTB BidResponse Types, Task 5: Serialization Round-Trip Test (Golden BidRequest), common-protocol Module, OpenRTB 2.6 Subset DTOs

### Community 13 - "OpenRTB Context DTOs"
Cohesion: 0.4
Nodes (4): Device, Geo, Site, User

### Community 14 - "OpenRTB BidResponse Types"
Cohesion: 0.4
Nodes (4): Bid, BidResponse, NoBidReason, SeatBid

### Community 15 - "Application Configuration"
Cohesion: 0.4
Nodes (3): AppConfig, InventoryConfig, ServerConfig

### Community 16 - "Auction Pipeline Orchestrator"
Cohesion: 0.4
Nodes (1): AuctionPipeline

### Community 17 - "Seed Data Loader"
Cohesion: 0.4
Nodes (1): SeedLoader

### Community 18 - "OpenRTB Serialization Tests"
Cohesion: 0.5
Nodes (1): OpenRtbSerializationTest

### Community 19 - "Application Bootstrap"
Cohesion: 0.5
Nodes (0): 

### Community 20 - "Frequency Client Interface"
Cohesion: 0.5
Nodes (2): EnrichResult, FrequencyClient

### Community 21 - "OpenRTB Impression DTOs"
Cohesion: 0.67
Nodes (2): Banner, Imp

### Community 22 - "Candidate Builder"
Cohesion: 0.67
Nodes (1): CandidateBuilder

### Community 23 - "Rule Stage Interface"
Cohesion: 0.67
Nodes (1): RuleStage

### Community 24 - "Fake Frequency Client (Phase 1)"
Cohesion: 0.67
Nodes (1): FakeFrequencyClient

### Community 25 - "Blocking Policy Stage"
Cohesion: 0.67
Nodes (1): BlockingPolicyStage

### Community 26 - "Selection Stage"
Cohesion: 0.67
Nodes (1): SelectionStage

### Community 27 - "Frequency+Compsep Stage"
Cohesion: 0.67
Nodes (1): FrequencyAndCompsepStage

### Community 28 - "Floor Price Stage"
Cohesion: 0.67
Nodes (1): FloorPriceStage

### Community 29 - "Health Endpoints"
Cohesion: 0.67
Nodes (1): HealthState

### Community 30 - "Creative Domain Type"
Cohesion: 0.67
Nodes (1): Creative

### Community 31 - "Inventory Snapshot"
Cohesion: 0.67
Nodes (1): InventorySnapshot

### Community 32 - "OpenRTB BidRequest DTO"
Cohesion: 1.0
Nodes (1): BidRequest

### Community 33 - "Auction Candidate"
Cohesion: 1.0
Nodes (1): Candidate

### Community 34 - "Auction Context"
Cohesion: 1.0
Nodes (1): AuctionContext

### Community 35 - "Bid Endpoint Route"
Cohesion: 1.0
Nodes (0): 

### Community 36 - "Campaign Domain Type"
Cohesion: 1.0
Nodes (1): Campaign

### Community 37 - "Module Build File"
Cohesion: 1.0
Nodes (0): 

### Community 38 - "Gradle Settings"
Cohesion: 1.0
Nodes (0): 

### Community 39 - "Module Build File"
Cohesion: 1.0
Nodes (0): 

### Community 40 - "Module Build File"
Cohesion: 1.0
Nodes (0): 

### Community 41 - "Module Build File"
Cohesion: 1.0
Nodes (0): 

### Community 42 - "Structured Logging"
Cohesion: 1.0
Nodes (1): Structured Logging (Logback + JSON MDC)

## Knowledge Gaps
- **37 isolated node(s):** `Site`, `Device`, `Geo`, `User`, `Imp` (+32 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `OpenRTB BidRequest DTO`** (2 nodes): `BidRequest`, `BidRequest.kt`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Auction Candidate`** (2 nodes): `Candidate.kt`, `Candidate`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Auction Context`** (2 nodes): `AuctionContext.kt`, `AuctionContext`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Bid Endpoint Route`** (2 nodes): `BidRoute.kt`, `bidRoutes()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Campaign Domain Type`** (2 nodes): `Campaign`, `Campaign.kt`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Module Build File`** (1 nodes): `build.gradle.kts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Gradle Settings`** (1 nodes): `settings.gradle.kts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Module Build File`** (1 nodes): `build.gradle.kts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Module Build File`** (1 nodes): `build.gradle.kts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Module Build File`** (1 nodes): `build.gradle.kts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Structured Logging`** (1 nodes): `Structured Logging (Logback + JSON MDC)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `kotlin_ad_server Project` connect `Project Structure & Phases` to `Phase 2-3: Frequency Service & Kafka`, `Observability Stack`, `OpenRTB Protocol Module`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **Why does `5-Stage Rule Engine Pipeline` connect `Project Structure & Phases` to `Phase 2-3: Frequency Service & Kafka`, `Observability Stack`?**
  _High betweenness centrality (0.012) - this node is a cross-community bridge._
- **Why does `common-protocol Module` connect `OpenRTB Protocol Module` to `Project Structure & Phases`?**
  _High betweenness centrality (0.010) - this node is a cross-community bridge._
- **What connects `Site`, `Device`, `Geo` to the rest of the system?**
  _37 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Project Structure & Phases` be split into smaller, more focused modules?**
  _Cohesion score 0.13 - nodes in this community are weakly interconnected._
- **Should `Phase 2-3: Frequency Service & Kafka` be split into smaller, more focused modules?**
  _Cohesion score 0.14 - nodes in this community are weakly interconnected._