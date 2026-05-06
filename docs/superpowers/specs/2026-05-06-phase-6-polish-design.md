# Phase 6 — Polish (README + Mermaid Diagrams + GitHub Actions CI)

**Goal:** Wrap the kotlin_ad_server portfolio project with a recruiter-ready README narrative, four Mermaid architecture diagrams, and a CI badge backed by a GitHub Actions workflow that runs the full test + lint suite.

**Date:** 2026-05-06

**Status:** Spec — pending implementation plan.

---

## 1. Scope

**In:**

1. README top-to-bottom rewrite for narrative flow (overview → architecture → deep dives → run → ops → CI → testing).
2. Four Mermaid diagrams embedded in the README:
   - Architecture (services + datastores + Kafka)
   - Rule engine (5-stage pipeline)
   - Kafka topology (topics + producers + consumers)
   - Request lifecycle (sequence diagram showing trace propagation)
3. GitHub Actions CI workflow — single job running `./gradlew check` (test + ktlint) on push and pull_request, with Gradle dep caching, wrapper validation, and JUnit/ktlint report artifacts on failure.
4. CI badge near the top of the README.
5. Status block updated: Phase 6 ✅, no further "next phase" line.

**Explicitly out of scope:**

- Jaeger/Grafana screenshots (operator capture, can land separately on `main`).
- Final test-coverage edge-case pass (low ROI for a portfolio repo).
- `docs/load-test/baseline.md` and `after.md` SVGs (Phase 5 P5-9/P5-11 operator runs, deferred).
- Any new functional code or test code.

## 2. README Structure

Sections in order:

1. **Title + tagline + CI badge** — one-line pitch.
2. **Architecture** — Mermaid diagram #1 + a 3–5 sentence paragraph describing the service topology.
3. **Modules** — bulleted list, tightened from current README.
4. **Rule engine** — Mermaid diagram #2 + per-stage prose with `file:line` pointers.
5. **Kafka topology** — Mermaid diagram #3 + topic table (name, key, value type, producer, consumer, retention).
6. **Request lifecycle** — Mermaid diagram #4 (sequenceDiagram) showing one bid request hitting the ad-server, calling frequency-service over gRPC, producing two Kafka events, and the Flink job consuming `impression-events` later.
7. **Run locally** — existing `docker compose up -d` + `kafka-init-topics.sh` + 3 service `./gradlew :*:run` + sample `curl`. Light edits only.
8. **Observability** — existing metrics table, tracing section, structured logging section. Light edits only.
9. **Load testing** — existing Phase 5 narrative (Gatling scenarios + Dispatchers.IO bottleneck story). Light edits only.
10. **CI** — one paragraph: "Every push and PR runs `./gradlew check` on Ubuntu 22.04 with JDK 21. Failed runs upload JUnit + ktlint reports as artifacts."
11. **Testing locally** — `./gradlew test`, `./scripts/smoke-test.sh`. Light edits only.

The existing "Status" block (Phase 1–5 ✅, Phase 6 ⏳) is removed — the CI badge replaces the implicit "this works" signal.

## 3. Mermaid Diagrams — Specs

Each diagram lives in a fenced ` ```mermaid ` block in the README. GitHub renders these natively.

### 3.1 Architecture

`flowchart` (top-down). Three service boxes (ad-server, frequency-service, flink-impression-aggregator), three datastores (Postgres, Redis, Kafka), three observability sinks (Prometheus, Grafana, Jaeger). Arrows labeled by protocol (`HTTP`, `gRPC`, `Kafka`, `JDBC`, `RESP`, `OTLP`). Subgraph boxes group `Services`, `Datastores`, `Observability`.

### 3.2 Rule Engine

`flowchart LR`. Five nodes: `Blocking` → `Frequency + Compsep` → `Floor` → `Selection` → `BidResponse`. The `Frequency + Compsep` stage has a side-branch labeled `enrichForAuction (gRPC, 8ms timeout, fail-open)` to a `frequency-service` node, illustrating the only network call in the hot path. A small `BidRequest` node feeds the start.

### 3.3 Kafka Topology

`flowchart LR`. Two topics (`auction-results`, `impression-events`) as central nodes. ad-server produces to both. flink-impression-aggregator consumes `impression-events` and writes back to Redis. Each arrow is labeled with Avro schema name (`AuctionResult`, `ImpressionEvent`). A note clarifies that `auction-results` is currently produce-only (no consumer in the demo).

### 3.4 Request Lifecycle

`sequenceDiagram`. Participants: `Client`, `ad-server`, `frequency-service`, `Redis`, `Kafka`. Steps:

1. `Client → ad-server: POST /openrtb/bid (BidRequest)`
2. `ad-server → ad-server: blocking + frequency stages start`
3. `ad-server → frequency-service: enrichForAuction (gRPC, traceparent header)`
4. `frequency-service → Redis: MGET freq:* + ZRANGE winhistory:*`
5. `Redis → frequency-service: counters + categories`
6. `frequency-service → ad-server: EnrichResponse`
7. `ad-server → ad-server: floor + selection stages`
8. `ad-server → Kafka (async): produce AuctionResult`
9. `ad-server → Client: BidResponse`
10. `ad-server → Kafka (async): produce ImpressionEvent`

A note at the top calls out that the W3C `traceparent` header propagates through gRPC, so all spans land in one Jaeger trace.

## 4. GitHub Actions CI Workflow

**File:** `.github/workflows/ci.yml`

**Triggers:** `push` on any branch, `pull_request` against any branch.

**Concurrency:** `group: ${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: true`. New pushes to a branch cancel any in-flight run on the same ref.

**Job:** `build` on `ubuntu-latest`.

**Steps:**

1. `actions/checkout@v4`
2. `gradle/actions/wrapper-validation@v4` — verifies `gradle/wrapper/gradle-wrapper.jar` against the known-good checksum list.
3. `actions/setup-java@v4` with `distribution: temurin`, `java-version: 21`.
4. `gradle/actions/setup-gradle@v4` — provides Gradle dependency cache + build cache, no manual `actions/cache` needed.
5. `./gradlew check` — runs `test` + `ktlintCheck` across every module. Exits non-zero on any failure. (No `--no-daemon` flag — `setup-gradle@v4` manages daemon lifecycle.)
6. `actions/upload-artifact@v4` (with `if: failure()`) uploading `**/build/reports/tests/**` and `**/build/reports/ktlint/**` as `test-and-lint-reports` so failures are debuggable from the Actions UI.

**Expected wall-clock:** 6–10 min cold cache, 3–5 min warm. Ubuntu runners ship Docker, so Testcontainers Postgres / Redis / Kafka / Schema Registry start in-CI without extra setup.

**Risks tracked, not pre-mitigated:**

- KRaft-mode Kafka container can be flaky in CI. If it flakes more than once in the first 10 runs, add `gradle-retry-plugin` scoped to `:flink-impression-aggregator:test` and `:ad-server:test`. Not adding preemptively — a flaky-test-retry layer that hides real problems is worse than the occasional rerun.
- `flink-streaming-java` test utilities pull a large dependency tree. The Gradle cache should keep this under control after the first run.

**Badge:** `[![CI](https://github.com/rubinder/kotlin_ad_server/actions/workflows/ci.yml/badge.svg)](https://github.com/rubinder/kotlin_ad_server/actions/workflows/ci.yml)` placed under the H1 title.

## 5. Verification

- **Mermaid renders:** each diagram is pasted into [mermaid.live](https://mermaid.live) before commit; if it renders cleanly there, GitHub will render it. No automated lint.
- **README link integrity:** `grep -E 'docs/[a-z/-]+\.(md|svg)'` after rewrite; broken relative links fixed inline.
- **Smoke test:** `./scripts/smoke-test.sh` continues to pass (touched only to bump the success banner if needed; otherwise unchanged).
- **CI workflow:** validated by the first push triggering it. If the first run fails, fix forward in the same PR.

No new test code. No new application code.

## 6. Branch + Delivery Strategy

- New branch `f6` cut from `main` after the Phase 5 PR merges.
- One commit per task (per the implementation plan).
- Push `f6` to origin, open PR, user merges.
- Post-merge: `main` carries the CI badge; the badge build is the final live demo of the project's quality bar.

## 7. Subagent Strategy

- **Mermaid diagrams** → foreground (judgment + iteration on diagram readability).
- **README rewrite** → Sonnet subagent (multi-section coordination, light judgment).
- **CI workflow YAML** → Haiku subagent (mechanical scaffolding from the spec).
- **Final review** → spec compliance + code quality reviewers per established lighter-cadence policy: full review for the README rewrite, manual verify for the CI YAML.

## 8. Done Criteria

- [ ] README renders cleanly on GitHub with all 4 Mermaid diagrams visible.
- [ ] CI badge is green on `main` after the first post-merge run.
- [ ] `./scripts/smoke-test.sh` still passes locally.
- [ ] No production source files modified (only `README.md`, new `.github/workflows/ci.yml`, possibly `scripts/smoke-test.sh` banner).

## 9. Out of Scope

- Operator screenshots (Jaeger, Grafana).
- `docs/load-test/baseline.md`, `docs/load-test/after.md`, flame graph SVGs.
- Edge-case test additions.
- Branch protection rules / required-checks configuration on the GitHub repo settings (UI-only, not in this spec).
- Any change to application code, build files, or test code.
