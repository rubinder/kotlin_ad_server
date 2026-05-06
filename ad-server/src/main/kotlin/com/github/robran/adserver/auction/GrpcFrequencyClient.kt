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

    override suspend fun enrich(
        userId: String,
        campaignIds: List<String>,
    ): EnrichResult {
        val request =
            EnrichRequest.newBuilder()
                .setUserId(userId)
                .addAllCampaignIds(campaignIds)
                .build()
        var nanos = 0L
        return try {
            var resp: com.github.robran.adserver.protocol.frequency.EnrichResponse
            nanos =
                measureNanoTime {
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

    private fun newTimer(
        registry: MeterRegistry,
        outcome: String,
    ): Timer =
        Timer.builder(METRIC_NAME)
            .tag("outcome", outcome)
            .publishPercentileHistogram()
            .register(registry)

    companion object {
        const val METRIC_NAME = "frequency.grpc.duration"
    }
}
