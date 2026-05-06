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

        val redisSpan =
            tracer.spanBuilder("redis.enrich")
                .setAttribute("user.id", userId)
                .setAttribute("campaign_ids.count", campaignIds.size.toLong())
                .startSpan()
        val redisScope = redisSpan.makeCurrent()
        return try {
            val freqCounts =
                if (campaignIds.isEmpty()) {
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
            val nanos =
                measureNanoTime {
                    rawWins = redis.zrangeByScore(winhistoryKey, 0.0, Double.POSITIVE_INFINITY)
                }
            zrangeTimer.record(nanos, TimeUnit.NANOSECONDS)
            val recentCategories =
                rawWins.mapNotNullTo(mutableSetOf()) { entry ->
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

        private fun lookupTimer(
            registry: MeterRegistry,
            op: String,
        ): Timer =
            Timer.builder(METRIC_NAME)
                .tag("op", op)
                .publishPercentileHistogram()
                .register(registry)
    }
}
