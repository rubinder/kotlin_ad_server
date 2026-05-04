package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.ManagedChannel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
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
            val response = withContext(Dispatchers.IO) {
                withTimeout(timeoutMs) { stub.enrichForAuction(request) }
            }
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
