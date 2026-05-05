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
