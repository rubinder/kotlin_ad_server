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
    override suspend fun evaluate(
        ctx: AuctionContext,
        candidates: List<Candidate>,
    ): List<Candidate> {
        if (candidates.isEmpty()) return emptyList()
        val campaignIds = candidates.map { it.campaign.id }
        val enrich = frequencyClient.enrich(ctx.userId, campaignIds)

        return candidates.filter { c ->
            val count = enrich.freqCounts[c.campaign.id] ?: 0
            count < c.campaign.frequencyCap && c.campaign.category !in enrich.recentCategories
        }
    }
}
