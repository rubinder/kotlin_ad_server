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
    override suspend fun evaluate(
        ctx: AuctionContext,
        candidates: List<Candidate>,
    ): List<Candidate> {
        if (candidates.isEmpty()) return emptyList()
        if (candidates.size == 1) return candidates
        val maxBid = candidates.maxOf { it.bidPrice }
        val tied = candidates.filter { it.bidPrice == maxBid }
        return if (tied.size == 1) tied else listOf(tied[random.nextInt(tied.size)])
    }
}
