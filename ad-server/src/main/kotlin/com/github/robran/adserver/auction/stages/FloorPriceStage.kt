package com.github.robran.adserver.auction.stages

import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.RuleStage

/**
 * Drops candidates whose bid price is strictly below the imp's bid floor.
 * "At or above" wins (i.e., bid >= floor → keep). USD only in this phase.
 */
class FloorPriceStage : RuleStage {
    override suspend fun evaluate(
        ctx: AuctionContext,
        candidates: List<Candidate>,
    ): List<Candidate> {
        val floor = ctx.imp.bidfloor
        if (floor <= 0.0) return candidates
        return candidates.filter { it.bidPrice >= floor }
    }
}
