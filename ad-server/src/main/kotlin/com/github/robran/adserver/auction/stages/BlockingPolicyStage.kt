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
