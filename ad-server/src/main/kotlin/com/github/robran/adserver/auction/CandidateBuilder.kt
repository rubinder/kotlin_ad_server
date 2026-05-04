package com.github.robran.adserver.auction

import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.inventory.matches

/**
 * Builds the initial candidate list by matching the imp's banner size against each campaign's creatives.
 * A campaign contributes one [Candidate] per creative that matches the requested size; campaigns
 * with no matching creative are excluded entirely. No I/O, pure function over the snapshot.
 */
class CandidateBuilder(private val snapshot: InventorySnapshot) {
    fun build(ctx: AuctionContext): List<Candidate> {
        val w = ctx.imp.banner.w
        val h = ctx.imp.banner.h
        return buildList {
            for (campaign in snapshot.activeCampaigns()) {
                for (creative in campaign.creatives) {
                    if (creative.matches(w, h)) {
                        add(Candidate(campaign, creative))
                    }
                }
            }
        }
    }
}
