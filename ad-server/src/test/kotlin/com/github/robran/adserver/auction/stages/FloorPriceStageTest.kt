package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class FloorPriceStageTest {
    private fun candidate(
        id: String,
        bid: Double,
    ): Candidate {
        val campaign =
            Campaign(
                id = id,
                advertiserId = "adv",
                advertiserDomain = "x.example.com",
                category = "IAB1",
                bidPrice = bid,
                frequencyCap = 5,
                active = true,
                creatives = emptyList(),
            )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private fun ctx(floor: Double) =
        AuctionContext(
            request =
                BidRequest(
                    id = "r",
                    imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
                ),
            userId = "u",
        )

    @Test
    fun `passes through when floor is zero`() =
        runTest {
            val candidates = listOf(candidate("c1", 0.5), candidate("c2", 1.0))
            val out = FloorPriceStage().evaluate(ctx(floor = 0.0), candidates)
            assertThat(out).hasSize(2)
        }

    @Test
    fun `drops candidates below the floor`() =
        runTest {
            val candidates =
                listOf(
                    candidate("c1", 0.5), // below
                    candidate("c2", 1.0), // exactly at floor → keep
                    candidate("c3", 2.0), // above → keep
                )
            val out = FloorPriceStage().evaluate(ctx(floor = 1.0), candidates)
            assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c2", "c3")
        }

    @Test
    fun `returns empty when no candidate meets the floor`() =
        runTest {
            val candidates = listOf(candidate("c1", 0.5), candidate("c2", 0.9))
            val out = FloorPriceStage().evaluate(ctx(floor = 1.0), candidates)
            assertThat(out).isEmpty()
        }
}
