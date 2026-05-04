package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class BlockingPolicyStageTest {

    private fun candidate(id: String, category: String, advertiserDomain: String): Candidate {
        val campaign = Campaign(
            id = id,
            advertiserId = "adv-$id",
            advertiserDomain = advertiserDomain,
            category = category,
            bidPrice = 1.0,
            frequencyCap = 5,
            active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private fun ctx(bcat: List<String> = emptyList(), badv: List<String> = emptyList()) =
        AuctionContext(
            request = BidRequest(
                id = "r",
                imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
                bcat = bcat,
                badv = badv,
            ),
            userId = "u",
        )

    @Test
    fun `passes through when no blocks specified`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "ok.example.com"),
            candidate("c2", "IAB2", "fine.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(), candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c2")
    }

    @Test
    fun `drops candidates whose category is in bcat`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "a.example.com"),
            candidate("c2", "IAB7-39", "b.example.com"),
            candidate("c3", "IAB3", "c.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB7-39")), candidates)
        assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c3")
    }

    @Test
    fun `drops candidates whose advertiser domain is in badv`() = runTest {
        val candidates = listOf(
            candidate("c1", "IAB1", "good.example.com"),
            candidate("c2", "IAB2", "blocked.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(badv = listOf("blocked.example.com")), candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }

    @Test
    fun `bcat matching is exact, not prefix`() = runTest {
        // IAB13 must NOT block IAB13-1 in this implementation. OpenRTB allows either; we choose exact.
        // (Documented: this is a design choice for the spec subset.)
        val candidates = listOf(
            candidate("c1", "IAB13-1", "a.example.com"),
        )
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB13")), candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }

    @Test
    fun `returns empty when all candidates are blocked`() = runTest {
        val candidates = listOf(candidate("c1", "IAB1", "x.example.com"))
        val out = BlockingPolicyStage().evaluate(ctx(bcat = listOf("IAB1")), candidates)
        assertThat(out).isEmpty()
    }
}
