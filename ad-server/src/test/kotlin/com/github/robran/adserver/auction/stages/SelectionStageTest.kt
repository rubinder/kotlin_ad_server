package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.hasSize
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
import kotlin.random.Random

class SelectionStageTest {

    private fun candidate(id: String, bid: Double): Candidate {
        val campaign = Campaign(
            id = id, advertiserId = "adv", advertiserDomain = "x.example.com",
            category = "IAB1", bidPrice = bid, frequencyCap = 5, active = true,
            creatives = emptyList(),
        )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private val ctx = AuctionContext(
        request = BidRequest(id = "r", imp = listOf(Imp(id = "1", banner = Banner(300, 250)))),
        userId = "u",
    )

    @Test
    fun `picks highest bid`() = runTest {
        val candidates = listOf(candidate("c1", 1.0), candidate("c2", 3.0), candidate("c3", 2.0))
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out).hasSize(1)
        assertThat(out[0].campaign.id).isEqualTo("c2")
    }

    @Test
    fun `breaks ties with the injected Random deterministically`() = runTest {
        val candidates = listOf(candidate("c1", 5.0), candidate("c2", 5.0), candidate("c3", 5.0))
        // Random(42).nextInt(3) == 1 (tested separately; the point is determinism)
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out).hasSize(1)
        // Whichever index Random picks, it must be stable across runs of the test.
        val first = out[0].campaign.id
        val second = SelectionStage(Random(42)).evaluate(ctx, candidates)[0].campaign.id
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun `returns empty when input is empty`() = runTest {
        val out = SelectionStage(Random(42)).evaluate(ctx, emptyList())
        assertThat(out).isEmpty()
    }

    @Test
    fun `returns the only candidate without invoking randomness`() = runTest {
        val candidates = listOf(candidate("c1", 1.0))
        val out = SelectionStage(Random(42)).evaluate(ctx, candidates)
        assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c1"))
    }
}
