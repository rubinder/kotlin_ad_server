package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import org.junit.jupiter.api.Test
import java.time.Instant

class CandidateBuilderTest {
    private fun campaign(
        id: String,
        active: Boolean = true,
        vararg sizes: Pair<Int, Int>,
    ): Campaign {
        val creatives =
            sizes.mapIndexed { i, (w, h) ->
                Creative(id = "$id-cre-$i", campaignId = id, width = w, height = h, markup = "<m>")
            }
        return Campaign(
            id = id,
            advertiserId = "adv-x",
            advertiserDomain = "x.example.com",
            category = "IAB1",
            bidPrice = 1.0,
            frequencyCap = 5,
            active = active,
            creatives = creatives,
        )
    }

    private fun snapshot(vararg campaigns: Campaign) = InventorySnapshot(campaigns.toList(), Instant.now())

    private fun request(
        w: Int,
        h: Int,
    ) = BidRequest(
        id = "req",
        imp = listOf(Imp(id = "1", banner = Banner(w = w, h = h))),
        user = null,
    )

    @Test
    fun `includes only creatives whose size matches the imp banner`() {
        val s =
            snapshot(
                campaign("c1", true, 300 to 250, 728 to 90),
                campaign("c2", true, 300 to 250),
                campaign("c3", true, 160 to 600),
            )
        val ctx = AuctionContext(request = request(300, 250), userId = "u")
        val candidates = CandidateBuilder(s).build(ctx)

        val ids = candidates.map { it.creative.id }
        assertThat(ids).containsExactlyInAnyOrder("c1-cre-0", "c2-cre-0")
    }

    @Test
    fun `excludes inactive campaigns`() {
        val s =
            snapshot(
                campaign("c1", active = false, 300 to 250),
                campaign("c2", active = true, 300 to 250),
            )
        val candidates =
            CandidateBuilder(s).build(
                AuctionContext(request = request(300, 250), userId = "u"),
            )
        assertThat(candidates).hasSize(1)
    }

    @Test
    fun `returns empty when no creative matches`() {
        val s = snapshot(campaign("c1", true, 300 to 250))
        val candidates =
            CandidateBuilder(s).build(
                AuctionContext(request = request(728, 90), userId = "u"),
            )
        assertThat(candidates).isEmpty()
    }
}
