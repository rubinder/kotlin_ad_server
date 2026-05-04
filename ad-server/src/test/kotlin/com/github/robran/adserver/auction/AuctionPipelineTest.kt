package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random

class AuctionPipelineTest {
    private fun campaign(
        id: String,
        category: String = "IAB1",
        domain: String = "x.example.com",
        bid: Double = 1.0,
        cap: Int = 5,
        sizes: List<Pair<Int, Int>> = listOf(300 to 250),
    ): Campaign {
        val creatives =
            sizes.mapIndexed { i, (w, h) ->
                Creative(id = "$id-cre-$i", campaignId = id, width = w, height = h, markup = "<m>")
            }
        return Campaign(id, "adv-$id", domain, category, bid, cap, true, creatives)
    }

    private fun pipeline(
        snapshot: InventorySnapshot,
        freq: FrequencyClient = FakeFrequencyClient(),
    ) = AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages =
            listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(freq),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
    )

    private fun req(
        userId: String = "u",
        floor: Double = 0.0,
        bcat: List<String> = emptyList(),
        badv: List<String> = emptyList(),
    ) = BidRequest(
        id = "req",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
        user = com.github.robran.adserver.protocol.openrtb.User(id = userId),
        bcat = bcat,
        badv = badv,
    )

    @Test
    fun `returns winner when all stages pass`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0), campaign("c2", bid = 4.5)), Instant.now())
            val resp = pipeline(s).runAuction(req())
            assertThat(resp.seatbid).hasSize(1)
            assertThat(resp.seatbid[0].bid[0].cid).isEqualTo("c2")
            assertThat(resp.seatbid[0].bid[0].price).isEqualTo(4.5)
            assertThat(resp.nbr).isNull()
        }

    @Test
    fun `no-fill when blocked categories eliminate everything`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", category = "IAB7-39")), Instant.now())
            val resp = pipeline(s).runAuction(req(bcat = listOf("IAB7-39")))
            assertThat(resp.seatbid).hasSize(0)
            assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
        }

    @Test
    fun `no-fill when frequency cap eliminates everything`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", cap = 5)), Instant.now())
            val resp = pipeline(s, FakeFrequencyClient(counts = mapOf("c1" to 5))).runAuction(req())
            assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
        }

    @Test
    fun `no-fill when floor eliminates everything`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", bid = 0.5)), Instant.now())
            val resp = pipeline(s).runAuction(req(floor = 1.5))
            assertThat(resp.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FLOOR)
        }

    @Test
    fun `winner Bid carries creative size and category`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", category = "IAB13", bid = 3.0)), Instant.now())
            val resp = pipeline(s).runAuction(req())
            val bid = resp.seatbid[0].bid[0]
            assertThat(bid.w).isEqualTo(300)
            assertThat(bid.h).isEqualTo(250)
            assertThat(bid.cat).isEqualTo(listOf("IAB13"))
            assertThat(bid.crid).isNotNull()
        }

    @Test
    fun `falls back to user buyeruid then anonymous when user id is null`() =
        runTest {
            val s = InventorySnapshot(listOf(campaign("c1", bid = 1.0)), Instant.now())
            val rawNoUser =
                BidRequest(
                    id = "r",
                    imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
                    user = null,
                )
            val resp = pipeline(s).runAuction(rawNoUser)
            // Anonymous still gets to bid in this phase.
            assertThat(resp.seatbid).hasSize(1)
        }
}
