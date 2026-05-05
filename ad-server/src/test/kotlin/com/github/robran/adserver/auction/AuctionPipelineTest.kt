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
import com.github.robran.adserver.kafka.NoOpEventEmitter
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

    @Test
    fun `emits AuctionResultEvent for both filled and no-fill outcomes`() =
        runTest {
            val recordingEmitter =
                object : com.github.robran.adserver.kafka.EventEmitter {
                    val results = mutableListOf<com.github.robran.adserver.protocol.events.AuctionResultEvent>()
                    val impressions = mutableListOf<com.github.robran.adserver.protocol.events.ImpressionEvent>()

                    override fun emitAuctionResult(event: com.github.robran.adserver.protocol.events.AuctionResultEvent) {
                        results += event
                    }

                    override fun emitImpression(event: com.github.robran.adserver.protocol.events.ImpressionEvent) {
                        impressions += event
                    }
                }

            val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0)), Instant.now())
            val pipe =
                AuctionPipeline(
                    candidateBuilder = CandidateBuilder(s),
                    stages =
                        listOf(
                            BlockingPolicyStage(),
                            FrequencyAndCompsepStage(FakeFrequencyClient()),
                            FloorPriceStage(),
                            SelectionStage(Random(42)),
                        ),
                    eventEmitter = recordingEmitter,
                    clock = { 12345L },
                )

            pipe.runAuction(req())

            assertThat(recordingEmitter.results).hasSize(1)
            assertThat(recordingEmitter.results[0].outcome).isEqualTo(com.github.robran.adserver.protocol.events.Outcome.FILLED)
            assertThat(recordingEmitter.results[0].tsMillis).isEqualTo(12345L)
            assertThat(recordingEmitter.impressions).hasSize(1)
            assertThat(recordingEmitter.impressions[0].campaignId.toString()).isEqualTo("c1")

            // No-fill case (categories blocked)
            recordingEmitter.results.clear()
            recordingEmitter.impressions.clear()
            val pipeBlocked =
                AuctionPipeline(
                    candidateBuilder = CandidateBuilder(s),
                    stages =
                        listOf(
                            BlockingPolicyStage(),
                            FrequencyAndCompsepStage(FakeFrequencyClient()),
                            FloorPriceStage(),
                            SelectionStage(Random(42)),
                        ),
                    eventEmitter = recordingEmitter,
                    clock = { 99L },
                )
            pipeBlocked.runAuction(req(bcat = listOf("IAB1")))
            assertThat(recordingEmitter.results).hasSize(1)
            assertThat(recordingEmitter.results[0].outcome)
                .isEqualTo(com.github.robran.adserver.protocol.events.Outcome.NO_FILL_BLOCKING)
            assertThat(recordingEmitter.impressions).hasSize(0)
        }

    @Test
    fun `records request total + per-stage timers + candidates surviving`() = runTest {
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val s = InventorySnapshot(listOf(campaign("c1", bid = 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            meterRegistry = registry,
        )
        pipe.runAuction(req())

        val requestTimer = registry.timer("adserver.request.duration", "outcome", "filled")
        assertThat(requestTimer.count()).isEqualTo(1L)

        for (stage in listOf("blocking", "freq+compsep", "floor", "selection")) {
            val t = registry.timer("adserver.stage.duration", "stage", stage)
            assertThat(t.count()).isEqualTo(1L)
        }

        val survivingInitial = registry.summary("adserver.candidates.surviving", "stage", "initial")
        assertThat(survivingInitial.count()).isEqualTo(1L)
    }
}
