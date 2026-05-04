package com.github.robran.adserver.auction.stages

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.AuctionContext
import com.github.robran.adserver.auction.Candidate
import com.github.robran.adserver.auction.EnrichResult
import com.github.robran.adserver.auction.FakeFrequencyClient
import com.github.robran.adserver.auction.FrequencyClient
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class FrequencyAndCompsepStageTest {
    private fun candidate(
        id: String,
        category: String,
        cap: Int = 5,
    ): Candidate {
        val campaign =
            Campaign(
                id = id,
                advertiserId = "adv",
                advertiserDomain = "x.example.com",
                category = category,
                bidPrice = 1.0,
                frequencyCap = cap,
                active = true,
                creatives = emptyList(),
            )
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "")
        return Candidate(campaign, creative)
    }

    private val ctx =
        AuctionContext(
            request = BidRequest(id = "r", imp = listOf(Imp(id = "1", banner = Banner(300, 250)))),
            userId = "u",
        )

    @Test
    fun `passes through when no caps hit and no recent categories`() =
        runTest {
            val candidates = listOf(candidate("c1", "IAB1"), candidate("c2", "IAB2"))
            val out = FrequencyAndCompsepStage(FakeFrequencyClient()).evaluate(ctx, candidates)
            assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c2")
        }

    @Test
    fun `drops candidates at or above their freq cap`() =
        runTest {
            val candidates =
                listOf(
                    // count 5 → exactly at cap → drop
                    candidate("c1", "IAB1", cap = 5),
                    // count 4 → below cap → keep
                    candidate("c2", "IAB2", cap = 5),
                    // count 0 (missing) → keep
                    candidate("c3", "IAB3", cap = 3),
                )
            val client = FakeFrequencyClient(counts = mapOf("c1" to 5, "c2" to 4))
            val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
            assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c2", "c3")
        }

    @Test
    fun `drops candidates whose category was recently served`() =
        runTest {
            val candidates =
                listOf(
                    candidate("c1", "IAB1"),
                    candidate("c2", "IAB2"),
                    candidate("c3", "IAB3"),
                )
            val client = FakeFrequencyClient(recentCategories = setOf("IAB2"))
            val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
            assertThat(out.map { it.campaign.id }).containsExactlyInAnyOrder("c1", "c3")
        }

    @Test
    fun `applies both filters in one pass`() =
        runTest {
            val candidates =
                listOf(
                    candidate("c1", "IAB1", cap = 3),
                    // category blocked
                    candidate("c2", "IAB1", cap = 5),
                    // count over cap
                    candidate("c3", "IAB2", cap = 5),
                    // survives
                    candidate("c4", "IAB3", cap = 5),
                )
            val client =
                FakeFrequencyClient(
                    counts = mapOf("c3" to 7),
                    recentCategories = setOf("IAB1"),
                )
            val out = FrequencyAndCompsepStage(client).evaluate(ctx, candidates)
            assertThat(out.map { it.campaign.id }).isEqualTo(listOf("c4"))
        }

    @Test
    fun `empty candidate list short-circuits without calling the client`() =
        runTest {
            var called = false
            val tracker =
                object : FrequencyClient {
                    override suspend fun enrich(
                        userId: String,
                        campaignIds: List<String>,
                    ): EnrichResult {
                        called = true
                        return EnrichResult(emptyMap(), emptySet())
                    }
                }
            val out = FrequencyAndCompsepStage(tracker).evaluate(ctx, emptyList())
            assertThat(out).isEmpty()
            assertThat(called).isEqualTo(false)
        }
}
