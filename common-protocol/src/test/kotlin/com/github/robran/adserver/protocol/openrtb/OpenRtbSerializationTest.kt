package com.github.robran.adserver.protocol.openrtb

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

class OpenRtbSerializationTest {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = false
        explicitNulls = false
    }

    @Test
    fun `parses golden BidRequest banner fixture`() {
        val raw = this::class.java.getResource("/golden/bidrequest-banner.json")!!.readText()
        val req = json.decodeFromString<BidRequest>(raw)

        assertThat(req.id).isEqualTo("req-001")
        assertThat(req.imp).hasSize(1)
        assertThat(req.imp[0].banner.w).isEqualTo(300)
        assertThat(req.imp[0].banner.h).isEqualTo(250)
        assertThat(req.imp[0].bidfloor).isEqualTo(1.5)
        assertThat(req.user?.id).isEqualTo("user-zalia")
        assertThat(req.bcat).isEqualTo(listOf("IAB7-39"))
    }

    @Test
    fun `BidResponse round-trips through JSON`() {
        val resp = BidResponse(
            id = "req-001",
            seatbid = listOf(
                SeatBid(
                    seat = "advertiser-7",
                    bid = listOf(
                        Bid(
                            id = "bid-1",
                            impid = "1",
                            price = 2.50,
                            cid = "campaign-9",
                            crid = "creative-3",
                            cat = listOf("IAB3"),
                            w = 300,
                            h = 250,
                        ),
                    ),
                ),
            ),
            cur = "USD",
        )

        val encoded = json.encodeToString(BidResponse.serializer(), resp)
        val decoded = json.decodeFromString<BidResponse>(encoded)

        assertThat(decoded).isEqualTo(resp)
    }
}
