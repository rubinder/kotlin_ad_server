package com.github.robran.adserver.protocol.events

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class AvroSchemaTest {
    @Test
    fun `ImpressionEvent builder round-trips a record`() {
        val event =
            ImpressionEvent.newBuilder()
                .setUserId("user-1")
                .setCampaignId("camp-001")
                .setCreativeId("cre-001a")
                .setCategory("IAB13")
                .setPrice(2.50)
                .setTsMillis(1_700_000_000_000L)
                .build()

        assertThat(event.userId.toString()).isEqualTo("user-1")
        assertThat(event.campaignId.toString()).isEqualTo("camp-001")
        assertThat(event.tsMillis).isEqualTo(1_700_000_000_000L)
    }

    @Test
    fun `AuctionResultEvent supports nullable winner fields for no-fill`() {
        val event =
            AuctionResultEvent.newBuilder()
                .setRequestId("req-1")
                .setUserId("user-1")
                .setImpId("1")
                .setTsMillis(1L)
                .setOutcome(Outcome.NO_FILL_FLOOR)
                .setCandidatesInitial(5)
                .setCandidatesAfterBlocking(5)
                .setCandidatesAfterFreqCompsep(3)
                .setCandidatesAfterFloor(0)
                .build()

        assertThat(event.outcome).isEqualTo(Outcome.NO_FILL_FLOOR)
        assertThat(event.winnerCampaignId == null).isEqualTo(true)
    }
}
