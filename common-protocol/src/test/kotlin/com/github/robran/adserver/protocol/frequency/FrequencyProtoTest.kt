package com.github.robran.adserver.protocol.frequency

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class FrequencyProtoTest {
    @Test
    fun `EnrichRequest builder accepts user_id and campaign_ids`() {
        val req =
            EnrichRequest.newBuilder()
                .setUserId("user-1")
                .addCampaignIds("camp-001")
                .addCampaignIds("camp-002")
                .build()

        assertThat(req.userId).isEqualTo("user-1")
        assertThat(req.campaignIdsCount).isEqualTo(2)
        assertThat(req.getCampaignIds(0)).isEqualTo("camp-001")
    }

    @Test
    fun `EnrichResponse builder accepts freq_counts and recent_categories`() {
        val resp =
            EnrichResponse.newBuilder()
                .putFreqCounts("camp-001", 5)
                .putFreqCounts("camp-002", 3)
                .addRecentCategories("IAB1")
                .addRecentCategories("IAB13-1")
                .build()

        assertThat(resp.freqCountsMap["camp-001"]).isEqualTo(5)
        assertThat(resp.recentCategoriesCount).isEqualTo(2)
    }
}
