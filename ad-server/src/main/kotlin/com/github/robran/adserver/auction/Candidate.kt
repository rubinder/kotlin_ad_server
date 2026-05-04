package com.github.robran.adserver.auction

import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative

/**
 * One eligible (campaign, creative) pair for an impression. The auction works on a list of these.
 * Filtered down by each [RuleStage]; the [SelectionStage] picks one (or none).
 */
data class Candidate(
    val campaign: Campaign,
    val creative: Creative,
    val bidPrice: Double = campaign.bidPrice,
)
