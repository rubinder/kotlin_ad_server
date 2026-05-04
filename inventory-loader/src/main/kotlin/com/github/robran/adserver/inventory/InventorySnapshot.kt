package com.github.robran.adserver.inventory

import java.time.Instant

/**
 * Immutable snapshot of all active campaigns + creatives. Hydrated at boot from Postgres
 * by [InventoryLoader] and held in memory for the lifetime of the process. Reload is a future
 * concern (admin endpoint). The request path never touches the DB — only this snapshot.
 */
class InventorySnapshot(
    val campaigns: List<Campaign>,
    val loadedAt: Instant,
) {
    /** Index for fast lookup; built once at construction. */
    val byCampaignId: Map<String, Campaign> = campaigns.associateBy { it.id }

    val size: Int get() = campaigns.size

    fun activeCampaigns(): Sequence<Campaign> = campaigns.asSequence().filter { it.active }
}
