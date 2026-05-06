package com.github.robran.adserver.inventory

/**
 * A served-side campaign — distinct from an advertiser. One campaign owns many creatives.
 * Held in the in-memory snapshot; never read from Postgres on the request path.
 */
data class Campaign(
    val id: String,
    val advertiserId: String,
    val advertiserDomain: String,
    // primary IAB category, e.g., "IAB3"
    val category: String,
    // CPM the campaign will pay if it wins, USD
    val bidPrice: Double,
    // per-user cap over the cap window (24h)
    val frequencyCap: Int,
    val active: Boolean,
    val creatives: List<Creative>,
)
