package com.github.robran.adserver.inventory

/**
 * A served-side campaign — distinct from an advertiser. One campaign owns many creatives.
 * Held in the in-memory snapshot; never read from Postgres on the request path.
 */
data class Campaign(
    val id: String,
    val advertiserId: String,
    val advertiserDomain: String,
    val category: String,             // primary IAB category, e.g., "IAB3"
    val bidPrice: Double,             // CPM the campaign will pay if it wins, USD
    val frequencyCap: Int,            // per-user cap over the cap window (24h)
    val active: Boolean,
    val creatives: List<Creative>,
)
