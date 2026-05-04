package com.github.robran.adserver.auction

/**
 * Single combined call: returns per-campaign impression counts AND the IAB categories
 * recently served to this user, in one round-trip. Phase 2 swaps the implementation for a
 * gRPC client; Phase 1 uses [FakeFrequencyClient]. Suspending so the real impl can be async.
 */
interface FrequencyClient {
    suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult
}

data class EnrichResult(
    /** Map of campaignId → current count over the cap window. Missing campaign means count=0. */
    val freqCounts: Map<String, Int>,
    /** Distinct IAB categories served to the user within the competitive-separation window. */
    val recentCategories: Set<String>,
)
