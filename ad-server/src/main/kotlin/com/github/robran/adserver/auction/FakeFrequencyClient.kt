package com.github.robran.adserver.auction

/**
 * Phase 1 stub: every user is "fresh" — no caps hit, no recent categories.
 * Configurable via constructor for tests that want to exercise the freq+compsep stage logic
 * without a real Redis-backed service.
 */
class FakeFrequencyClient(
    private val counts: Map<String, Int> = emptyMap(),
    private val recentCategories: Set<String> = emptySet(),
) : FrequencyClient {
    override suspend fun enrich(userId: String, campaignIds: List<String>): EnrichResult =
        EnrichResult(freqCounts = counts, recentCategories = recentCategories)
}
