package com.github.robran.adserver.auction

/**
 * One step in the auction rule pipeline. Each stage reads the input candidate list and returns
 * the surviving subset (possibly empty). Stages are run in declaration order:
 *
 *   blocking → frequency+compsep → floor → selection
 *
 * `suspend` because Phase 2 introduces a gRPC call inside [stages.FrequencyAndCompsepStage].
 * Phase 1 stages are CPU-only but signed `suspend` for forward compatibility.
 */
fun interface RuleStage {
    suspend fun evaluate(ctx: AuctionContext, candidates: List<Candidate>): List<Candidate>
}
