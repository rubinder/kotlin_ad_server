package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import java.util.UUID

/**
 * Runs the rule pipeline:
 *
 *   candidates(0) → blocking → freq+compsep → floor → selection
 *
 * Stages are passed in declaration order. If a stage returns empty, the auction terminates and
 * the response is no-fill with a stage-specific [NoBidReason] (Phase 1 debug-only — Phase 4 will
 * also emit observability spans). Pure orchestration; no I/O lives here.
 */
class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
) {
    /**
     * Run a full auction. Always returns a [BidResponse], either with a winning seatbid or with
     * `nbr` set to indicate which stage filtered out the last candidate.
     */
    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)
        if (initial.isEmpty()) {
            return BidResponse(id = request.id, nbr = NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
        }

        var current = initial
        var stageIndex = 0
        for (stage in stages) {
            current = stage.evaluate(ctx, current)
            if (current.isEmpty()) {
                return BidResponse(id = request.id, nbr = noBidReasonFor(stageIndex))
            }
            stageIndex++
        }

        // Selection always returns 0 or 1 candidate. If we got here, current.size == 1.
        val winner = current.single()
        return BidResponse(
            id = request.id,
            seatbid =
                listOf(
                    SeatBid(
                        seat = winner.campaign.advertiserId,
                        bid =
                            listOf(
                                Bid(
                                    id = UUID.randomUUID().toString(),
                                    impid = ctx.imp.id,
                                    price = winner.bidPrice,
                                    cid = winner.campaign.id,
                                    crid = winner.creative.id,
                                    adid = winner.creative.id,
                                    cat = listOf(winner.campaign.category),
                                    w = winner.creative.width,
                                    h = winner.creative.height,
                                    adm = winner.creative.markup,
                                ),
                            ),
                    ),
                ),
        )
    }

    private fun resolveUserId(request: BidRequest): String =
        request.user?.id
            ?: request.user?.buyeruid
            ?: "anonymous"

    private fun noBidReasonFor(stageIndex: Int): Int =
        when (stageIndex) {
            0 -> NoBidReason.NO_CANDIDATES_AFTER_BLOCKING
            1 -> NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP
            2 -> NoBidReason.NO_CANDIDATES_AFTER_FLOOR
            else -> NoBidReason.UNKNOWN_ERROR
        }
}
