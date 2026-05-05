package com.github.robran.adserver.auction

import com.github.robran.adserver.kafka.EventEmitter
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import java.util.UUID

class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
    private val eventEmitter: EventEmitter = NoOpEventEmitter,
    private val clock: () -> Long = { System.currentTimeMillis() },
) {
    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)

        val sizes = IntArray(4) // [initial, post-blocking, post-freq+compsep, post-floor]
        sizes[0] = initial.size

        if (initial.isEmpty()) {
            emitOutcome(request, ctx, sizes, Outcome.NO_FILL_BLOCKING, winner = null)
            return BidResponse(id = request.id, nbr = NoBidReason.NO_MATCHING_CREATIVE)
        }

        var current = initial
        for ((idx, stage) in stages.withIndex()) {
            current = stage.evaluate(ctx, current)
            if (idx + 1 < sizes.size) sizes[idx + 1] = current.size
            if (current.isEmpty()) {
                val outcome =
                    when (idx) {
                        0 -> Outcome.NO_FILL_BLOCKING
                        1 -> Outcome.NO_FILL_FREQ_COMPSEP
                        2 -> Outcome.NO_FILL_FLOOR
                        else -> Outcome.NO_FILL_OTHER
                    }
                emitOutcome(request, ctx, sizes, outcome, winner = null)
                return BidResponse(id = request.id, nbr = noBidReasonFor(idx))
            }
        }

        val winner = current.single()
        emitOutcome(request, ctx, sizes, Outcome.FILLED, winner = winner)
        emitImpression(ctx, winner)

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

    private fun emitOutcome(
        request: BidRequest,
        ctx: AuctionContext,
        sizes: IntArray,
        outcome: Outcome,
        winner: Candidate?,
    ) {
        val event =
            AuctionResultEvent.newBuilder()
                .setRequestId(request.id)
                .setUserId(ctx.userId)
                .setImpId(ctx.imp.id)
                .setTsMillis(clock())
                .setOutcome(outcome)
                .setWinnerCampaignId(winner?.campaign?.id)
                .setWinnerPrice(winner?.bidPrice)
                .setCandidatesInitial(sizes[0])
                .setCandidatesAfterBlocking(sizes[1])
                .setCandidatesAfterFreqCompsep(sizes[2])
                .setCandidatesAfterFloor(sizes[3])
                .build()
        eventEmitter.emitAuctionResult(event)
    }

    private fun emitImpression(
        ctx: AuctionContext,
        winner: Candidate,
    ) {
        val event =
            ImpressionEvent.newBuilder()
                .setUserId(ctx.userId)
                .setCampaignId(winner.campaign.id)
                .setCreativeId(winner.creative.id)
                .setCategory(winner.campaign.category)
                .setPrice(winner.bidPrice)
                .setTsMillis(clock())
                .build()
        eventEmitter.emitImpression(event)
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
