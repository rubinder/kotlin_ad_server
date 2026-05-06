package com.github.robran.adserver.auction

import com.github.robran.adserver.kafka.EventEmitter
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.metrics.PipelineMetrics
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import com.github.robran.adserver.protocol.openrtb.Bid
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.SeatBid
import io.micrometer.core.instrument.MeterRegistry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import java.util.UUID
import kotlin.system.measureNanoTime

class AuctionPipeline(
    private val candidateBuilder: CandidateBuilder,
    private val stages: List<RuleStage>,
    private val eventEmitter: EventEmitter = NoOpEventEmitter,
    private val clock: () -> Long = { System.currentTimeMillis() },
    meterRegistry: MeterRegistry = PipelineMetrics.defaultRegistry(),
    openTelemetry: OpenTelemetry = OpenTelemetry.noop(),
) {
    private val metrics = PipelineMetrics(meterRegistry)
    private val tracer: Tracer = openTelemetry.getTracer("com.github.robran.adserver")

    private val stageNames = listOf("blocking", "freq+compsep", "floor", "selection")

    suspend fun runAuction(request: BidRequest): BidResponse {
        require(request.imp.isNotEmpty()) { "BidRequest.imp must contain at least one impression" }
        val imp = request.imp[0]
        val rootSpan =
            tracer.spanBuilder("adserver.request")
                .setAttribute("user.id", resolveUserId(request))
                .setAttribute("imp.id", imp.id)
                .setAttribute("slot.size", "${imp.banner.w}x${imp.banner.h}")
                .setAttribute("request.id", request.id)
                .startSpan()
        val rootScope = rootSpan.makeCurrent()
        var outcomeTag = "no-fill"
        return try {
            val response: BidResponse
            val totalNanos: Long =
                run {
                    var inner: BidResponse
                    val nanos =
                        measureNanoTime {
                            inner = runAuctionInner(request, rootSpan)
                            outcomeTag = if (inner.seatbid.isNotEmpty()) "filled" else "no-fill"
                        }
                    response = inner
                    nanos
                }
            metrics.requestTimer(outcomeTag).record(totalNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
            rootSpan.setAttribute("outcome", outcomeTag)
            response
        } catch (t: Throwable) {
            metrics.requestTimer("error").record(0, java.util.concurrent.TimeUnit.NANOSECONDS)
            rootSpan.setStatus(StatusCode.ERROR, t.message ?: t.javaClass.simpleName)
            rootSpan.recordException(t)
            throw t
        } finally {
            rootScope.close()
            rootSpan.end()
        }
    }

    private suspend fun runAuctionInner(
        request: BidRequest,
        rootSpan: Span,
    ): BidResponse {
        val ctx = AuctionContext(request = request, userId = resolveUserId(request))
        val initial = candidateBuilder.build(ctx)
        metrics.candidatesSurvivingSummary("initial").record(initial.size.toDouble())
        rootSpan.setAttribute("candidates.initial", initial.size.toLong())

        val sizes = IntArray(4)
        sizes[0] = initial.size

        if (initial.isEmpty()) {
            emitOutcome(request, ctx, sizes, Outcome.NO_FILL_BLOCKING, winner = null)
            return BidResponse(id = request.id, nbr = NoBidReason.NO_MATCHING_CREATIVE)
        }

        var current = initial
        for ((idx, stage) in stages.withIndex()) {
            val stageName = stageNames.getOrElse(idx) { "stage-$idx" }
            val stageSpan =
                tracer.spanBuilder("rule.$stageName")
                    .setParent(Context.current())
                    .setAttribute("candidates.in", current.size.toLong())
                    .startSpan()
            val stageScope = stageSpan.makeCurrent()
            val newCurrent: List<Candidate>
            val stageNanos: Long =
                try {
                    measureNanoTime { newCurrent = stage.evaluate(ctx, current) }
                } catch (t: Throwable) {
                    stageSpan.setStatus(StatusCode.ERROR, t.message ?: t.javaClass.simpleName)
                    stageSpan.recordException(t)
                    stageScope.close()
                    stageSpan.end()
                    throw t
                }
            current = newCurrent
            stageSpan.setAttribute("candidates.out", current.size.toLong())
            stageScope.close()
            stageSpan.end()
            metrics.stageTimer(stageName).record(stageNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
            metrics.candidatesSurvivingSummary(stageName).record(current.size.toDouble())
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
        rootSpan.setAttribute("winner.campaign_id", winner.campaign.id)
        rootSpan.setAttribute("winner.bid", winner.bidPrice)
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
