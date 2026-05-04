package com.github.robran.adserver.auction

import com.github.robran.adserver.protocol.openrtb.BidRequest

/**
 * Per-request immutable context. Carries the inbound request, the resolved user id, and
 * the imp under auction (Phase 1 supports single-imp only).
 *
 * Future extensions (later phases): MeterRegistry, OpenTelemetry Span, gRPC client handles.
 */
data class AuctionContext(
    val request: BidRequest,
    val userId: String,
    val impIndex: Int = 0,
) {
    val imp get() = request.imp[impIndex]
}
