package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

/**
 * OpenRTB 2.6 BidResponse. `nbr` (no-bid reason) is set when no winner found.
 */
@Serializable
data class BidResponse(
    // matches BidRequest.id
    val id: String,
    val seatbid: List<SeatBid> = emptyList(),
    val cur: String = "USD",
    // no-bid reason; 0 = unknown error / no fill
    val nbr: Int? = null,
)

@Serializable
data class SeatBid(
    // advertiser/seat id
    val seat: String,
    val bid: List<Bid>,
)

@Serializable
data class Bid(
    // unique bid id
    val id: String,
    // matches Imp.id
    val impid: String,
    // CPM, in BidResponse.cur
    val price: Double,
    // creative id
    val adid: String? = null,
    // win notice URL (out of scope this phase, kept for shape parity)
    val nurl: String? = null,
    // creative markup (passed through from Creative.markup)
    val adm: String? = null,
    // campaign id
    val cid: String? = null,
    // creative id (alternative to adid)
    val crid: String? = null,
    // IAB categories
    val cat: List<String> = emptyList(),
    val w: Int? = null,
    val h: Int? = null,
)

/**
 * Standard no-bid reasons per OpenRTB 2.6.
 */
object NoBidReason {
    const val UNKNOWN_ERROR = 0
    const val TECHNICAL_ERROR = 1
    const val INVALID_REQUEST = 2
    const val KNOWN_WEB_SPIDER = 3
    const val SUSPECTED_NON_HUMAN_TRAFFIC = 4
    const val UNSUPPORTED_DEVICE = 8
    const val BLOCKED_PUBLISHER = 9
    const val UNMATCHED_USER = 10

    // 100+ reserved for exchange-specific. We use 200+ for our stage rejections (debug-only).
    const val NO_MATCHING_CREATIVE = 200
    const val NO_CANDIDATES_AFTER_BLOCKING = 201
    const val NO_CANDIDATES_AFTER_FREQ_COMPSEP = 202
    const val NO_CANDIDATES_AFTER_FLOOR = 203
}
