package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

/**
 * OpenRTB 2.6 BidResponse. `nbr` (no-bid reason) is set when no winner found.
 */
@Serializable
data class BidResponse(
    val id: String,                       // matches BidRequest.id
    val seatbid: List<SeatBid> = emptyList(),
    val cur: String = "USD",
    val nbr: Int? = null,                 // no-bid reason; 0 = unknown error / no fill
)

@Serializable
data class SeatBid(
    val seat: String,                     // advertiser/seat id
    val bid: List<Bid>,
)

@Serializable
data class Bid(
    val id: String,                       // unique bid id
    val impid: String,                    // matches Imp.id
    val price: Double,                    // CPM, in BidResponse.cur
    val adid: String? = null,             // creative id
    val nurl: String? = null,             // win notice URL (out of scope this phase, kept for shape parity)
    val adm: String? = null,              // creative markup (out of scope; demo returns null)
    val cid: String? = null,              // campaign id
    val crid: String? = null,             // creative id (alternative to adid)
    val cat: List<String> = emptyList(),  // IAB categories
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
    const val NO_CANDIDATES_AFTER_BLOCKING = 200
    const val NO_CANDIDATES_AFTER_FREQ_COMPSEP = 201
    const val NO_CANDIDATES_AFTER_FLOOR = 202
}
