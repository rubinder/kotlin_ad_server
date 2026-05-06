package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

@Serializable
data class Imp(
    val id: String,
    val banner: Banner,
    val bidfloor: Double = 0.0,
    val bidfloorcur: String = "USD",
    // ad slot identifier on the publisher side
    val tagid: String? = null,
    // 1 if HTTPS required
    val secure: Int = 0,
)

@Serializable
data class Banner(
    val w: Int,
    val h: Int,
    val pos: Int? = null,
)
