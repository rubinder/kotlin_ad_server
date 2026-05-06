package com.github.robran.adserver.inventory

data class Creative(
    val id: String,
    val campaignId: String,
    val width: Int,
    val height: Int,
    // demo: opaque string
    val markup: String,
)

/** Helper: does this creative match the requested banner size? */
fun Creative.matches(
    bannerW: Int,
    bannerH: Int,
): Boolean = width == bannerW && height == bannerH
