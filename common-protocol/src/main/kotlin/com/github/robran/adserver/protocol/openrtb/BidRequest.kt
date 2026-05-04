package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

/**
 * OpenRTB 2.6 BidRequest — subset for kotlin_ad_server: single-imp, banner only.
 * Field names match the OpenRTB 2.6 spec (lowercased) so the schema is recognizable.
 */
@Serializable
data class BidRequest(
    val id: String,
    val imp: List<Imp>,
    val site: Site? = null,
    val device: Device? = null,
    val user: User? = null,
    val tmax: Int? = null,            // max auction time in ms
    val cur: List<String> = listOf("USD"),
    val bcat: List<String> = emptyList(),  // blocked IAB categories
    val badv: List<String> = emptyList(),  // blocked advertiser domains
    val bapp: List<String> = emptyList(),  // blocked app bundle IDs
)
