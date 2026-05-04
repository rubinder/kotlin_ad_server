package com.github.robran.adserver.protocol.openrtb

import kotlinx.serialization.Serializable

@Serializable
data class Site(
    val id: String,
    val domain: String? = null,
    val cat: List<String> = emptyList(),
    val page: String? = null,
)

@Serializable
data class Device(
    val ua: String? = null,
    val ip: String? = null,
    val devicetype: Int? = null,
    val os: String? = null,
    val geo: Geo? = null,
)

@Serializable
data class Geo(
    val country: String? = null,
    val region: String? = null,
    val city: String? = null,
)

@Serializable
data class User(
    val id: String? = null,
    val buyeruid: String? = null,
    val yob: Int? = null,
    val gender: String? = null,
)
