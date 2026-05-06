package com.github.robran.adserver.load

import io.gatling.javaapi.core.ChainBuilder
import io.gatling.javaapi.core.CoreDsl.StringBody
import io.gatling.javaapi.core.CoreDsl.csv
import io.gatling.javaapi.core.CoreDsl.exec
import io.gatling.javaapi.core.CoreDsl.feed
import io.gatling.javaapi.core.FeederBuilder
import io.gatling.javaapi.http.HttpDsl
import io.gatling.javaapi.http.HttpDsl.http
import io.gatling.javaapi.http.HttpProtocolBuilder
import java.io.File

/**
 * Shared Gatling primitives for kotlin_ad_server load tests:
 *  - HTTP protocol pointing at the local ad-server (override via env var ADSERVER_BASE_URL)
 *  - Feeder reading the workload-generator output
 *  - A single chain step `bidRequest` that POSTs /openrtb/bid with feeder values substituted in
 */
object BidProtocol {
    private val baseUrl: String = System.getenv("ADSERVER_BASE_URL") ?: "http://localhost:8080"

    val httpProtocol: HttpProtocolBuilder =
        http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json")
            .userAgentHeader("kotlin_ad_server-load/1.0")

    val feeder: FeederBuilder<*> by lazy {
        val csvPath =
            System.getenv("LOAD_FEEDER_CSV")
                ?: "load-test/build/feeders/users.csv"
        val file = File(csvPath)
        require(file.exists()) {
            "Feeder CSV not found at $csvPath. Run WorkloadGenerator first."
        }
        csv(file.absolutePath).circular()
    }

    /** One bid request: feeder gives us user_id + banner size + slot. Body is built per request. */
    val bidRequest: ChainBuilder =
        feed(feeder).exec(
            http("POST /openrtb/bid")
                .post("/openrtb/bid")
                .body(
                    StringBody { session ->
                        val uid = session.getString("user_id")
                        val w = session.getInt("banner_w")
                        val h = session.getInt("banner_h")
                        val slot = session.getString("slot_id")
                        val nano = System.nanoTime()
                        """{"id":"req-$nano",""" +
                            """"imp":[{"id":"1","banner":{"w":$w,"h":$h},"tagid":"$slot"}],""" +
                            """"user":{"id":"$uid"}}"""
                    },
                )
                .check(HttpDsl.status().shouldBe(200)),
        )
}
