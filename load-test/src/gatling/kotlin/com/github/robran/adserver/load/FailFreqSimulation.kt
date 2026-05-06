package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * FailFreq: steady 5K QPS for 90 seconds. Operator kills frequency-service at the 30s mark
 * and restarts it at the 60s mark. The point is to verify fail-open: ad-server should keep
 * responding 200 OK with empty seatbid, not 500.
 */
class FailFreqSimulation : Simulation() {
    private val rps: Int = System.getenv("FAILFREQ_RPS")?.toInt() ?: 5_000
    private val durationSec: Long = System.getenv("FAILFREQ_DURATION_SECONDS")?.toLong() ?: 90L

    private val scn =
        scenario("FailFreq")
            .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                constantUsersPerSec(rps.toDouble()).during(Duration.ofSeconds(durationSec)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
