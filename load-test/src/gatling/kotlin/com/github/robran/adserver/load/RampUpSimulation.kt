package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.rampUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * RampUp: 0 → target QPS over rampSeconds, then steady at target for steadySeconds.
 * Defaults: 5,000 QPS, 5 min ramp, 5 min steady.
 */
class RampUpSimulation : Simulation() {

    private val targetRps: Int = System.getenv("RAMP_TARGET_RPS")?.toInt() ?: 5_000
    private val rampSec: Long = System.getenv("RAMP_DURATION_SECONDS")?.toLong() ?: 300L
    private val steadySec: Long = System.getenv("RAMP_STEADY_SECONDS")?.toLong() ?: 300L

    private val scn = scenario("RampUp")
        .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                rampUsersPerSec(0.0).to(targetRps.toDouble()).during(Duration.ofSeconds(rampSec)),
                constantUsersPerSec(targetRps.toDouble()).during(Duration.ofSeconds(steadySec)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
