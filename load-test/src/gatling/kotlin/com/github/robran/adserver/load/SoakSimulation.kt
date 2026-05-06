package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * Soak: 3K QPS for 30 minutes. Slow-burn problems (memory leaks, connection exhaustion,
 * thread pool growth, descriptor exhaustion) surface here.
 */
class SoakSimulation : Simulation() {

    private val rps: Int = System.getenv("SOAK_RPS")?.toInt() ?: 3_000
    private val durationMin: Long = System.getenv("SOAK_DURATION_MINUTES")?.toLong() ?: 30L

    private val scn = scenario("Soak")
        .exec(BidProtocol.bidRequest)

    init {
        setUp(
            scn.injectOpen(
                constantUsersPerSec(rps.toDouble()).during(Duration.ofMinutes(durationMin)),
            ),
        ).protocols(BidProtocol.httpProtocol)
    }
}
