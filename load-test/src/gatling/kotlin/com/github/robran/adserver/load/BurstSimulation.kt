package com.github.robran.adserver.load

import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.OpenInjectionStep
import io.gatling.javaapi.core.Simulation
import java.time.Duration

/**
 * Burst: 1K baseline interleaved with 5K spikes (30s baseline, 30s spike, 3 cycles).
 * Tail latency under load transition is the signal we want to surface.
 */
class BurstSimulation : Simulation() {
    private val baseRps: Int = System.getenv("BURST_BASE_RPS")?.toInt() ?: 1_000
    private val spikeRps: Int = System.getenv("BURST_SPIKE_RPS")?.toInt() ?: 5_000
    private val cycles: Int = System.getenv("BURST_CYCLES")?.toInt() ?: 3
    private val phaseSec: Long = System.getenv("BURST_PHASE_SECONDS")?.toLong() ?: 30L

    private val scn =
        scenario("Burst")
            .exec(BidProtocol.bidRequest)

    init {
        val steps = mutableListOf<OpenInjectionStep>()
        repeat(cycles) {
            steps += constantUsersPerSec(baseRps.toDouble()).during(Duration.ofSeconds(phaseSec))
            steps += constantUsersPerSec(spikeRps.toDouble()).during(Duration.ofSeconds(phaseSec))
        }
        setUp(
            scn.injectOpen(*steps.toTypedArray()),
        ).protocols(BidProtocol.httpProtocol)
    }
}
