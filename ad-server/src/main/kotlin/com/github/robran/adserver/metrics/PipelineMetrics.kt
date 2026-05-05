package com.github.robran.adserver.metrics

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

/**
 * Owns the metric names + handles for the auction pipeline. Constructed once at boot from the
 * process registry, reused for every request. Tag values are bounded:
 *
 *   adserver.request.duration : outcome ∈ {filled, no-fill, error}
 *   adserver.stage.duration   : stage   ∈ {blocking, freq+compsep, floor, selection}
 *   adserver.candidates.surviving : stage ∈ same as above + "initial"
 *
 * For tests / no-op contexts, [defaultRegistry] returns a SimpleMeterRegistry — counters still
 * accumulate but nothing is exposed.
 */
class PipelineMetrics(private val registry: MeterRegistry) {
    fun requestTimer(outcome: String): Timer =
        Timer.builder(REQUEST_DURATION)
            .tag("outcome", outcome)
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry)

    fun stageTimer(stage: String): Timer =
        Timer.builder(STAGE_DURATION)
            .tag("stage", stage)
            .publishPercentileHistogram()
            .register(registry)

    fun candidatesSurvivingSummary(stage: String): DistributionSummary =
        DistributionSummary.builder(CANDIDATES_SURVIVING)
            .tag("stage", stage)
            .register(registry)

    companion object {
        const val REQUEST_DURATION = "adserver.request.duration"
        const val STAGE_DURATION = "adserver.stage.duration"
        const val CANDIDATES_SURVIVING = "adserver.candidates.surviving"

        /** Default no-op registry for tests that don't care about metrics. */
        fun defaultRegistry(): MeterRegistry = SimpleMeterRegistry()
    }
}
