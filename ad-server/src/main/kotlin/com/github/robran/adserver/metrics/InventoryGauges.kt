package com.github.robran.adserver.metrics

import com.github.robran.adserver.inventory.InventorySnapshot
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * Gauges over the current inventory snapshot. The snapshot reference is held in an
 * [AtomicReference] so future hot-reload (Phase 5/6) can swap it in atomically.
 */
class InventoryGauges(snapshot: InventorySnapshot, registry: MeterRegistry) {
    private val snapshotRef = AtomicReference(snapshot)

    init {
        Gauge.builder("inventory.snapshot.size") { snapshotRef.get().size.toDouble() }
            .description("Number of campaigns currently in the in-memory inventory snapshot")
            .register(registry)

        Gauge.builder("inventory.snapshot.age_seconds") {
            val loadedAt = snapshotRef.get().loadedAt
            (Instant.now().epochSecond - loadedAt.epochSecond).toDouble().coerceAtLeast(0.0)
        }
            .description("Seconds since the inventory snapshot was last hydrated")
            .register(registry)
    }

    /** Swap in a freshly hydrated snapshot. Future hot-reload feature. */
    fun update(snapshot: InventorySnapshot) {
        snapshotRef.set(snapshot)
    }
}
