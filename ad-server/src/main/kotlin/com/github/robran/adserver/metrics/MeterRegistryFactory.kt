package com.github.robran.adserver.metrics

import com.github.robran.adserver.MetricsConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

/**
 * Builds the process-wide Prometheus registry, attaches JVM/process binders, and applies common
 * tags so every emitted metric is labeled by service+env without each call site repeating itself.
 */
object MeterRegistryFactory {
    fun build(config: MetricsConfig): PrometheusMeterRegistry {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        config.commonTags.forEach { (k, v) -> registry.config().commonTags(k, v) }
        bindJvmMetrics(registry)
        return registry
    }

    private fun bindJvmMetrics(registry: MeterRegistry) {
        ClassLoaderMetrics().bindTo(registry)
        JvmMemoryMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        JvmThreadMetrics().bindTo(registry)
        ProcessorMetrics().bindTo(registry)
        UptimeMetrics().bindTo(registry)
    }
}
