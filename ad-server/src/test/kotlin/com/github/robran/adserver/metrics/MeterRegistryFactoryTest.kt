package com.github.robran.adserver.metrics

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import com.github.robran.adserver.MetricsConfig
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.junit.jupiter.api.Test

class MeterRegistryFactoryTest {

    @Test
    fun `registry has JVM and process binders attached`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, commonTags = mapOf("service" to "ad-server")),
        )
        val names = registry.meters.map { it.id.name }.toSet()
        assertThat(names).contains("jvm.memory.used")
        assertThat(names).contains("jvm.threads.live")
        assertThat(names).contains("system.cpu.count")
        registry.close()
    }

    @Test
    fun `commonTags are applied to every metric`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(
                enabled = true,
                commonTags = mapOf("service" to "ad-server", "env" to "test"),
            ),
        )
        val sample = registry.meters.first()
        val tagKeys = sample.id.tags.map { it.key }
        assertThat(tagKeys).contains("service")
        assertThat(tagKeys).contains("env")
        assertThat(sample.id.getTag("service")).isEqualTo("ad-server")
        registry.close()
    }

    @Test
    fun `scrape returns Prometheus-formatted text`() {
        val registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, commonTags = mapOf("service" to "ad-server")),
        )
        val text = registry.scrape()
        assertThat(text).contains("# TYPE")
        assertThat(text).contains("jvm_memory_used_bytes")
        registry.close()
    }
}
