package com.github.robran.adserver.frequency.metrics

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import com.github.robran.adserver.frequency.MetricsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.HttpURLConnection
import java.net.URL

class MetricsHttpServerTest {

    private lateinit var registry: io.micrometer.prometheusmetrics.PrometheusMeterRegistry
    private lateinit var server: MetricsHttpServer
    private val port = freePort()

    @BeforeEach
    fun setup() {
        registry = MeterRegistryFactory.build(
            MetricsConfig(enabled = true, port = port, commonTags = mapOf("service" to "frequency-service")),
        )
        server = MetricsHttpServer(registry, port)
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.close()
        registry.close()
    }

    @Test
    fun `GET metrics returns 200 with Prometheus text`() {
        val conn = URL("http://localhost:$port/metrics").openConnection() as HttpURLConnection
        try {
            assertThat(conn.responseCode).isEqualTo(200)
            val body = conn.inputStream.bufferedReader().readText()
            assertThat(body).contains("# TYPE")
            assertThat(body).contains("jvm_memory_used_bytes")
        } finally {
            conn.disconnect()
        }
    }

    @Test
    fun `GET healthz returns 200`() {
        val conn = URL("http://localhost:$port/healthz").openConnection() as HttpURLConnection
        try {
            assertThat(conn.responseCode).isEqualTo(200)
            assertThat(conn.inputStream.bufferedReader().readText().trim()).isEqualTo("ok")
        } finally {
            conn.disconnect()
        }
    }

    private fun freePort(): Int {
        java.net.ServerSocket(0).use { return it.localPort }
    }
}
