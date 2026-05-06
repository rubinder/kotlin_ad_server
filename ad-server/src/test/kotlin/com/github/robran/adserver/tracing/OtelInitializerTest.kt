package com.github.robran.adserver.tracing

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.robran.adserver.TracingConfig
import io.opentelemetry.api.OpenTelemetry
import org.junit.jupiter.api.Test

class OtelInitializerTest {
    @Test
    fun `disabled config returns no-op OpenTelemetry`() {
        val sdk =
            OtelInitializer.init(
                TracingConfig(enabled = false, serviceName = "test", otlpEndpoint = ""),
            )
        // OpenTelemetry.noop() returns a singleton; comparison should be by reference.
        assertThat(sdk === OpenTelemetry.noop()).isEqualTo(true)
    }
}
