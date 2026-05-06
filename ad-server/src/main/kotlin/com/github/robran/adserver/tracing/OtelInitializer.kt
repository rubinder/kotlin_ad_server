package com.github.robran.adserver.tracing

import com.github.robran.adserver.TracingConfig
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Builds the OpenTelemetry SDK with an OTLP gRPC exporter pointing at Jaeger (or any other
 * OTLP collector). The SDK is registered as the JVM-global instance so any libraries that
 * call [io.opentelemetry.api.GlobalOpenTelemetry.get] pick it up.
 *
 * When tracing is disabled, returns a no-op instance that emits no spans — useful for tests
 * and environments without a collector.
 */
object OtelInitializer {
    private val log = LoggerFactory.getLogger(javaClass)

    fun init(config: TracingConfig): OpenTelemetry {
        if (!config.enabled) {
            log.info("OpenTelemetry tracing disabled by config")
            return OpenTelemetry.noop()
        }
        val resource =
            Resource.getDefault().merge(
                Resource.create(
                    Attributes.of(
                        AttributeKey.stringKey("service.name"),
                        config.serviceName,
                    ),
                ),
            )
        val exporter: SpanExporter =
            OtlpGrpcSpanExporter.builder()
                .setEndpoint(config.otlpEndpoint)
                .setTimeout(2, TimeUnit.SECONDS)
                .build()
        val tracerProvider =
            SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .setResource(resource)
                .build()
        val sdk =
            OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal()
        log.info("OpenTelemetry SDK initialized: service={}, otlp={}", config.serviceName, config.otlpEndpoint)
        return sdk
    }
}
