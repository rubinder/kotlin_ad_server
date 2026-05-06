package com.github.robran.adserver.frequency

import com.github.robran.adserver.frequency.metrics.MeterRegistryFactory
import com.github.robran.adserver.frequency.metrics.MetricsHttpServer
import com.github.robran.adserver.frequency.tracing.OtelInitializer
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val meterRegistry = MeterRegistryFactory.build(config.metrics)
    val openTelemetry = OtelInitializer.init(config.tracing)
    val grpcTelemetry = GrpcTelemetry.create(openTelemetry)

    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis, meterRegistry)

    val server =
        NettyServerBuilder.forPort(config.server.port)
            .addService(service)
            .intercept(grpcTelemetry.newServerInterceptor())
            .build()
            .start()

    val metricsServer = MetricsHttpServer(meterRegistry, config.metrics.port)
    metricsServer.start()

    log.info(
        "frequency-service listening on port {} (redis={}) — /metrics on port {}, otlp={}",
        config.server.port,
        config.redis.url,
        config.metrics.port,
        config.tracing.otlpEndpoint,
    )

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down frequency-service")
            server.shutdown()
            metricsServer.close()
            redis.close()
            meterRegistry.close()
        },
    )

    server.awaitTermination()
}
