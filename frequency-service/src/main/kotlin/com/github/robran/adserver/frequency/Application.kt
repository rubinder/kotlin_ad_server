package com.github.robran.adserver.frequency

import com.github.robran.adserver.frequency.metrics.MeterRegistryFactory
import com.github.robran.adserver.frequency.metrics.MetricsHttpServer
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val meterRegistry = MeterRegistryFactory.build(config.metrics)

    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis)

    val server = NettyServerBuilder.forPort(config.server.port)
        .addService(service)
        .build()
        .start()

    val metricsServer = MetricsHttpServer(meterRegistry, config.metrics.port)
    metricsServer.start()

    log.info(
        "frequency-service listening on port {} (redis={}) — /metrics on port {}",
        config.server.port,
        config.redis.url,
        config.metrics.port,
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
