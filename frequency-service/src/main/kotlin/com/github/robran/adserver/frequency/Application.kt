package com.github.robran.adserver.frequency

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.frequency.Application")

fun main() {
    val config = AppConfig.load()
    val redis = RedisClient.connect(config.redis.url)
    val service = EnrichService(redis)

    val server = NettyServerBuilder.forPort(config.server.port)
        .addService(service)
        .build()
        .start()

    log.info("frequency-service listening on port {} (redis={})", config.server.port, config.redis.url)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down frequency-service")
            server.shutdown()
            redis.close()
        },
    )

    server.awaitTermination()
}
