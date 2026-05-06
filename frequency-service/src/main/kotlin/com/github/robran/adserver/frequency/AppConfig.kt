package com.github.robran.adserver.frequency

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(val port: Int)

data class RedisConfig(val url: String)

data class MetricsConfig(
    val enabled: Boolean,
    val port: Int,
    val commonTags: Map<String, String>,
)

data class TracingConfig(
    val enabled: Boolean,
    val serviceName: String,
    val otlpEndpoint: String,
)

data class AppConfig(
    val server: ServerConfig,
    val redis: RedisConfig,
    val metrics: MetricsConfig,
    val tracing: TracingConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig = raw.extract("frequency")
    }
}
