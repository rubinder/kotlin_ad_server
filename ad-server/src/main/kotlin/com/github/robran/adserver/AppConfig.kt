package com.github.robran.adserver

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(
    val host: String,
    val port: Int,
)

data class InventoryConfig(
    val jdbcUrl: String,
    val user: String,
    val password: String,
    val skipMigrate: Boolean,
)

data class FrequencyConfig(
    val host: String,
    val port: Int,
    val timeoutMs: Long,
)

data class KafkaConfig(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val topicAuctionResults: String,
    val topicImpressionEvents: String,
    val lingerMs: Int,
    val acks: String,
)

data class MetricsConfig(
    val enabled: Boolean,
    val commonTags: Map<String, String>,
)

data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
    val frequency: FrequencyConfig,
    val kafka: KafkaConfig,
    val metrics: MetricsConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig =
            raw.extract("adserver")
    }
}
