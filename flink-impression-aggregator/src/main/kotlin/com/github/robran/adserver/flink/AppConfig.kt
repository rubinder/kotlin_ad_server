package com.github.robran.adserver.flink

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class FlinkSourceConfig(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val topicImpressionEvents: String,
    val groupId: String,
)

data class RedisSinkConfig(
    val url: String,
    val capWindowSeconds: Long,
    val winhistoryWindowSeconds: Long,
)

data class FlinkAppConfig(
    val source: FlinkSourceConfig,
    val sink: RedisSinkConfig,
    val checkpointIntervalMs: Long,
    val windowSeconds: Long,
    val allowedLatenessSeconds: Long,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): FlinkAppConfig = raw.extract("flink-aggregator")
    }
}
