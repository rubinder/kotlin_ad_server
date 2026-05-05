package com.github.robran.adserver.flink

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.github.robran.adserver.flink.Application")

fun main() {
    val config = FlinkAppConfig.load()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
    env.parallelism = 1

    ImpressionAggregatorJob.build(env, config)

    log.info(
        "ImpressionAggregator starting: kafka={} schemaRegistry={} redis={}",
        config.source.bootstrapServers,
        config.source.schemaRegistryUrl,
        config.sink.url,
    )

    env.execute("kotlin_ad_server-impression-aggregator")
}
