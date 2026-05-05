package com.github.robran.adserver.flink

import com.github.robran.adserver.protocol.events.ImpressionEvent
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Duration

object ImpressionAggregatorJob {

    fun build(env: StreamExecutionEnvironment, config: FlinkAppConfig): DataStream<WindowedCount> {
        val watermarks = WatermarkStrategy
            .forBoundedOutOfOrderness<ImpressionEvent>(Duration.ofSeconds(config.allowedLatenessSeconds))
            .withTimestampAssigner { event, _ -> event.tsMillis }

        val source = kafkaSource(config)
        val stream: DataStream<ImpressionEvent> = env
            .fromSource(source, watermarks, "kafka-impression-events")

        val aggregated: DataStream<WindowedCount> = stream
            .keyBy { event -> "${event.userId}|${event.campaignId}" }
            .window(TumblingEventTimeWindows.of(Time.seconds(config.windowSeconds)))
            .allowedLateness(Time.seconds(config.allowedLatenessSeconds))
            .process(CountAndEmit())

        aggregated.addSink(
            RedisCounterSink(
                redisUrl = config.sink.url,
                capWindowSeconds = config.sink.capWindowSeconds,
                winhistoryWindowSeconds = config.sink.winhistoryWindowSeconds,
            ),
        ).name("redis-counter-sink")

        return aggregated
    }

    private fun kafkaSource(config: FlinkAppConfig): KafkaSource<ImpressionEvent> {
        val deserializer = ConfluentRegistryAvroDeserializationSchema.forSpecific(
            ImpressionEvent::class.java,
            config.source.schemaRegistryUrl,
        )
        return KafkaSource.builder<ImpressionEvent>()
            .setBootstrapServers(config.source.bootstrapServers)
            .setTopics(config.source.topicImpressionEvents)
            .setGroupId(config.source.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(deserializer)
            .build()
    }

    /** Counts events per window and emits one WindowedCount with the window's end timestamp. */
    private class CountAndEmit : ProcessWindowFunction<ImpressionEvent, WindowedCount, String, TimeWindow>() {
        override fun process(
            key: String,
            context: Context,
            elements: Iterable<ImpressionEvent>,
            out: Collector<WindowedCount>,
        ) {
            val list = elements.toList()
            if (list.isEmpty()) return
            val sample = list.first()
            val count = list.size.toLong()
            out.collect(
                WindowedCount(
                    userId = sample.userId.toString(),
                    campaignId = sample.campaignId.toString(),
                    category = sample.category.toString(),
                    count = count,
                    windowEndMs = context.window().end,
                ),
            )
        }
    }
}
