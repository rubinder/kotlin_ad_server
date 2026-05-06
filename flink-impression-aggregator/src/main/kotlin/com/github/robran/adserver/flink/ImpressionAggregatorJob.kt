package com.github.robran.adserver.flink

import com.github.robran.adserver.protocol.events.ImpressionEvent
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.formats.avro.typeutils.AvroSerializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

object ImpressionAggregatorJob {
    fun build(
        env: StreamExecutionEnvironment,
        config: FlinkAppConfig,
    ): DataStream<WindowedCount> {
        val watermarks =
            WatermarkStrategy
                .forBoundedOutOfOrderness<ImpressionEvent>(Duration.ofSeconds(config.allowedLatenessSeconds))
                .withTimestampAssigner { event, _ -> event.tsMillis }

        val source = kafkaSource(config)
        val stream: DataStream<ImpressionEvent> =
            env
                .fromSource(source, watermarks, "kafka-impression-events", ImpressionEventTypeInfo)

        val aggregated: DataStream<WindowedCount> =
            stream
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
        val schemaRegistryUrl = config.source.schemaRegistryUrl
        val deserializer = KafkaAvroImpressionDeserializer(schemaRegistryUrl)
        return KafkaSource.builder<ImpressionEvent>()
            .setBootstrapServers(config.source.bootstrapServers)
            .setTopics(config.source.topicImpressionEvents)
            .setGroupId(config.source.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(deserializer)
            .build()
    }

    /**
     * A [KafkaRecordDeserializationSchema] that uses [KafkaAvroDeserializer] to decode
     * Avro-encoded [ImpressionEvent] records with Confluent Schema Registry (including mock://).
     * [KafkaAvroDeserializer] natively handles mock:// URLs via MockSchemaRegistry.
     */
    private class KafkaAvroImpressionDeserializer(
        private val schemaRegistryUrl: String,
    ) : KafkaRecordDeserializationSchema<ImpressionEvent> {
        @Transient
        private var inner: KafkaAvroDeserializer? = null

        private fun deserializer(): KafkaAvroDeserializer {
            if (inner == null) {
                inner = KafkaAvroDeserializer()
                inner!!.configure(
                    mapOf(
                        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to true,
                    ),
                    false,
                )
            }
            return inner!!
        }

        override fun deserialize(
            record: ConsumerRecord<ByteArray, ByteArray>,
            out: Collector<ImpressionEvent>,
        ) {
            val event = deserializer().deserialize(record.topic(), record.value()) as? ImpressionEvent
            if (event != null) out.collect(event)
        }

        override fun getProducedType(): TypeInformation<ImpressionEvent> = ImpressionEventTypeInfo
    }

    /**
     * Custom [TypeInformation] for [ImpressionEvent] that uses Flink's [AvroSerializer]
     * (Avro binary encoding) instead of Kryo, bypassing Flink's POJO detector which
     * fails for Avro-generated classes with private fields.
     */
    private object ImpressionEventTypeInfo : TypeInformation<ImpressionEvent>() {
        override fun isBasicType(): Boolean = false

        override fun isTupleType(): Boolean = false

        override fun getArity(): Int = 1

        override fun getTotalFields(): Int = 1

        override fun getTypeClass(): Class<ImpressionEvent> = ImpressionEvent::class.java

        override fun isKeyType(): Boolean = false

        override fun createSerializer(config: SerializerConfig): TypeSerializer<ImpressionEvent> =
            AvroSerializer(ImpressionEvent::class.java)

        @Suppress("OVERRIDE_DEPRECATION")
        override fun createSerializer(config: ExecutionConfig): TypeSerializer<ImpressionEvent> =
            AvroSerializer(ImpressionEvent::class.java)

        override fun toString(): String = "ImpressionEventTypeInfo"

        override fun equals(other: Any?): Boolean = other is ImpressionEventTypeInfo

        override fun hashCode(): Int = ImpressionEvent::class.java.hashCode()

        override fun canEqual(obj: Any?): Boolean = obj is ImpressionEventTypeInfo
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
