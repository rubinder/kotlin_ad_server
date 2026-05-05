package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

/**
 * Builds Kafka producers configured for fire-and-forget Avro emission against Confluent Schema
 * Registry. Producer instances are heavyweight (each owns a thread + network socket) — create
 * one per JVM and reuse it across requests.
 */
object ProducerFactory {
    fun avroProducer(config: KafkaConfig): KafkaProducer<String, Any> {
        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
                put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl)
                put(ProducerConfig.ACKS_CONFIG, config.acks)
                put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs)
                put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                put(ProducerConfig.RETRIES_CONFIG, 3)
            }
        return KafkaProducer(props)
    }
}
