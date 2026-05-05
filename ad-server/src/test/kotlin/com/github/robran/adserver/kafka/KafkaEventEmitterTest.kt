package com.github.robran.adserver.kafka

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import com.github.robran.adserver.protocol.events.Outcome
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.Timeout
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class KafkaEventEmitterTest {
    private val kafka: ConfluentKafkaContainer =
        ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
        )

    private val mockSchemaRegistryScope = "phase3-test-${UUID.randomUUID()}"
    private val mockSchemaRegistryUrl = "mock://$mockSchemaRegistryScope"

    private lateinit var producer: KafkaProducer<String, Any>
    private lateinit var emitter: KafkaEventEmitter
    private lateinit var config: KafkaConfig
    private lateinit var registry: io.micrometer.core.instrument.simple.SimpleMeterRegistry

    @BeforeAll
    fun setup() {
        kafka.start()
        config =
            KafkaConfig(
                bootstrapServers = kafka.bootstrapServers,
                schemaRegistryUrl = mockSchemaRegistryUrl,
                topicAuctionResults = "auction-results-test",
                topicImpressionEvents = "impression-events-test",
                lingerMs = 0,
                acks = "1",
            )
        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
                put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl)
                put(ProducerConfig.ACKS_CONFIG, "1")
            }
        producer = KafkaProducer(props)
        registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        emitter = KafkaEventEmitter(producer, config, meterRegistry = registry)
    }

    @AfterAll
    fun tearDown() {
        emitter.close()
        kafka.stop()
        MockSchemaRegistry.dropScope(mockSchemaRegistryScope)
    }

    @Order(1)
    @Timeout(value = 30)
    @Test
    fun `emitImpression sends an Avro-encoded record to the configured topic`() {
        val event =
            ImpressionEvent.newBuilder()
                .setUserId("user-1")
                .setCampaignId("camp-001")
                .setCreativeId("cre-001a")
                .setCategory("IAB13")
                .setPrice(2.50)
                .setTsMillis(1L)
                .build()
        emitter.emitImpression(event)
        producer.flush()

        val received = pollOne<ImpressionEvent>(config.topicImpressionEvents)
        assertThat(received.userId.toString()).isEqualTo("user-1")
        assertThat(received.campaignId.toString()).isEqualTo("camp-001")
    }

    @Order(2)
    @Timeout(value = 30)
    @Test
    fun `emitAuctionResult sends an Avro-encoded record with outcome enum`() {
        val event =
            AuctionResultEvent.newBuilder()
                .setRequestId("r1")
                .setUserId("user-1")
                .setImpId("1")
                .setTsMillis(1L)
                .setOutcome(Outcome.FILLED)
                .setWinnerCampaignId("camp-007")
                .setWinnerPrice(3.10)
                .setCandidatesInitial(5)
                .setCandidatesAfterBlocking(5)
                .setCandidatesAfterFreqCompsep(3)
                .setCandidatesAfterFloor(2)
                .build()
        emitter.emitAuctionResult(event)
        producer.flush()

        val received = pollOne<AuctionResultEvent>(config.topicAuctionResults)
        assertThat(received.outcome).isEqualTo(Outcome.FILLED)
        assertThat(received.winnerCampaignId.toString()).isEqualTo("camp-007")
    }

    @Order(3)
    @Timeout(value = 30)
    @Test
    fun `records kafka_producer_send_duration tagged by topic`() {
        emitter.emitImpression(
            ImpressionEvent.newBuilder()
                .setUserId("u1")
                .setCampaignId("c1")
                .setCreativeId("cre1")
                .setCategory("IAB1")
                .setPrice(1.0)
                .setTsMillis(1L)
                .build(),
        )
        producer.flush()
        // Wait for the async callback to record the timing.
        val deadline = System.currentTimeMillis() + 5_000
        var observed = 0L
        while (System.currentTimeMillis() < deadline && observed == 0L) {
            observed = registry.timer(
                "kafka.producer.send.duration",
                "topic",
                config.topicImpressionEvents,
            ).count()
            if (observed == 0L) Thread.sleep(50)
        }
        assertThat(observed).isEqualTo(1L)
    }

    private inline fun <reified T> pollOne(topic: String): T {
        val consumerProps =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java.name)
                put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl)
                put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
            }
        val consumer = KafkaConsumer<String, T>(consumerProps)
        consumer.use {
            it.subscribe(listOf(topic))
            val deadline = System.currentTimeMillis() + 20_000
            while (System.currentTimeMillis() < deadline) {
                val records = it.poll(Duration.ofMillis(500))
                for (record in records) {
                    return record.value()
                }
            }
            throw AssertionError("No record received on topic $topic within 20s")
        }
    }
}
