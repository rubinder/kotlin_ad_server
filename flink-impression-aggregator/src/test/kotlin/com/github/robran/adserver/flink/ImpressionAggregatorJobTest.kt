package com.github.robran.adserver.flink

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.events.ImpressionEvent
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.UUID
import io.lettuce.core.RedisClient as LettuceClient

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ImpressionAggregatorJobTest {

    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
    )

    private val redis: GenericContainer<*> = GenericContainer(
        DockerImageName.parse("redis:7-alpine"),
    ).withExposedPorts(6379)

    private val mockScope = "phase3-flink-${UUID.randomUUID()}"
    private val mockUrl = "mock://$mockScope"
    private val topic = "impression-events-test"

    private lateinit var producer: KafkaProducer<String, ImpressionEvent>
    private lateinit var redisLettuce: LettuceClient
    private lateinit var redisConn: StatefulRedisConnection<String, String>

    private val miniCluster = MiniClusterWithClientResource(
        MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build(),
    )

    @BeforeAll
    fun setup() {
        kafka.start()
        redis.start()
        miniCluster.before()

        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockUrl)
            put(ProducerConfig.ACKS_CONFIG, "1")
        }
        producer = KafkaProducer(producerProps)

        redisLettuce = LettuceClient.create(RedisURI.create("redis://${redis.host}:${redis.getMappedPort(6379)}"))
        redisConn = redisLettuce.connect()
    }

    @AfterAll
    fun tearDown() {
        producer.close()
        redisConn.close()
        redisLettuce.shutdown()
        miniCluster.after()
        redis.stop()
        kafka.stop()
        MockSchemaRegistry.dropScope(mockScope)
    }

    @Timeout(value = 180)
    @Test
    fun `aggregates impressions and writes counts back to Redis`() {
        val baseTs = 0L
        val events = (1..5).map { i ->
            ImpressionEvent.newBuilder()
                .setUserId("user-A")
                .setCampaignId("camp-001")
                .setCreativeId("cre-001a")
                .setCategory("IAB13")
                .setPrice(2.0)
                .setTsMillis(baseTs + i * 1000L)
                .build()
        }
        // One impression in the next window, used to drive the watermark past the first window's end.
        val watermarkDriver = ImpressionEvent.newBuilder()
            .setUserId("other")
            .setCampaignId("camp-099")
            .setCreativeId("cre-x")
            .setCategory("IAB1")
            .setPrice(1.0)
            .setTsMillis(15_000L)
            .build()

        for (e in events + watermarkDriver) {
            producer.send(ProducerRecord(topic, e.userId.toString(), e)).get()
        }
        producer.flush()

        // Build the job using a config pointed at the test infra.
        val config = FlinkAppConfig(
            source = FlinkSourceConfig(
                bootstrapServers = kafka.bootstrapServers,
                schemaRegistryUrl = mockUrl,
                topicImpressionEvents = topic,
                groupId = "test-${UUID.randomUUID()}",
            ),
            sink = RedisSinkConfig(
                url = "redis://${redis.host}:${redis.getMappedPort(6379)}",
                capWindowSeconds = 86400,
                winhistoryWindowSeconds = 3600,
            ),
            checkpointIntervalMs = 5_000,
            windowSeconds = 10,
            allowedLatenessSeconds = 2,
        )
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
        env.parallelism = 1

        ImpressionAggregatorJob.build(env, config)

        val jobThread = Thread {
            try {
                env.execute("test-job")
            } catch (_: InterruptedException) {
                // shutdown signal
            }
        }.also { it.isDaemon = true; it.start() }

        // Poll Redis for the expected counter; allow up to 90s for the job + Kafka catch-up.
        val sync = redisConn.sync()
        val deadline = System.currentTimeMillis() + 90_000
        var observed: Long = 0
        while (System.currentTimeMillis() < deadline) {
            val raw = sync.get("freq:user-A:camp-001")
            if (raw != null) {
                observed = raw.toLong()
                if (observed >= 5) break
            }
            Thread.sleep(500)
        }
        jobThread.interrupt()

        assertThat(observed).isEqualTo(5L)

        val winSize = sync.zcard("winhistory:user-A")
        assertThat(winSize).isEqualTo(1L)
    }
}
