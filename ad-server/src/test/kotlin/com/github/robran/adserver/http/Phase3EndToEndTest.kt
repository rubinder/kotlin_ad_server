package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.isGreaterThanOrEqualTo
import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.flink.FlinkAppConfig
import com.github.robran.adserver.flink.FlinkSourceConfig
import com.github.robran.adserver.flink.ImpressionAggregatorJob
import com.github.robran.adserver.flink.RedisSinkConfig
import com.github.robran.adserver.frequency.EnrichService
import com.github.robran.adserver.frequency.RedisClient as FreqRedisClient
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.kafka.KafkaEventEmitter
import com.github.robran.adserver.kafka.ProducerFactory
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.User
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Phase3EndToEndTest {

    private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("kotlin_ad_server_test")
        .withUsername("test")
        .withPassword("test")

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0"),
    )

    private val mockScope = "phase3-e2e-${UUID.randomUUID()}"
    private val mockSchemaUrl = "mock://$mockScope"

    private lateinit var freqRedis: FreqRedisClient
    private lateinit var freqServer: Server
    private lateinit var freqChannel: ManagedChannel
    private lateinit var pipeline: AuctionPipeline
    private lateinit var emitter: KafkaEventEmitter

    private val freqServerName = InProcessServerBuilder.generateName()

    private val miniCluster = MiniClusterWithClientResource(
        MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build(),
    )

    @BeforeAll
    fun setup() {
        postgres.start()
        redis.start()
        kafka.start()
        miniCluster.before()

        // Hydrate inventory from Postgres.
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        val snapshot = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                InventoryLoader(ds).load()
            }

        // Boot frequency-service against the test Redis.
        val redisUrl = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        freqRedis = FreqRedisClient.connect(redisUrl)
        freqServer = InProcessServerBuilder.forName(freqServerName)
            .directExecutor()
            .addService(EnrichService(freqRedis))
            .build()
            .start()
        freqChannel = InProcessChannelBuilder.forName(freqServerName).directExecutor().build()
        val grpcClient = GrpcFrequencyClient(freqChannel, timeoutMs = 1_000L)

        // Pre-create Kafka topics so Flink doesn't fail on startup before the first produce.
        val adminProps = java.util.Properties().apply {
            put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
        }
        org.apache.kafka.clients.admin.AdminClient.create(adminProps).use { admin ->
            admin.createTopics(
                listOf(
                    org.apache.kafka.clients.admin.NewTopic("impression-events-test", 1, 1),
                    org.apache.kafka.clients.admin.NewTopic("auction-results-test", 1, 1),
                ),
            ).all().get()
        }

        // Build the ad-server's Kafka emitter.
        val kafkaConfig = KafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            schemaRegistryUrl = mockSchemaUrl,
            topicAuctionResults = "auction-results-test",
            topicImpressionEvents = "impression-events-test",
            lingerMs = 0,
            acks = "all",
        )
        val producer = ProducerFactory.avroProducer(kafkaConfig)
        emitter = KafkaEventEmitter(producer, kafkaConfig)
        pipeline = buildPipeline(snapshot, grpcClient, emitter)

        // Start the Flink job.
        val flinkConfig = FlinkAppConfig(
            source = FlinkSourceConfig(
                bootstrapServers = kafka.bootstrapServers,
                schemaRegistryUrl = mockSchemaUrl,
                topicImpressionEvents = "impression-events-test",
                groupId = "phase3-e2e",
            ),
            sink = RedisSinkConfig(
                url = redisUrl,
                capWindowSeconds = 86400,
                winhistoryWindowSeconds = 3600,
            ),
            checkpointIntervalMs = 5_000,
            windowSeconds = 5,
            allowedLatenessSeconds = 1,
        )
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(flinkConfig.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
        env.parallelism = 1
        ImpressionAggregatorJob.build(env, flinkConfig)
        Thread {
            try { env.execute("phase3-e2e-flink") } catch (_: InterruptedException) {}
        }.also { it.isDaemon = true; it.start() }
    }

    @AfterAll
    fun tearDown() {
        emitter.close()
        freqChannel.shutdownNow()
        freqServer.shutdownNow()
        freqRedis.close()
        miniCluster.after()
        kafka.stop()
        redis.stop()
        postgres.stop()
        io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry.dropScope(mockScope)
    }

    @Timeout(value = 240)
    @Test
    fun `loop closes - drive impressions, observe Redis counters increase`() {
        val userId = "loop-test-user"

        // Drive 30 HTTP requests through the ad-server. Use testApplication only for the HTTP
        // phase so that its coroutine scope exits cleanly before we start blocking polls.
        testApplication {
            application {
                adServerModule(
                    HealthState().apply { ready.set(true) },
                    pipeline,
                    io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT),
                )
            }
            val httpClient = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

            repeat(30) {
                val response = httpClient.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(
                        BidRequest(
                            id = "req-${System.nanoTime()}",
                            imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
                            user = User(id = userId),
                        ),
                    )
                }
                response.body<BidResponse>()
            }
        }

        // Nudge the Flink watermark past the window boundary by sending a synthetic event
        // with a timestamp 60 seconds in the future. Without this, the event-time window never
        // fires because no subsequent events arrive to advance the watermark.
        val futureTs = System.currentTimeMillis() + 60_000L
        val nudge = com.github.robran.adserver.protocol.events.ImpressionEvent.newBuilder()
            .setUserId("watermark-nudge")
            .setCampaignId("nudge-camp")
            .setCreativeId("nudge-cre")
            .setCategory("IAB1")
            .setPrice(0.0)
            .setTsMillis(futureTs)
            .build()
        emitter.emitImpression(nudge)
        Thread.sleep(2_000) // give the producer time to flush

        // Wait for Flink to drain its window + write to Redis. Window is 5s + 1s lateness;
        // give it generous slack. Blocking Thread.sleep here is fine — we're on a plain JUnit thread.
        val deadline = System.currentTimeMillis() + 120_000
        var totalCounted = 0L
        val sync = io.lettuce.core.RedisClient.create(
            io.lettuce.core.RedisURI.create("redis://${redis.host}:${redis.getMappedPort(6379)}"),
        ).connect().sync()
        while (System.currentTimeMillis() < deadline) {
            val keys = sync.keys("freq:$userId:*")
            totalCounted = keys.sumOf { (sync.get(it)?.toLongOrNull() ?: 0L) }
            if (totalCounted > 0) break
            Thread.sleep(500)
        }

        assertThat(totalCounted).isGreaterThanOrEqualTo(5L)
    }
}
