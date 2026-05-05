package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.frequency.EnrichService
import com.github.robran.adserver.frequency.RedisClient
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
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
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Phase2EndToEndTest {
    private val postgres: PostgreSQLContainer<*> =
        PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("kotlin_ad_server_test")
            .withUsername("test")
            .withPassword("test")

    private val redis: GenericContainer<*> =
        GenericContainer(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)

    private lateinit var redisClient: RedisClient
    private lateinit var freqServer: Server
    private lateinit var freqChannel: ManagedChannel
    private lateinit var pipeline: AuctionPipeline

    private val serverName = InProcessServerBuilder.generateName()

    @BeforeAll
    fun setup() {
        postgres.start()
        redis.start()

        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        val snapshot =
            InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
                .use { ds ->
                    SeedLoader.seed(ds)
                    InventoryLoader(ds).load()
                }

        val redisUrl = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        redisClient = RedisClient.connect(redisUrl)
        freqServer =
            InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(EnrichService(redisClient))
                .build()
                .start()

        freqChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        val grpcClient = GrpcFrequencyClient(freqChannel, timeoutMs = 1_000L)
        pipeline = buildPipeline(snapshot, grpcClient)
    }

    @AfterAll
    fun tearDown() {
        freqChannel.shutdownNow()
        freqServer.shutdownNow()
        redisClient.close()
        redis.stop()
        postgres.stop()
    }

    private fun banner300x250Request(userId: String) =
        BidRequest(
            id = "req-${System.nanoTime()}",
            imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
            user = User(id = userId),
        )

    @Test
    fun `auction returns a winner when no caps are set`() =
        testApplication {
            application {
                adServerModule(
                    HealthState().apply { ready.set(true) },
                    pipeline,
                    io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT),
                )
            }
            val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request(userId = "fresh-user"))
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(1)
            assertThat(body.seatbid[0].bid[0].cid).isNotNull()
        }

    @Test
    fun `auction skips a campaign whose freq cap is hit`() =
        testApplication {
            application {
                adServerModule(
                    HealthState().apply { ready.set(true) },
                    pipeline,
                    io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT),
                )
            }
            val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

            val userId = "capped-user"
            runBlocking {
                for (i in 1..50) {
                    val cid = "camp-%03d".format(i)
                    redisClient.set("freq:$userId:$cid", "999")
                }
            }

            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request(userId = userId))
                }
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(0)
            assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
        }

    @Test
    fun `competitive separation drops candidates whose category is in winhistory`() =
        testApplication {
            application {
                adServerModule(
                    HealthState().apply { ready.set(true) },
                    pipeline,
                    io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT),
                )
            }
            val client = createClient { install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) } }

            val userId = "saturated-user"
            val seedCategories =
                listOf(
                    "IAB13", "IAB13-1", "IAB13-2", "IAB13-3", "IAB13-7", "IAB13-9",
                    "IAB19", "IAB19-6", "IAB19-15", "IAB19-30",
                    "IAB8", "IAB8-1", "IAB8-5", "IAB8-9",
                    "IAB2", "IAB2-2", "IAB2-5",
                    "IAB20", "IAB20-1", "IAB20-3", "IAB20-26",
                    "IAB5", "IAB5-1", "IAB5-3", "IAB5-13",
                    "IAB16", "IAB16-1", "IAB16-7",
                )
            runBlocking {
                redisClient.zadd(
                    "winhistory:$userId",
                    *seedCategories.mapIndexed { i, c -> "fake-camp-$i:$c" to i.toDouble() }.toTypedArray(),
                )
            }

            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request(userId = userId))
                }
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(0)
            assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FREQ_COMPSEP)
        }
}
