package com.github.robran.adserver.http

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThanOrEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.github.robran.adserver.adServerModule
import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.FakeFrequencyClient
import com.github.robran.adserver.buildPipeline
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.SeedLoader
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.BidResponse
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.NoBidReason
import com.github.robran.adserver.protocol.openrtb.User
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

// Manual @BeforeAll/@AfterAll lifecycle: do NOT use @Testcontainers/@Container — see
// InventoryLoaderTest in Task 9 for the rationale (PER_CLASS + @Container interact badly).
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BidRouteIntegrationTest {
    private val postgres: PostgreSQLContainer<*> =
        PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("kotlin_ad_server_test")
            .withUsername("test")
            .withPassword("test")

    private lateinit var pipeline: AuctionPipeline

    @BeforeAll
    fun setup() {
        postgres.start()
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds ->
                SeedLoader.seed(ds)
                val snapshot = InventoryLoader(ds).load()
                pipeline = buildPipeline(snapshot, FakeFrequencyClient())
            }
    }

    @AfterAll
    fun tearDown() {
        postgres.stop()
    }

    private fun banner300x250Request(
        userId: String = "user-zalia",
        floor: Double = 0.0,
        bcat: List<String> = emptyList(),
    ) = BidRequest(
        id = "req-it-${System.nanoTime()}",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250), bidfloor = floor)),
        user = User(id = userId),
        bcat = bcat,
    )

    @Test
    fun `POST openrtb bid returns a winning Bid for a 300x250 request`() =
        testApplication {
            application {
                adServerModule(HealthState().apply { ready.set(true) }, pipeline)
            }
            val client =
                createClient {
                    install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
                }
            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request())
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(1)
            val bid = body.seatbid[0].bid[0]
            assertThat(bid.cid).isNotNull()
            assertThat(bid.crid).isNotNull()
            assertThat(bid.price).isGreaterThanOrEqualTo(0.0)
            assertThat(bid.w).isEqualTo(300)
            assertThat(bid.h).isEqualTo(250)
            assertThat(body.nbr).isNull()
        }

    @Test
    fun `POST openrtb bid honors bcat blocking — empty result when all categories blocked`() =
        testApplication {
            application {
                adServerModule(HealthState().apply { ready.set(true) }, pipeline)
            }
            val client =
                createClient {
                    install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
                }
            // Block ALL categories present in the seed data.
            val allCategories =
                listOf(
                    "IAB13", "IAB13-1", "IAB13-2", "IAB13-3", "IAB13-7", "IAB13-9",
                    "IAB19", "IAB19-6", "IAB19-15", "IAB19-30",
                    "IAB8", "IAB8-1", "IAB8-5", "IAB8-9",
                    "IAB2", "IAB2-2", "IAB2-5",
                    "IAB20", "IAB20-1", "IAB20-3", "IAB20-26",
                    "IAB5", "IAB5-1", "IAB5-3", "IAB5-13",
                    "IAB16", "IAB16-1", "IAB16-7",
                )
            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request(bcat = allCategories))
                }
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(0)
            assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_BLOCKING)
        }

    @Test
    fun `POST openrtb bid honors floor price`() =
        testApplication {
            application {
                adServerModule(HealthState().apply { ready.set(true) }, pipeline)
            }
            val client =
                createClient {
                    install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
                }
            // Set floor higher than every campaign's bid (max in seed is 6.10).
            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(banner300x250Request(floor = 99.0))
                }
            val body: BidResponse = response.body()
            assertThat(body.seatbid).hasSize(0)
            assertThat(body.nbr).isEqualTo(NoBidReason.NO_CANDIDATES_AFTER_FLOOR)
        }

    @Test
    fun `healthz returns 200`() =
        testApplication {
            application {
                adServerModule(HealthState().apply { ready.set(true) }, pipeline)
            }
            val response = client.get("/healthz")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `POST openrtb bid returns 400 when imp list is empty`() =
        testApplication {
            application {
                adServerModule(HealthState().apply { ready.set(true) }, pipeline)
            }
            val client =
                createClient {
                    install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
                }
            val emptyImpRequest =
                BidRequest(
                    id = "req-empty",
                    imp = emptyList(),
                    user = User(id = "u"),
                )
            val response =
                client.post("/openrtb/bid") {
                    contentType(ContentType.Application.Json)
                    setBody(emptyImpRequest)
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
        }
}
