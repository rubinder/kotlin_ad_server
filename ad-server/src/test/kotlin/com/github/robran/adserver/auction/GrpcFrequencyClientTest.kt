package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.EnrichResponse
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.Server
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GrpcFrequencyClientTest {
    private lateinit var server: Server
    private lateinit var serverName: String
    private var fakeBehavior: suspend (EnrichRequest) -> EnrichResponse = { EnrichResponse.getDefaultInstance() }

    @BeforeEach
    fun setUp() {
        serverName = InProcessServerBuilder.generateName()
        val service =
            object : FrequencyGrpcKt.FrequencyCoroutineImplBase() {
                override suspend fun enrichForAuction(request: EnrichRequest): EnrichResponse = fakeBehavior(request)
            }
        server =
            InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(service)
                .build()
                .start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdownNow()
    }

    private fun newClient(timeoutMs: Long = 8L): GrpcFrequencyClient {
        val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        return GrpcFrequencyClient(channel, timeoutMs)
    }

    @Test
    fun `returns mapped EnrichResult on a successful RPC`() =
        runTest {
            fakeBehavior = { _ ->
                EnrichResponse.newBuilder()
                    .putFreqCounts("c1", 3)
                    .putFreqCounts("c2", 7)
                    .addRecentCategories("IAB1")
                    .addRecentCategories("IAB13-1")
                    .build()
            }
            val client = newClient(timeoutMs = 5_000L) // generous for the success path

            val result = client.enrich("u1", listOf("c1", "c2"))

            assertThat(result.freqCounts).isEqualTo(mapOf("c1" to 3, "c2" to 7))
            assertThat(result.recentCategories).containsExactlyInAnyOrder("IAB1", "IAB13-1")
        }

    @Test
    fun `fails open on slow server (timeout exceeded)`() =
        runTest {
            fakeBehavior = { _ ->
                delay(50) // longer than the 8ms timeout
                EnrichResponse.newBuilder().putFreqCounts("c1", 99).build()
            }
            val client = newClient(timeoutMs = 8L)

            val result = client.enrich("u1", listOf("c1"))

            // Fail-open returns empty.
            assertThat(result.freqCounts).isEmpty()
            assertThat(result.recentCategories).isEmpty()
        }

    @Test
    fun `fails open on server error`() =
        runTest {
            fakeBehavior = { _ ->
                throw StatusRuntimeException(Status.UNAVAILABLE.withDescription("redis down"))
            }
            val client = newClient(timeoutMs = 5_000L)

            val result = client.enrich("u1", listOf("c1"))

            assertThat(result.freqCounts).isEmpty()
            assertThat(result.recentCategories).isEmpty()
        }

    @Test
    fun `passes user_id and campaign_ids through to the server`() =
        runTest {
            var captured: EnrichRequest? = null
            fakeBehavior = { req ->
                captured = req
                EnrichResponse.getDefaultInstance()
            }
            val client = newClient(timeoutMs = 5_000L)

            client.enrich("user-zalia", listOf("camp-001", "camp-002", "camp-003"))

            val req = captured!!
            assertThat(req.userId).isEqualTo("user-zalia")
            assertThat(req.campaignIdsList.toList()).containsExactlyInAnyOrder("camp-001", "camp-002", "camp-003")
        }
}
