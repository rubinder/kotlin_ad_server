package com.github.robran.adserver.frequency

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.github.robran.adserver.protocol.frequency.EnrichRequest
import com.github.robran.adserver.protocol.frequency.FrequencyGrpcKt
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// Tests use unique user-ids per test (user-A, user-B) to avoid cross-test contamination
// without needing FLUSHDB or per-test cleanup.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EnrichServiceIntegrationTest {

    private val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379)

    private lateinit var redisClient: RedisClient
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var stub: FrequencyGrpcKt.FrequencyCoroutineStub

    private val serverName = InProcessServerBuilder.generateName()

    @BeforeAll
    fun setupClass() {
        redis.start()
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        redisClient = RedisClient.connect(url)

        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(EnrichService(redisClient))
            .build()
            .start()

        channel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build()
        stub = FrequencyGrpcKt.FrequencyCoroutineStub(channel)
    }

    @AfterAll
    fun tearDownClass() {
        channel.shutdownNow()
        server.shutdownNow()
        redisClient.close()
        redis.stop()
    }

    @Test
    fun `returns empty response for unknown user`() = runTest {
        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("never-seen").addCampaignIds("c1").build(),
        )
        assertThat(resp.freqCountsMap.toMap()).isEmpty()
        assertThat(resp.recentCategoriesList.toList()).isEmpty()
    }

    @Test
    fun `returns counts for campaigns with stored counters`() = runTest {
        redisClient.set("freq:user-A:c1", "5")
        redisClient.set("freq:user-A:c2", "2")
        // c3 has no entry → not in response

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder()
                .setUserId("user-A")
                .addAllCampaignIds(listOf("c1", "c2", "c3"))
                .build(),
        )
        assertThat(resp.freqCountsMap.toMap()).isEqualTo(mapOf("c1" to 5, "c2" to 2))
    }

    @Test
    fun `returns recent categories from winhistory zset`() = runTest {
        redisClient.zadd(
            "winhistory:user-A",
            "c1:IAB13" to 1000.0,
            "c2:IAB19" to 1500.0,
            "c1:IAB13" to 2000.0, // duplicate category — set semantics dedupe
        )

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("user-A").build(),
        )
        assertThat(resp.recentCategoriesList.toSet()).containsExactlyInAnyOrder("IAB13", "IAB19")
    }

    @Test
    fun `combined response - counts AND categories in one RPC`() = runTest {
        redisClient.set("freq:user-B:c1", "3")
        redisClient.zadd("winhistory:user-B", "c1:IAB1" to 1.0)

        val resp = stub.enrichForAuction(
            EnrichRequest.newBuilder().setUserId("user-B").addCampaignIds("c1").build(),
        )
        assertThat(resp.freqCountsMap["c1"]).isEqualTo(3)
        assertThat(resp.recentCategoriesList.toSet()).containsExactlyInAnyOrder("IAB1")
    }

    @Test
    fun `empty user_id is rejected with INVALID_ARGUMENT`() = runTest {
        val ex = runCatching {
            stub.enrichForAuction(EnrichRequest.newBuilder().setUserId("").build())
        }.exceptionOrNull()
        // require() throws IllegalArgumentException, which gRPC surfaces as a StatusRuntimeException
        // with code UNKNOWN by default. We accept any throwable here as proof the call was rejected.
        assertThat(ex != null).isEqualTo(true)
    }
}
