package com.github.robran.adserver.frequency

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// Manual @BeforeAll/@AfterAll lifecycle — see InventoryLoaderTest in Phase 1 for the rationale
// (PER_CLASS + @Container interact badly).
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisClientTest {
    private val redis: GenericContainer<*> =
        GenericContainer(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)

    private lateinit var client: RedisClient

    @BeforeAll
    fun setup() {
        redis.start()
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        client = RedisClient.connect(url)
    }

    @AfterAll
    fun tearDown() {
        client.close()
        redis.stop()
    }

    @Test
    fun `get returns null for missing key`() =
        runTest {
            assertThat(client.get("missing-key")).isNull()
        }

    @Test
    fun `set then get round-trips a value`() =
        runTest {
            client.set("my-key", "my-value")
            assertThat(client.get("my-key")).isEqualTo("my-value")
        }

    @Test
    fun `mget returns values in input order, nulls for missing keys`() =
        runTest {
            client.set("k1", "v1")
            client.set("k3", "v3")
            val values = client.mget(listOf("k1", "k2", "k3"))
            assertThat(values).isEqualTo(listOf("v1", null, "v3"))
        }

    @Test
    fun `mget with empty input returns empty list without calling Redis`() =
        runTest {
            val values = client.mget(emptyList())
            assertThat(values).isEmpty()
        }

    @Test
    fun `zrangeByScore reads members ordered by score`() =
        runTest {
            client.zadd("scores", "alpha" to 1.0, "bravo" to 2.0, "charlie" to 3.0)
            val members = client.zrangeByScore("scores", 1.5, 3.5)
            assertThat(members).containsExactlyInAnyOrder("bravo", "charlie")
        }

    @Test
    fun `zrangeByScore returns empty for missing key`() =
        runTest {
            assertThat(client.zrangeByScore("missing-zset", 0.0, Double.POSITIVE_INFINITY)).isEmpty()
        }
}
