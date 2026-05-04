package com.github.robran.adserver.inventory

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InventoryLoaderTest {

    @Container
    val postgres: PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
        .withDatabaseName("kotlin_ad_server_test")
        .withUsername("test")
        .withPassword("test")

    @BeforeAll
    fun migrateAndSeed() {
        postgres.start()
        InventoryLoader.migrate(postgres.jdbcUrl, postgres.username, postgres.password)
        InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .use { ds -> SeedLoader.seed(ds) }
    }

    @AfterAll
    fun stop() {
        postgres.stop()
    }

    @Test
    fun `loads 50 campaigns from seed data`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            assertThat(snapshot.size).isEqualTo(50)
        }
    }

    @Test
    fun `each campaign has at least one creative and an advertiser domain`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            for (c in snapshot.campaigns) {
                assertThat(c.creatives.size).isGreaterThan(0)
                assertThat(c.advertiserDomain).isNotNull()
                assertThat(c.advertiserDomain).contains("example.com")
            }
        }
    }

    @Test
    fun `byCampaignId index has every campaign`() {
        val ds = InventoryLoader.pooledDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        ds.use { dataSource ->
            val snapshot = InventoryLoader(dataSource).load()
            assertThat(snapshot.byCampaignId["camp-001"]).isNotNull()
            assertThat(snapshot.byCampaignId["camp-050"]).isNotNull()
            assertThat(snapshot.byCampaignId.size).isEqualTo(snapshot.size)
        }
    }
}
