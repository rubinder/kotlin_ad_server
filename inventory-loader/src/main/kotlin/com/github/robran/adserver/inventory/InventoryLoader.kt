package com.github.robran.adserver.inventory

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import java.time.Instant
import javax.sql.DataSource

class InventoryLoader(private val dataSource: DataSource) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Read all campaigns + their creatives from Postgres and assemble an immutable [InventorySnapshot].
     * Returns active campaigns only (active=true).
     */
    fun load(): InventorySnapshot {
        val started = System.nanoTime()
        val campaigns = readCampaigns()
        val creativesByCampaign = readCreativesGroupedByCampaign()
        val advertiserDomainsById = readAdvertiserDomains()

        val assembled =
            campaigns
                .filter { it.active }
                .map { c ->
                    c.copy(
                        advertiserDomain =
                            advertiserDomainsById[c.advertiserId]
                                ?: error("orphan campaign ${c.id}: advertiser ${c.advertiserId} not found"),
                        creatives = creativesByCampaign[c.id].orEmpty(),
                    )
                }

        val durMs = (System.nanoTime() - started) / 1_000_000
        log.info("Inventory snapshot loaded: {} campaigns ({} ms)", assembled.size, durMs)
        return InventorySnapshot(assembled, Instant.now())
    }

    private fun readCampaigns(): List<Campaign> =
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT id, advertiser_id, category, bid_price, frequency_cap, active
                FROM campaigns
                """.trimIndent(),
            ).use { ps ->
                ps.executeQuery().use { rs ->
                    buildList {
                        while (rs.next()) {
                            add(
                                Campaign(
                                    id = rs.getString("id"),
                                    advertiserId = rs.getString("advertiser_id"),
                                    // filled in by load()
                                    advertiserDomain = "",
                                    category = rs.getString("category"),
                                    bidPrice = rs.getBigDecimal("bid_price").toDouble(),
                                    frequencyCap = rs.getInt("frequency_cap"),
                                    active = rs.getBoolean("active"),
                                    // filled in by load()
                                    creatives = emptyList(),
                                ),
                            )
                        }
                    }
                }
            }
        }

    private fun readCreativesGroupedByCampaign(): Map<String, List<Creative>> =
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT id, campaign_id, width, height, markup
                FROM creatives
                """.trimIndent(),
            ).use { ps ->
                ps.executeQuery().use { rs ->
                    val acc = mutableMapOf<String, MutableList<Creative>>()
                    while (rs.next()) {
                        val campaignId = rs.getString("campaign_id")
                        acc.getOrPut(campaignId) { mutableListOf() }.add(
                            Creative(
                                id = rs.getString("id"),
                                campaignId = campaignId,
                                width = rs.getInt("width"),
                                height = rs.getInt("height"),
                                markup = rs.getString("markup"),
                            ),
                        )
                    }
                    acc
                }
            }
        }

    private fun readAdvertiserDomains(): Map<String, String> =
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT id, domain FROM advertisers").use { ps ->
                ps.executeQuery().use { rs ->
                    buildMap { while (rs.next()) put(rs.getString("id"), rs.getString("domain")) }
                }
            }
        }

    companion object {
        fun migrate(
            jdbcUrl: String,
            user: String,
            password: String,
        ) {
            Flyway.configure()
                .dataSource(jdbcUrl, user, password)
                .locations("classpath:db/migration")
                .load()
                .migrate()
        }

        fun pooledDataSource(
            jdbcUrl: String,
            user: String,
            password: String,
        ): HikariDataSource {
            val cfg =
                HikariConfig().apply {
                    this.jdbcUrl = jdbcUrl
                    this.username = user
                    this.password = password
                    this.maximumPoolSize = 4 // boot-time only, not on hot path
                    this.poolName = "inventory-pool"
                }
            return HikariDataSource(cfg)
        }
    }
}
