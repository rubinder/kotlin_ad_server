package com.github.robran.adserver.inventory

import org.slf4j.LoggerFactory
import javax.sql.DataSource

/**
 * Loads the seed CSVs from classpath into a freshly migrated database.
 * Use in tests and (optionally) at boot in dev.
 */
object SeedLoader {
    private val log = LoggerFactory.getLogger(javaClass)

    fun seed(dataSource: DataSource) {
        seedTable(dataSource, "advertisers", "/seed/advertisers.csv", listOf("id", "name", "domain"))
        seedTable(
            dataSource,
            "campaigns",
            "/seed/campaigns.csv",
            listOf("id", "advertiser_id", "category", "bid_price", "frequency_cap", "active"),
        )
        seedTable(
            dataSource,
            "creatives",
            "/seed/creatives.csv",
            listOf("id", "campaign_id", "width", "height", "markup"),
        )
    }

    private fun seedTable(dataSource: DataSource, table: String, resource: String, cols: List<String>) {
        val rows = parseCsv(resource)
        val placeholders = cols.joinToString(",") { "?" }
        val sql = "INSERT INTO $table (${cols.joinToString(",")}) VALUES ($placeholders) ON CONFLICT (id) DO NOTHING"
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(sql).use { ps ->
                for (row in rows) {
                    cols.forEachIndexed { i, col ->
                        val v = row[col] ?: error("missing column $col in $resource: $row")
                        when (col) {
                            "active" -> ps.setBoolean(i + 1, v.toBoolean())
                            "frequency_cap", "width", "height" -> ps.setInt(i + 1, v.toInt())
                            "bid_price" -> ps.setBigDecimal(i + 1, java.math.BigDecimal(v))
                            else -> ps.setString(i + 1, v)
                        }
                    }
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        }
        log.info("Seeded {} rows into {} from {}", rows.size, table, resource)
    }

    private fun parseCsv(resource: String): List<Map<String, String>> {
        val text = SeedLoader::class.java.getResourceAsStream(resource)
            ?.bufferedReader()
            ?.readText()
            ?: error("resource not found: $resource")
        val lines = text.lines().filter { it.isNotBlank() }
        if (lines.size < 2) return emptyList()
        val header = lines.first().split(",")
        return lines.drop(1).map { line ->
            val cells = line.split(",")
            require(cells.size == header.size) { "malformed CSV row: $line" }
            header.zip(cells).toMap()
        }
    }
}
