package com.github.robran.adserver

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(
    val host: String,
    val port: Int,
)

data class InventoryConfig(
    val jdbcUrl: String,
    val user: String,
    val password: String,
    val skipMigrate: Boolean,
)

data class FrequencyConfig(
    val host: String,
    val port: Int,
    val timeoutMs: Long,
)

data class AppConfig(
    val server: ServerConfig,
    val inventory: InventoryConfig,
    val frequency: FrequencyConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig = raw.extract("adserver")
    }
}
