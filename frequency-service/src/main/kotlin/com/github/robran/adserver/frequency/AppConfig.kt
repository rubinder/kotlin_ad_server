package com.github.robran.adserver.frequency

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class ServerConfig(val port: Int)

data class RedisConfig(val url: String)

data class AppConfig(
    val server: ServerConfig,
    val redis: RedisConfig,
) {
    companion object {
        fun load(raw: Config = ConfigFactory.load()): AppConfig =
            raw.extract("frequency")
    }
}
