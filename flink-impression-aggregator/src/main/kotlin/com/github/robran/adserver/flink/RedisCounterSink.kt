package com.github.robran.adserver.flink

import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory
import io.lettuce.core.RedisClient as LettuceClient

/**
 * One windowed aggregation record from the upstream operator.
 *
 * @property userId destination user
 * @property campaignId destination campaign
 * @property category primary IAB category of the campaign (for winhistory member)
 * @property count number of impressions to add
 * @property windowEndMs end of the window (used as the winhistory score)
 */
data class WindowedCount(
    val userId: String,
    val campaignId: String,
    val category: String,
    val count: Long,
    val windowEndMs: Long,
)

/**
 * Writes windowed impression counts back to Redis.
 *
 * Uses Lua scripts (registered once per task) to keep INCRBY+EXPIRE and ZADD+ZREMRANGEBYSCORE
 * atomic. The connection lifecycle is per task instance — Flink calls open() on job startup
 * and close() on shutdown.
 */
class RedisCounterSink(
    private val redisUrl: String,
    private val capWindowSeconds: Long,
    private val winhistoryWindowSeconds: Long,
) : RichSinkFunction<WindowedCount>() {
    private val log = LoggerFactory.getLogger(javaClass)

    @Transient private var lettuce: LettuceClient? = null

    @Transient private var connection: StatefulRedisConnection<String, String>? = null

    @Transient private var scripts: LuaScripts? = null

    override fun open(openContext: OpenContext) {
        val uri = RedisURI.create(redisUrl)
        lettuce = LettuceClient.create(uri)
        connection = lettuce!!.connect()
        scripts = LuaScripts(connection!!.sync())
        log.info("RedisCounterSink open: connected to {}", redisUrl)
    }

    override fun invoke(
        value: WindowedCount,
        context: SinkFunction.Context,
    ) {
        val s = scripts ?: error("Sink not opened")
        val freqKey = "freq:${value.userId}:${value.campaignId}"
        s.incrFreqWithExpiry(freqKey, value.count, capWindowSeconds)

        val winKey = "winhistory:${value.userId}"
        val member = "${value.campaignId}:${value.category}"
        val trimBefore = value.windowEndMs - winhistoryWindowSeconds * 1000
        s.addWinHistory(winKey, value.windowEndMs, member, trimBefore)
    }

    override fun close() {
        try {
            connection?.close()
        } finally {
            lettuce?.shutdown()
        }
        log.info("RedisCounterSink closed")
    }
}
