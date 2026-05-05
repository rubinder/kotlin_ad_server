package com.github.robran.adserver.frequency

import io.lettuce.core.Range
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.reactive.RedisReactiveCommands
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.LoggerFactory
import io.lettuce.core.RedisClient as LettuceClient

/**
 * Thin coroutine facade over Lettuce's reactive API. Exposes only the operations the read-side
 * frequency service needs. Closing the client closes the underlying connection and shuts down the
 * Lettuce client thread pool.
 */
class RedisClient(
    private val lettuce: LettuceClient,
    private val connection: StatefulRedisConnection<String, String>,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private val cmd: RedisReactiveCommands<String, String> = connection.reactive()

    suspend fun get(key: String): String? =
        cmd.get(key).awaitSingleOrNull()

    suspend fun set(key: String, value: String): String =
        cmd.set(key, value).awaitSingle()

    suspend fun mget(keys: List<String>): List<String?> {
        if (keys.isEmpty()) return emptyList()
        // Lettuce returns KeyValue<K,V>; map to V or null in the input order.
        val results = cmd.mget(*keys.toTypedArray())
            .collectList()
            .awaitSingle()
        // The reactive mget preserves input order. Each result is a KeyValue with hasValue() flag.
        val byKey = results.associateBy { it.key }
        return keys.map { k -> byKey[k]?.let { kv -> if (kv.hasValue()) kv.value else null } }
    }

    suspend fun zadd(key: String, vararg members: Pair<String, Double>): Long {
        val scoredValues = members.map { (m, s) -> io.lettuce.core.ScoredValue.just(s, m) }.toTypedArray()
        return cmd.zadd(key, *scoredValues).awaitSingle()
    }

    suspend fun zrangeByScore(key: String, min: Double, max: Double): List<String> {
        val range = Range.create(min, max)
        return cmd.zrangebyscore(key, range)
            .collectList()
            .awaitSingle()
    }

    override fun close() {
        try {
            connection.close()
        } finally {
            lettuce.shutdown()
        }
    }

    companion object {
        fun connect(redisUrl: String): RedisClient {
            val uri = RedisURI.create(redisUrl)
            val lettuce = LettuceClient.create(uri)
            val connection = lettuce.connect()
            return RedisClient(lettuce, connection)
        }
    }
}
