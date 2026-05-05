package com.github.robran.adserver.flink

import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import org.slf4j.LoggerFactory

/**
 * Loads the Lua scripts from the classpath, registers them with Redis on first use (SCRIPT LOAD),
 * and caches the SHA1 for subsequent EVALSHA calls. This is the pattern Lettuce + Redis recommend.
 *
 * Not thread-safe: a single LuaScripts instance is owned by a single Flink RichSinkFunction and
 * accessed serially by the sink's checkpoint thread.
 */
class LuaScripts(private val sync: RedisCommands<String, String>) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val incrFreqSrc: String = readResource("/lua/incrFreqWithExpiry.lua")
    private val addWinHistorySrc: String = readResource("/lua/addWinHistory.lua")

    private var incrFreqSha: String? = null
    private var addWinHistorySha: String? = null

    fun incrFreqWithExpiry(key: String, increment: Long, ttlSeconds: Long): Long {
        val sha = ensureLoaded(incrFreqSrc, ::incrFreqShaHolder)
        return sync.evalsha<Long>(
            sha,
            ScriptOutputType.INTEGER,
            arrayOf(key),
            increment.toString(),
            ttlSeconds.toString(),
        )
    }

    fun addWinHistory(key: String, scoreMs: Long, member: String, trimBeforeMs: Long): Long {
        val sha = ensureLoaded(addWinHistorySrc, ::addWinHistoryShaHolder)
        return sync.evalsha<Long>(
            sha,
            ScriptOutputType.INTEGER,
            arrayOf(key),
            scoreMs.toString(),
            member,
            trimBeforeMs.toString(),
        )
    }

    private fun incrFreqShaHolder(value: String?): String? {
        incrFreqSha = value
        return incrFreqSha
    }

    private fun addWinHistoryShaHolder(value: String?): String? {
        addWinHistorySha = value
        return addWinHistorySha
    }

    private fun ensureLoaded(source: String, holder: (String?) -> String?): String {
        val current = holder(null) // read
        if (current != null) return current
        val sha = sync.scriptLoad(source)
        holder(sha) // write
        return sha
    }

    companion object {
        private fun readResource(path: String): String =
            LuaScripts::class.java.getResourceAsStream(path)
                ?.bufferedReader()
                ?.use { it.readText() }
                ?: error("Lua script not found on classpath: $path")
    }
}
