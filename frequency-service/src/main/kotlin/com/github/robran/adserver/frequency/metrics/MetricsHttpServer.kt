package com.github.robran.adserver.frequency.metrics

import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * Tiny HTTP server backing /metrics for the frequency-service. We use Java's built-in
 * [com.sun.net.httpserver.HttpServer] rather than Ktor because frequency-service is gRPC-only —
 * adding a full Ktor stack just for /metrics is overkill.
 */
class MetricsHttpServer(
    private val registry: PrometheusMeterRegistry,
    private val port: Int,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)
    private var server: HttpServer? = null

    fun start() {
        val s = HttpServer.create(InetSocketAddress(port), 0)
        s.createContext("/metrics") { exchange ->
            val body = registry.scrape().toByteArray()
            exchange.responseHeaders.set("Content-Type", "text/plain; version=0.0.4")
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        s.createContext("/healthz") { exchange ->
            val body = "ok\n".toByteArray()
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        s.executor = null
        s.start()
        server = s
        log.info("MetricsHttpServer listening on port {}", port)
    }

    override fun close() {
        server?.stop(0)
        server = null
    }
}
