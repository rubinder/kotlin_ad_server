package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

/** Public emitter contract — pipeline depends on this, not the concrete Kafka type. */
interface EventEmitter {
    fun emitImpression(event: ImpressionEvent)

    fun emitAuctionResult(event: AuctionResultEvent)
}

/** No-op for tests that don't care about emission. */
object NoOpEventEmitter : EventEmitter {
    override fun emitImpression(event: ImpressionEvent) {}

    override fun emitAuctionResult(event: AuctionResultEvent) {}
}

/**
 * Fire-and-forget Kafka implementation. The request path calls [emitImpression] /
 * [emitAuctionResult] and returns immediately. Errors land in the producer callback but never
 * propagate up to the request handler.
 */
class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) : EventEmitter, AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun emitImpression(event: ImpressionEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicImpressionEvents, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicImpressionEvents, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicImpressionEvents, e.message)
            }
        }
    }

    override fun emitAuctionResult(event: AuctionResultEvent) {
        emitterScope.launch {
            try {
                producer.send(
                    ProducerRecord(config.topicAuctionResults, event.userId.toString(), event),
                ) { _, ex ->
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", config.topicAuctionResults, ex.message)
                }
            } catch (e: Throwable) {
                log.warn("kafka.emit.fail topic={} error={}", config.topicAuctionResults, e.message)
            }
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}
