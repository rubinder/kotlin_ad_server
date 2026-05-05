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

/**
 * Fire-and-forget event emitter. The request path calls [emitImpression] / [emitAuctionResult]
 * and returns immediately. Producer.send() is non-blocking by default (returns a Future and
 * batches behind the scenes); we don't even await the Future. Errors land in the producer
 * callback but never propagate up to the request handler.
 */
class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(javaClass)

    fun emitImpression(event: ImpressionEvent) {
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

    fun emitAuctionResult(event: AuctionResultEvent) {
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
