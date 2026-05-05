package com.github.robran.adserver.kafka

import com.github.robran.adserver.KafkaConfig
import com.github.robran.adserver.protocol.events.AuctionResultEvent
import com.github.robran.adserver.protocol.events.ImpressionEvent
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

interface EventEmitter {
    fun emitImpression(event: ImpressionEvent)

    fun emitAuctionResult(event: AuctionResultEvent)
}

object NoOpEventEmitter : EventEmitter {
    override fun emitImpression(event: ImpressionEvent) {}

    override fun emitAuctionResult(event: AuctionResultEvent) {}
}

class KafkaEventEmitter(
    private val producer: Producer<String, Any>,
    private val config: KafkaConfig,
    private val emitterScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : EventEmitter, AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)
    private val impressionSendTimer: Timer = sendTimer(meterRegistry, config.topicImpressionEvents)
    private val auctionSendTimer: Timer = sendTimer(meterRegistry, config.topicAuctionResults)

    override fun emitImpression(event: ImpressionEvent) {
        send(impressionSendTimer, config.topicImpressionEvents, event.userId.toString(), event)
    }

    override fun emitAuctionResult(event: AuctionResultEvent) {
        send(auctionSendTimer, config.topicAuctionResults, event.userId.toString(), event)
    }

    private fun <T : Any> send(
        timer: Timer,
        topic: String,
        key: String,
        value: T,
    ) {
        val startNanos = System.nanoTime()
        emitterScope.launch {
            try {
                producer.send(ProducerRecord(topic, key, value)) { _, ex ->
                    val elapsed = System.nanoTime() - startNanos
                    timer.record(elapsed, TimeUnit.NANOSECONDS)
                    if (ex != null) log.warn("kafka.send.fail topic={} error={}", topic, ex.message)
                }
            } catch (e: Throwable) {
                val elapsed = System.nanoTime() - startNanos
                timer.record(elapsed, TimeUnit.NANOSECONDS)
                log.warn("kafka.emit.fail topic={} error={}", topic, e.message)
            }
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }

    companion object {
        const val METRIC_NAME = "kafka.producer.send.duration"

        private fun sendTimer(
            registry: MeterRegistry,
            topic: String,
        ): Timer =
            Timer.builder(METRIC_NAME)
                .tag("topic", topic)
                .publishPercentileHistogram()
                .register(registry)
    }
}
