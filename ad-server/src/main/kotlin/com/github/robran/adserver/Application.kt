package com.github.robran.adserver

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.CandidateBuilder
import com.github.robran.adserver.auction.FrequencyClient
import com.github.robran.adserver.auction.GrpcFrequencyClient
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.http.HealthState
import com.github.robran.adserver.http.bidRoutes
import com.github.robran.adserver.http.healthRoutes
import com.github.robran.adserver.http.metricsRoutes
import com.github.robran.adserver.metrics.MeterRegistryFactory
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.random.Random

private val log = LoggerFactory.getLogger("com.github.robran.adserver.Application")

fun main() {
    val config = AppConfig.load()

    val snapshot =
        InventoryLoader.pooledDataSource(
            config.inventory.jdbcUrl,
            config.inventory.user,
            config.inventory.password,
        ).use { ds ->
            if (!config.inventory.skipMigrate) {
                InventoryLoader.migrate(
                    config.inventory.jdbcUrl,
                    config.inventory.user,
                    config.inventory.password,
                )
            }
            InventoryLoader(ds).load()
        }

    val meterRegistry = MeterRegistryFactory.build(config.metrics)

    val frequencyChannel =
        NettyChannelBuilder
            .forAddress(config.frequency.host, config.frequency.port)
            .usePlaintext()
            .build()
    val frequencyClient = GrpcFrequencyClient(frequencyChannel, timeoutMs = config.frequency.timeoutMs)
    val kafkaProducer = com.github.robran.adserver.kafka.ProducerFactory.avroProducer(config.kafka)
    val eventEmitter = com.github.robran.adserver.kafka.KafkaEventEmitter(kafkaProducer, config.kafka)
    val pipeline = buildPipeline(snapshot, frequencyClient, eventEmitter)

    log.info(
        "ad-server starting: {} campaigns loaded, frequency-service @ {}:{}",
        snapshot.size,
        config.frequency.host,
        config.frequency.port,
    )

    val healthState = HealthState().apply { ready.set(true) }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("Shutting down ad-server")
            eventEmitter.close()
            frequencyChannel.shutdown()
            meterRegistry.close()
        },
    )

    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, pipeline, meterRegistry)
    }.start(wait = true)
}

/**
 * Builds the auction pipeline with a caller-supplied [FrequencyClient].
 * Production wires a [GrpcFrequencyClient]; tests wire a fake.
 */
fun buildPipeline(
    snapshot: InventorySnapshot,
    frequencyClient: FrequencyClient,
    eventEmitter: com.github.robran.adserver.kafka.EventEmitter = com.github.robran.adserver.kafka.NoOpEventEmitter,
): AuctionPipeline =
    AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages =
            listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(frequencyClient),
                FloorPriceStage(),
                SelectionStage(Random.Default),
            ),
        eventEmitter = eventEmitter,
    )

fun Application.adServerModule(
    healthState: HealthState,
    pipeline: AuctionPipeline,
    meterRegistry: PrometheusMeterRegistry,
) {
    install(ContentNegotiation) {
        json(
            Json {
                ignoreUnknownKeys = true
                encodeDefaults = false
                explicitNulls = false
            },
        )
    }
    install(CallLogging)
    install(MicrometerMetrics) {
        registry = meterRegistry
    }
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(
                io.ktor.http.HttpStatusCode.BadRequest,
                mapOf("error" to "invalid_request", "message" to (cause.message ?: "bad request")),
            )
        }
    }

    routing {
        healthRoutes(healthState)
        bidRoutes(pipeline)
        metricsRoutes(meterRegistry)
    }
}
