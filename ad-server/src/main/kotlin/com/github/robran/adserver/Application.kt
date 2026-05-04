package com.github.robran.adserver

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.auction.CandidateBuilder
import com.github.robran.adserver.auction.FakeFrequencyClient
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.http.HealthState
import com.github.robran.adserver.http.bidRoutes
import com.github.robran.adserver.http.healthRoutes
import com.github.robran.adserver.inventory.InventoryLoader
import com.github.robran.adserver.inventory.InventorySnapshot
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.random.Random

private val log = LoggerFactory.getLogger("com.github.robran.adserver.Application")

fun main() {
    val config = AppConfig.load()

    if (!config.inventory.skipMigrate) {
        InventoryLoader.migrate(
            config.inventory.jdbcUrl,
            config.inventory.user,
            config.inventory.password,
        )
    }
    val ds =
        InventoryLoader.pooledDataSource(
            config.inventory.jdbcUrl,
            config.inventory.user,
            config.inventory.password,
        )
    val snapshot = InventoryLoader(ds).load()
    val pipeline = buildPipeline(snapshot)
    log.info("ad-server starting: {} campaigns loaded", snapshot.size)

    val healthState = HealthState().apply { ready.set(true) }

    embeddedServer(Netty, host = config.server.host, port = config.server.port) {
        adServerModule(healthState, pipeline)
    }.start(wait = true)
}

/**
 * Builds the auction pipeline. Phase 1 uses [FakeFrequencyClient] (always returns empty);
 * Phase 2 swaps it for a real gRPC client.
 */
fun buildPipeline(snapshot: InventorySnapshot): AuctionPipeline =
    AuctionPipeline(
        candidateBuilder = CandidateBuilder(snapshot),
        stages =
            listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random.Default),
            ),
    )

fun Application.adServerModule(
    healthState: HealthState,
    pipeline: AuctionPipeline,
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

    routing {
        healthRoutes(healthState)
        bidRoutes(pipeline)
    }
}
