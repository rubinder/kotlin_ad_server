package com.github.robran.adserver.auction

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.containsAll
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import com.github.robran.adserver.auction.stages.BlockingPolicyStage
import com.github.robran.adserver.auction.stages.FloorPriceStage
import com.github.robran.adserver.auction.stages.FrequencyAndCompsepStage
import com.github.robran.adserver.auction.stages.SelectionStage
import com.github.robran.adserver.inventory.Campaign
import com.github.robran.adserver.inventory.Creative
import com.github.robran.adserver.inventory.InventorySnapshot
import com.github.robran.adserver.kafka.NoOpEventEmitter
import com.github.robran.adserver.protocol.openrtb.Banner
import com.github.robran.adserver.protocol.openrtb.BidRequest
import com.github.robran.adserver.protocol.openrtb.Imp
import com.github.robran.adserver.protocol.openrtb.User
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.random.Random

class AuctionPipelineTracingTest {

    private fun campaign(id: String, bid: Double): Campaign {
        val creative = Creative(id = "$id-cre", campaignId = id, width = 300, height = 250, markup = "<m>")
        return Campaign(
            id = id,
            advertiserId = "adv",
            advertiserDomain = "x.example.com",
            category = "IAB13",
            bidPrice = bid,
            frequencyCap = 5,
            active = true,
            creatives = listOf(creative),
        )
    }

    private fun req(): BidRequest = BidRequest(
        id = "req-trace",
        imp = listOf(Imp(id = "1", banner = Banner(300, 250))),
        user = User(id = "u-trace"),
    )

    private fun otelWithExporter(exporter: InMemorySpanExporter): OpenTelemetry {
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build()
        return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build()
    }

    @Test
    fun `runAuction emits adserver_request span with user_id and outcome attributes`() = runTest {
        val exporter = InMemorySpanExporter.create()
        val otel = otelWithExporter(exporter)
        val s = InventorySnapshot(listOf(campaign("c1", 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            openTelemetry = otel,
        )
        pipe.runAuction(req())

        val spans = exporter.finishedSpanItems
        val rootSpans = spans.filter { it.name == "adserver.request" }
        assertThat(rootSpans).hasSize(1)
        val rootSpan = rootSpans[0]
        assertThat(rootSpan.attributes.asMap().keys.map { it.key })
            .containsAll("user.id", "imp.id", "slot.size", "request.id", "outcome")
        assertThat(
            rootSpan.attributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("outcome")),
        ).isEqualTo("filled")
    }

    @Test
    fun `runAuction emits a child span for every rule stage`() = runTest {
        val exporter = InMemorySpanExporter.create()
        val otel = otelWithExporter(exporter)
        val s = InventorySnapshot(listOf(campaign("c1", 2.0)), Instant.now())
        val pipe = AuctionPipeline(
            candidateBuilder = CandidateBuilder(s),
            stages = listOf(
                BlockingPolicyStage(),
                FrequencyAndCompsepStage(FakeFrequencyClient()),
                FloorPriceStage(),
                SelectionStage(Random(42)),
            ),
            eventEmitter = NoOpEventEmitter,
            openTelemetry = otel,
        )
        pipe.runAuction(req())

        val spanNames = exporter.finishedSpanItems.map { it.name }.toSet()
        assertThat(spanNames).contains("rule.blocking")
        assertThat(spanNames).contains("rule.freq+compsep")
        assertThat(spanNames).contains("rule.floor")
        assertThat(spanNames).contains("rule.selection")
    }
}
