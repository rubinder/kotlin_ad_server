plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.ktor)
    application
}

application {
    mainClass.set("com.github.robran.adserver.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))
    implementation(project(":inventory-loader"))

    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.status.pages)
    implementation(libs.ktor.server.call.logging)

    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation(libs.logstash.logback.encoder)
    implementation(libs.postgres.jdbc)
    implementation(libs.hikaricp)

    // Phase 2: gRPC client to frequency-service
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.kotlin.stub)

    // Phase 3: Kafka producer + Avro
    implementation(libs.kafka.clients)
    implementation(libs.confluent.kafka.avro.serializer)
    implementation(libs.confluent.kafka.schema.registry.client)

    // Phase 4a: Micrometer metrics
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.ktor.server.metrics.micrometer)

    // Phase 4b: OpenTelemetry tracing
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.instrumentation.bom.alpha))
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.context)
    implementation("io.opentelemetry.instrumentation:opentelemetry-grpc-1.6")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.content.negotiation)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgres)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.grpc.inprocess)
    testImplementation(project(":frequency-service"))
    testImplementation(project(":flink-impression-aggregator"))
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.flink.streaming.java)
    testImplementation(libs.flink.clients)
    testImplementation(libs.flink.connector.kafka)
    testImplementation(libs.flink.avro)
    testImplementation(libs.flink.avro.confluent.registry)
    testImplementation(libs.flink.test.utils)
    testImplementation(libs.flink.connector.base)
    testImplementation(libs.lettuce.core)
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
}

tasks.withType<Test> {
    // Flink 1.20 + Java 17/21: Kryo's chill package needs reflective access to java.util internals.
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}
