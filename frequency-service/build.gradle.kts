plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

application {
    mainClass.set("com.github.robran.adserver.frequency.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))

    // gRPC server (only the netty-shaded transport — keeps the deployment JAR self-contained)
    implementation(libs.grpc.netty.shaded)

    // Redis: Lettuce reactive + coroutine bridge
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.reactor)

    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)

    // Phase 4b: OpenTelemetry tracing
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.instrumentation.bom.alpha))
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.context)
    implementation(libs.opentelemetry.grpc.instrumentation)
    implementation(libs.opentelemetry.logback.mdc.instrumentation)
    implementation(libs.logstash.logback.encoder)

    // Config + logging
    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.grpc.inprocess)
}
