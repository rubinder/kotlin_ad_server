plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

application {
    mainClass.set("com.github.robran.adserver.flink.ApplicationKt")
}

dependencies {
    implementation(project(":common-protocol"))

    // Flink core + Kafka connector + Avro Schema Registry support
    implementation(libs.flink.streaming.java)
    implementation(libs.flink.clients)
    implementation(libs.flink.connector.kafka)
    implementation(libs.flink.avro)
    implementation(libs.flink.avro.confluent.registry)
    implementation(libs.kafka.clients)
    implementation(libs.confluent.kafka.avro.serializer)

    // Redis sink uses Lettuce (already in catalog)
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.config4k)
    implementation(libs.logback.classic)
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.assertk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.flink.test.utils)
    testImplementation(libs.flink.connector.base)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
}

tasks.withType<Test> {
    // Flink 1.20 + Java 17: Kryo's chill package needs reflective access to java.util internals.
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}
