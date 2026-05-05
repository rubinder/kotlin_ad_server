plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.ktlint) apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = rootProject.libs.plugins.ktlint.get().pluginId)

    // Force vanilla Apache Kafka client. Confluent's kafka-avro-serializer / schema-registry-client
    // (and Flink's Kafka connector) transitively pull org.apache.kafka:kafka-clients:7.7.0-ccs (the
    // Confluent-patched fork) from Confluent's maven repo. Our settings.gradle.kts content filter
    // doesn't allow org.apache.kafka from Confluent, so without this force the build fails.
    configurations.all {
        resolutionStrategy.force("org.apache.kafka:kafka-clients:${rootProject.libs.versions.kafka.clients.get()}")
    }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(21)
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            freeCompilerArgs.addAll(
                "-Xjsr305=strict",
                "-opt-in=kotlin.RequiresOptIn",
            )
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
        }
    }
}
