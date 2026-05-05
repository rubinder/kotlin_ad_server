rootProject.name = "kotlin_ad_server"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        maven {
            name = "Confluent"
            url = uri("https://packages.confluent.io/maven/")
            content {
                includeGroup("io.confluent")
                // The Avro confluent registry connector is also published here.
                includeGroup("org.apache.flink")
            }
        }
    }
}

include(
    "common-protocol",
    "inventory-loader",
    "ad-server",
    "frequency-service",
)
