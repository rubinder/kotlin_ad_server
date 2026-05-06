plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.gatling)
}

dependencies {
    // The gatling plugin sets up its own classpath; we add nothing here for the simulations.
    // BidRequest construction is done with hand-written JSON strings in the simulations to keep
    // this module self-contained and not pull in common-protocol.
    gatlingImplementation(libs.gatling.test.framework)
    gatlingImplementation(libs.gatling.highcharts)
}
