package com.github.robran.adserver.http

import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Liveness flips to true once the process is up. Readiness flips to true after inventory hydration.
 */
class HealthState {
    val ready = AtomicBoolean(false)
}

fun Route.healthRoutes(state: HealthState) {
    get("/healthz") {
        call.respondText("ok", status = HttpStatusCode.OK)
    }
    get("/readyz") {
        if (state.ready.get()) {
            call.respondText("ready", status = HttpStatusCode.OK)
        } else {
            call.respondText("not ready", status = HttpStatusCode.ServiceUnavailable)
        }
    }
}
