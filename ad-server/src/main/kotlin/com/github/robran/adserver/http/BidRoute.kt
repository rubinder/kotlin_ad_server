package com.github.robran.adserver.http

import com.github.robran.adserver.auction.AuctionPipeline
import com.github.robran.adserver.protocol.openrtb.BidRequest
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.post

fun Route.bidRoutes(pipeline: AuctionPipeline) {
    post("/openrtb/bid") {
        val req = call.receive<BidRequest>()
        val resp = pipeline.runAuction(req)
        call.respond(HttpStatusCode.OK, resp)
    }
}
