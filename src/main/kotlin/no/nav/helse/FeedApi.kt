package no.nav.helse

import io.ktor.response.respondBytes
import io.ktor.routing.Route
import io.ktor.routing.get

internal fun Route.feedApi(konsument: Vedtakskonsument) {
    get("/feed") {
        val maxAntall = this.context.parameters["maxAntall"]?.toInt() ?: 100
        context.respondBytes(feed(konsument, maxAntall).json())
    }
}

private fun Feed.json() = objectMapper.writeValueAsBytes(this)

private fun feed(konsument: Vedtakskonsument, maxAntall: Int): Feed {
    val vedtak = konsument.hentVedtak(maxAntall, 100)
    return Feed("", vedtak.isNotEmpty(), vedtak.map { it.toFeedElement() })
}
