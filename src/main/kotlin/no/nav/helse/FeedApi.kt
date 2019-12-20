package no.nav.helse

import io.ktor.response.respondBytes
import io.ktor.routing.Route
import io.ktor.routing.get
import java.lang.IllegalArgumentException

internal fun Route.feedApi(konsument: Vedtakskonsument) {
    get("/feed") {
        val maxAntall = this.context.parameters["maxAntall"]?.toInt() ?: 100
        val sekvensNr = this.context.parameters["sekvensNr"]?.toInt() ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")
        context.respondBytes(feed(konsument, maxAntall, sekvensNr).json())
    }
}

private fun Feed.json() = objectMapper.writeValueAsBytes(this)

private fun feed(konsument: Vedtakskonsument, maxAntall: Int, sekvensNr: Int): Feed {

    val vedtak = konsument.hentVedtak(maxAntall, sekvensNr)
    return Feed("", vedtak.isNotEmpty(), vedtak.map { it.toFeedElement() })
}


