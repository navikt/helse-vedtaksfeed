package no.nav.helse

import java.time.LocalDate

data class Feed(
    val tittel: String,
    val inneholderFlereElementer: Boolean,
    val elementer: List<FeedElement>
)

data class FeedElement(
    val type: String,
    val sekvensId: Long,
    val innhold: FeedElementInnhold,
    val metadata: FeedElementMetadata
)

data class FeedElementInnhold(
    val aktoerId: String,
    val foersteStoenadsdag: LocalDate,
    val sisteStoenadsdag: LocalDate,
    val utbetalingsreferanse: String,
    val forbrukteStoenadsdager: Int
)

data class FeedElementMetadata(
    val opprettetDato: LocalDate
)
