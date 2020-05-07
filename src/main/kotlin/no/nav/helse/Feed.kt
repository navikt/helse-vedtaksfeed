package no.nav.helse

import java.time.LocalDate
import java.time.LocalDateTime

class Feed(
    val tittel: String,
    val inneholderFlereElementer: Boolean,
    val elementer: List<Element>
) {
    class Element(
        val type: String,
        val sekvensId: Long,
        val innhold: Innhold,
        val metadata: Metadata
    ) {
        class Innhold(
            val aktoerId: String,
            val foersteStoenadsdag: LocalDate,
            val sisteStoenadsdag: LocalDate,
            val utbetalingsreferanse: String,
            val forbrukteStoenadsdager: Int
        )

        class Metadata(
            val opprettetDato: LocalDate
        )
    }
}

class Vedtak(
    val type: Vedtakstype,
    val opprettet: LocalDateTime,
    val aktørId: String,
    val fødselsnummer: String,
    val førsteStønadsdag: LocalDate,
    val sisteStønadsdag: LocalDate,
    val førsteFraværsdag: String,
    val forbrukteStønadsdager: Int
) {
    enum class Vedtakstype {
        SykepengerUtbetalt_v1, SykepengerAnnullert_v1
    }
}
