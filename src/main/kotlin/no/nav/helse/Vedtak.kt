package no.nav.helse

import java.time.LocalDate

data class Vedtak(
    val aktørId: String,
    val utbetalingsreferanse: String,
    val utbetalingslinjer: List<Utbetalingslinje>,
    val opprettet: LocalDate
)

data class Utbetalingslinje(
    val fom: LocalDate,
    val tom: LocalDate,
    val dagsats: Int
)
