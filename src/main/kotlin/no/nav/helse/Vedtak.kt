package no.nav.helse

import java.time.LocalDate

data class Vedtak(
    val aktørId: String,
    val fødselsnummer: String,
    val utbetalingsreferanse: String,
    val utbetalingslinjer: List<Utbetalingslinje>,
    val opprettet: LocalDate,
    val forbrukteSykedager: Int = 999 // Avtalt defaultverdi med IT-gjengen for vedtak fra før vi fikk på plass verdien
)

data class Utbetalingslinje(
    val fom: LocalDate,
    val tom: LocalDate,
    val dagsats: Int
)
