package no.nav.helse

import java.time.LocalDate

sealed class Vedtak2(
    val aktørId: String,
    val fødselsnummer: String,
    val førsteFraværsdag: LocalDate,
    val opprettet: LocalDate,
    val forbrukteSykedager: Int = 999 // Avtalt defaultverdi med IT-gjengen for vedtak fra før vi fikk på plass verdien
) {
    class VedtakV1(
        aktørId: String,
        fødselsnummer: String,
        førsteFraværsdag: LocalDate,
        opprettet: LocalDate,
        val utbetaling: List<Utbetalingslinjer>
    ): Vedtak2(aktørId, fødselsnummer, førsteFraværsdag, opprettet)

    class VedtakV2(
        aktørId: String,
        fødselsnummer: String,
        førsteFraværsdag: LocalDate,
        opprettet: LocalDate,
        val utbetalingslinjer: List<Utbetalingslinje>
    ): Vedtak2(aktørId, fødselsnummer, førsteFraværsdag, opprettet)
}

data class Utbetalingslinjer(
    val utbetalingsreferanse: String,
    val utbetalingslinjer: List<Utbetalingslinje>
)

data class Utbetalingslinje(
    val fom: LocalDate,
    val tom: LocalDate,
    val dagsats: Int
)
