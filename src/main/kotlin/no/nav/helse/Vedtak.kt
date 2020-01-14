package no.nav.helse

import java.time.LocalDate

// TODO: På sikt må vi plukke ut første og siste utbetalingsdag fra hele vedtaksperioden slik at vi kan sette riktig
//  første og siste dag i vedtaket som infotrygd leser
data class Vedtak(
    val aktørId: String,
    val fødselsnummer: String,
    val utbetalingsreferanse: String,
    val utbetalingslinjer: List<Utbetalingslinje>,
    val opprettet: LocalDate
)

data class Utbetalingslinje(
    val fom: LocalDate,
    val tom: LocalDate,
    val dagsats: Int
)
