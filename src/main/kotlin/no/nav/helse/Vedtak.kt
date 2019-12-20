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

fun Vedtak.toFeedElement() =
    FeedElement(
        "SykepengerInnvilget_v1",
        0,
        FeedElementInnhold(
            aktørId,
            utbetalingslinjer.map { it.fom }.min() ?: error("Mangler utbetalingslinjer"),
            utbetalingslinjer.map { it.tom }.max() ?: error("Mangler utbetalingslinjer"),
            utbetalingsreferanse
        ),
        FeedElementMetadata(opprettet)
    )

/*
{
  "tittel": "string",
  "inneholderFlereElementer": true,
  "elementer": [
    {
      "type": "string",
      "sekvensId": 0,
      "innhold": {},
      "metadata": {}
    }
  ]
}
 */
