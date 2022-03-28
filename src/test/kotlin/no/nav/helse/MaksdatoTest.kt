package no.nav.helse

import no.nav.helse.Vedtak.Vedtakstype.SykepengerUtbetalt_v1
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate

class MaksdatoTest {

    private val interneVedtak = mutableListOf<Vedtak>()
    private val rapid: TestRapid

    init {
        val publisher: (String, Vedtak) -> Long = { _, vedtak -> interneVedtak.add(vedtak); 42 }
         rapid = TestRapid().apply {
            UtbetalingUtbetaltRiver(this, publisher)
            AnnullertRiverV1(this, publisher)
        }
        aktiverMarkeringAvUtbetaltTilMaksdato = true
    }

    @Test
    fun `legger på 30 000 på forbrukte dager når det ble uttalt til maksdato`() {
        rapid.sendTestMessage(utbetalingUtbetaltUtenGjenståendeDager)
        assertEquals(30033, interneVedtak[0].forbrukteStønadsdager)
    }

    @Test
    fun `sender forbrukte dager urørt når det ikke ble uttalt til maksdato`() {
        rapid.sendTestMessage(utbetalingUtbetalt)
        assertEquals(26, interneVedtak[0].forbrukteStønadsdager)
    }

    @Test
    fun `setter TOM til maksdato hvis denne nås i perioden`() {
        rapid.sendTestMessage(utbetalingUtbetaltUtenGjenståendeDager)
        val vedtak = interneVedtak[0]
        assertEquals(SykepengerUtbetalt_v1, vedtak.type)
        assertEquals(LocalDate.of(2020, 8, 9), vedtak.førsteStønadsdag)
        assertEquals(LocalDate.of(2020, 8, 20), vedtak.sisteStønadsdag)
        assertEquals("XWZFMAISNZFMFO33LGFZ5XMDQY", vedtak.førsteFraværsdag)
        assertEquals(LocalDate.of(2020, 12, 14), vedtak.opprettet.toLocalDate())
    }
}

@Language("JSON")
private val utbetalingUtbetalt = """
    {
      "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
      "type": "UTBETALING",
      "fom": "2020-08-09",
      "tom": "2020-08-24",
      "maksdato": "2020-12-20",
      "gjenståendeSykedager": 136,
      "stønadsdager": 26,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "27a641a5-2a0d-4980-8899-aff768a5e600",
      "@event_name": "utbetaling_utbetalt",
      "@id": "d65f35dc-df67-4143-923f-d005075b0ee3",
      "@opprettet": "2020-12-14T15:38:14.419655",
      "aktørId": "1111110000000",
      "fødselsnummer": "11111100000",
      "organisasjonsnummer": "999999999"
  }
"""


@Language("JSON")
private val utbetalingUtbetaltUtenGjenståendeDager = """
    {
      "utbetalingId": "eca9f8dd-eff6-4a8e-8624-4ad802256da1",
      "type": "UTBETALING",
      "fom": "2020-08-09",
      "tom": "2020-08-24",
      "stønadsdager": 33,
      "maksdato": "2020-08-20",
      "gjenståendeSykedager": 0,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "bdb25601-126e-4ac2-bb7b-598b9edd8386",
      "@event_name": "utbetaling_utbetalt",
      "@id": "485e1fc9-b825-4089-874f-71278c93dce9",
      "@opprettet": "2020-12-14T15:38:14.419655",
      "aktørId": "1111110000000",
      "fødselsnummer": "11111100000",
      "organisasjonsnummer": "999999999"
  }
"""

