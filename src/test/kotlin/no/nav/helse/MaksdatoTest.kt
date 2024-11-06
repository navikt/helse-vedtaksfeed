package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class MaksdatoTest {

    private val interneVedtak = mutableListOf<Vedtak>()
    private val rapid: TestRapid

    init {
        val publisher: (String, Vedtak) -> Long = { _, vedtak -> interneVedtak.add(vedtak); 42 }
         rapid = TestRapid().apply {
            UtbetalingUtbetaltRiver(this, publisher)
            AnnullertRiverV1(this, publisher)
        }
    }

    @Test
    fun `legger på 5 000 på forbrukte dager når det ble uttalt til maksdato`() {
        rapid.sendTestMessage(utbetalingUtbetaltUtenGjenståendeDager)
        assertEquals(5033, interneVedtak[0].forbrukteStønadsdager)
    }

    @Test
    fun `sender forbrukte dager urørt når det ikke ble uttalt til maksdato`() {
        rapid.sendTestMessage(utbetalingUtbetalt)
        assertEquals(26, interneVedtak[0].forbrukteStønadsdager)
    }

    @Test
    fun `Vi skal hente første dag fra en linje som ikke er opphørt`() {
        rapid.sendTestMessage(utbetalingMedOpphørFørst)
        assertEquals(LocalDate.parse("2023-10-12"), interneVedtak[0].førsteStønadsdag)
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
      "organisasjonsnummer": "999999999",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
            "fom": "2020-08-09",
            "tom": "2020-08-24"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
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
      "organisasjonsnummer": "999999999",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
            "fom": "2020-08-09",
            "tom": "2020-08-24"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
  }
"""


@Language("JSON")
val utbetalingMedOpphørFørst = """
    {
      "@event_name": "utbetaling_utbetalt",
      "organisasjonsnummer": "111111111",
      "utbetalingId": "${UUID.randomUUID()}",
      "korrelasjonsId": "${UUID.randomUUID()}",
      "type": "REVURDERING",
      "fom": "2023-09-13",
      "tom": "2023-10-18",
      "maksdato": "2024-12-31",
      "forbrukteSykedager": 5,
      "gjenståendeSykedager": 243,
      "stønadsdager": 12,
      "tidspunkt": "${LocalDateTime.now()}",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
            "fom": "2023-10-19",
            "tom": "2023-10-27",
            "statuskode": "OPPH"
          },
          {
            "fom": "2023-10-12",
            "tom": "2023-10-18",
            "statuskode": null
          },
          {
            "fom": "2023-10-19",
            "tom": "2023-10-27",
            "statuskode": null
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      },
      "utbetalingsdager": [],
      "@id": "${UUID.randomUUID()}",
      "@opprettet": "2020-01-01T01:01:01.000000000",
      "aktørId": "1111110000000",
      "fødselsnummer": "11111100000"
    }
""".trimIndent()
