package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import java.time.LocalDate
import kotlin.test.assertEquals
import no.nav.helse.Vedtak.Vedtakstype.SykepengerUtbetalt_v1
import org.intellij.lang.annotations.Language
import org.junit.Test

class FjerneGuleLinjerTest {
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
    fun `revurdering som trekker tilbake penger i snute og hale - men magen er fortsatt utbetalt`() {
        rapid.sendTestMessage(utbetalingUtbetalt)

        assertEquals(1, interneVedtak.size)
        with(interneVedtak[0]) {
            assertEquals(SykepengerUtbetalt_v1, type)
            assertEquals(LocalDate.parse("2018-01-17"), førsteStønadsdag)
            assertEquals(LocalDate.parse("2018-01-31"), sisteStønadsdag)
        }

        rapid.sendTestMessage(utbetalingUtbetaltUtenSnuteOgHale)
        assertEquals(2, interneVedtak.size)
        with(interneVedtak[1]) {
            assertEquals(SykepengerUtbetalt_v1, type)
            // Sånn er det nå!
            assertEquals(LocalDate.parse("2018-01-17"), førsteStønadsdag)
            assertEquals(LocalDate.parse("2018-01-31"), sisteStønadsdag)

            // Sånn burde det vært?
            //assertEquals(LocalDate.parse("2018-01-21"), førsteStønadsdag)
            //assertEquals(LocalDate.parse("2018-01-25"), sisteStønadsdag)
        }
    }

    @Test
    fun `revurdering som trekker tilbake alle pengene for perioden`() {
        rapid.sendTestMessage(utbetalingUtbetalt)

        assertEquals(1, interneVedtak.size)
        with(interneVedtak[0]) {
            assertEquals(SykepengerUtbetalt_v1, type)
            assertEquals(LocalDate.parse("2018-01-17"), førsteStønadsdag)
            assertEquals(LocalDate.parse("2018-01-31"), sisteStønadsdag)
        }

        rapid.sendTestMessage(utbetalingUtbetaltSomTrekkerTilbakeAllePengene)
        assertEquals(2, interneVedtak.size)
        with(interneVedtak[1]) {
            assertEquals(LocalDate.parse("2018-01-17"), førsteStønadsdag)
            assertEquals(LocalDate.parse("2018-01-31"), sisteStønadsdag)

            // Sånn er det nå!
            assertEquals(SykepengerUtbetalt_v1, type)
            // Sånn burde det vært?
            //assertEquals(SykepengerAnnullert_v1, type)
        }
    }

    @Language("JSON")
    private val utbetalingUtbetalt = """
    {
      "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
      "type": "UTBETALING",
      "gjenståendeSykedager": 136,
      "stønadsdager": 26,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "27a641a5-2a0d-4980-8899-aff768a5e600",
      "@event_name": "utbetaling_utbetalt",
      "fødselsnummer": "11111100000",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
              "fom": "2018-01-17",
              "tom": "2018-01-31"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
  }
"""

    @Language("JSON")
    private val utbetalingUtbetaltUtenSnuteOgHale = """
    {
      "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
      "type": "REVURDERING",
      "gjenståendeSykedager": 136,
      "stønadsdager": 26,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "27a641a5-2a0d-4980-8899-aff768a5e600",
      "@event_name": "utbetaling_utbetalt",
      "fødselsnummer": "11111100000",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
              "fom": "2018-01-17",
              "tom": "2018-01-20",
              "statuskode": "OPPH"
          },
          {
              "fom": "2018-01-21",
              "tom": "2018-01-25"
          },
          {
              "fom": "2018-01-26",
              "tom": "2018-01-31",
              "statuskode": "OPPH"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
  }
"""

    @Language("JSON")
    private val utbetalingUtbetaltSomTrekkerTilbakeAllePengene = """
    {
      "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
      "type": "REVURDERING",
      "gjenståendeSykedager": 136,
      "stønadsdager": 26,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "27a641a5-2a0d-4980-8899-aff768a5e600",
      "@event_name": "utbetaling_utbetalt",
      "fødselsnummer": "11111100000",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
              "fom": "2018-01-17",
              "tom": "2018-01-31",
              "statuskode": "OPPH"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
  }
"""
}

