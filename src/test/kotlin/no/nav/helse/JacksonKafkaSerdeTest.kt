package no.nav.helse

import no.nav.helse.Vedtak.Vedtakstype.SykepengerAnnullert_v1
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime

internal class JacksonKafkaSerdeTest {
    private val vedtak =
        Vedtak(
            type = SykepengerAnnullert_v1,
            opprettet = LocalDateTime.parse("2026-01-02T03:04:05.123456"),
            fødselsnummer = "12345678910",
            førsteStønadsdag = LocalDate.parse("2026-01-01"),
            sisteStønadsdag = LocalDate.parse("2026-01-31"),
            førsteFraværsdag = "PMHCWCIRABDZBEHFPMYNPJEKCM",
            forbrukteStønadsdager = 5_010,
        )

    @Language("JSON")
    private val forventetJson = """{"type":"SykepengerAnnullert_v1","opprettet":"2026-01-02T03:04:05.123456","fødselsnummer":"12345678910","førsteStønadsdag":"2026-01-01","sisteStønadsdag":"2026-01-31","førsteFraværsdag":"PMHCWCIRABDZBEHFPMYNPJEKCM","forbrukteStønadsdager":5010}"""

    @Test
    fun `serialiserer vedtak med samme format som før`() {
        assertEquals(forventetJson, VedtakSerializer().serialize("topic", vedtak).toString(Charsets.UTF_8))
    }

    @Test
    fun `deserialiserer vedtak med æøå i feltnavn`() {
        val lest = VedtakDeserializer().deserialize("topic", forventetJson.toByteArray(Charsets.UTF_8))
        assertEquals(vedtak.type, lest.type)
        assertEquals(vedtak.opprettet, lest.opprettet)
        assertEquals(vedtak.fødselsnummer, lest.fødselsnummer)
        assertEquals(vedtak.førsteStønadsdag, lest.førsteStønadsdag)
        assertEquals(vedtak.sisteStønadsdag, lest.sisteStønadsdag)
        assertEquals(vedtak.førsteFraværsdag, lest.førsteFraværsdag)
        assertEquals(vedtak.forbrukteStønadsdager, lest.forbrukteStønadsdager)
    }
}
