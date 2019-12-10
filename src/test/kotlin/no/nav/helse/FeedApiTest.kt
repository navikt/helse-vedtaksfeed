package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.routing.routing
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.withTestApplication
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertFalse

internal class FeedApiTest {

    @Test
    internal fun `feed returnerer ingen elementer`() {
        withTestApplication({
            routing {
                feedApi(mockk() {
                    every { hentVedtak(any(), any()) } returns emptyList()
                })
            }
        }) {
            with(handleRequest(HttpMethod.Get, "/feed")) {
                assertEquals(HttpStatusCode.OK, response.status())
                assertFalse(response.content.isNullOrBlank())
            }
        }
    }

    @Test
    internal fun `feed returnerer ett element uten utbetalingslinjer`() {
        withTestApplication({
            routing {
                feedApi(mockk() {
                    every { hentVedtak(any(), any()) } returns listOf(
                        Vedtak(
                            "aktørId",
                            "utbetalingsreferanse",
                            emptyList(),
                            LocalDate.of(2019, 12, 10)
                        )
                    )
                })
            }
        }) {
            assertThrows<IllegalStateException> {
                handleRequest(HttpMethod.Get, "/feed")
            }
        }
    }

    @Test
    internal fun `feed returnerer ett element med utbetalingslinje`() {
        withTestApplication({
            routing {
                feedApi(mockk() {
                    every { hentVedtak(any(), any()) } returns listOf(
                        Vedtak(
                            "aktørId",
                            "utbetalingsreferanse",
                            listOf(
                                Utbetalingslinje(
                                    LocalDate.of(2019, 12, 1),
                                    LocalDate.of(2019, 12, 10),
                                    42
                                )
                            ),
                            LocalDate.of(2019, 12, 10)
                        )
                    )
                })
            }
        }) {
            with(handleRequest(HttpMethod.Get, "/feed")) {
                assertEquals(HttpStatusCode.OK, response.status())
                assertFalse(response.content.isNullOrBlank())

                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(LocalDate.of(2019,12,1), feed.elementer.first().innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019,12,10), feed.elementer.first().innhold.sisteStoenadsdag)
            }
        }
    }
}
