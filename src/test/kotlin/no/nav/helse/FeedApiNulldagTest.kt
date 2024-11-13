package no.nav.helse

import com.github.navikt.tbd_libs.naisful.test.TestContext
import com.github.navikt.tbd_libs.naisful.test.naisfulTestApp
import com.github.navikt.tbd_libs.result_object.ok
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.navikt.tbd_libs.test_support.kafkaTest
import io.ktor.client.call.body
import io.ktor.client.request.*
import io.ktor.server.routing.routing
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.Properties
import kotlin.collections.set

internal class FeedApiNulldagTest {
    @Test
    fun `får tilbake elementer fra feed`() {
        kafkaTest(kafkaContainer) {
            val vedtaksfeedConsumer = KafkaConsumer(Properties().apply {
                putAll(kafkaContainer.connectionProperties)
                this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
                this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
            }, StringDeserializer(), VedtakDeserializer())

            val speedClient = mockk<SpeedClient> {
                every { hentFødselsnummerOgAktørId(any(), any()) } returns IdentResponse(
                    fødselsnummer = "fnr",
                    aktørId = "aktørId",
                    npid = null,
                    kilde = IdentResponse.KildeResponse.PDL
                ).ok()
            }

            naisfulTestApp(
                testApplicationModule = {
                    routing { feedApi(VedtaksfeedConsumer.KafkaVedtaksfeedConsumer(topicnavn, vedtaksfeedConsumer), speedClient) }
                },
                objectMapper = objectMapper,
                meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
            ) {
                feedRequest(sistLesteSekvensId = 0, maxAntall = 10).also { feed ->
                    assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic er tom")
                }

                val (fom1, tom1) = LocalDate.of(2020, 3, 1) to LocalDate.of(2020, 3, 15)
                send("0", vedtak(fom1, tom1))
                feedRequest(sistLesteSekvensId = 0, maxAntall = 10).also { feed ->
                    assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic bare har ett element")
                }

                val (fom2, tom2) = LocalDate.of(2020, 4, 1) to LocalDate.of(2020, 4, 15)
                send("0", vedtak(fom2, tom2))
                feedRequest(sistLesteSekvensId = 0, maxAntall = 10).also { feed ->
                    assertTrue(
                        feed.elementer.isNotEmpty(), "Feed skal ha elementer når det er mer enn ett element på topic"
                    )
                    assertEquals(2, feed.elementer.size)
                    assertEquals(fom1.toString(), feed.elementer[0].innhold.utbetalingsreferanse)
                    assertEquals(fom2.toString(), feed.elementer[1].innhold.utbetalingsreferanse)
                }
            }
        }
    }

    private suspend fun TestContext.feedRequest(sistLesteSekvensId: Int, maxAntall: Int): Feed {
        return client.get("/feed?sistLesteSekvensId=$sistLesteSekvensId&maxAntall=$maxAntall").body<Feed>()
    }
}

private fun vedtak(fom: LocalDate, tom: LocalDate) = """
    {
      "type": "SykepengerUtbetalt_v1",
      "opprettet": "2018-01-01T12:00:00",
      "fødselsnummer": "fnr",
      "førsteStønadsdag": "$fom",
      "sisteStønadsdag": "$tom",
      "førsteFraværsdag": "$fom",
      "forbrukteStønadsdager": 123
    }
"""
