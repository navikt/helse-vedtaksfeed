package no.nav.helse

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import no.nav.common.KafkaEnvironment
import no.nav.helse.rapids_rivers.inMemoryRapid
import org.apache.kafka.clients.producer.KafkaProducer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class EndToEndTest {

    private lateinit var appBaseUrl: String
    private val wireMockServer: WireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())
    private lateinit var jwtStub: JwtStub

    private val internTopic = "privat-helse-vedtaksfeed-infotrygd"
    private val topicInfos = listOf(KafkaEnvironment.TopicInfo(internTopic, partitions = 1))
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        wireMockServer.start()
        await("vent på WireMockServer har startet")
            .atMost(5, TimeUnit.SECONDS)
            .until {
                try {
                    Socket("localhost", wireMockServer.port()).use { it.isConnected }
                } catch (err: Exception) {
                    false
                }
            }
        val jwtIssuer = "jwtIssuer"
        jwtStub = JwtStub(jwtIssuer, wireMockServer)
        WireMock.stubFor(jwtStub.stubbedJwkProvider())
        WireMock.stubFor(jwtStub.stubbedConfigProvider())

        val randomPort = ServerSocket(0).use { it.localPort }
        appBaseUrl = "http://localhost:$randomPort"

        val rapid = inMemoryRapid {
            ktor {
                port(randomPort)
                module {
                    val testEnv = Environment(
                        kafkaBootstrapServers = "",
                        jwksUrl = "${wireMockServer.baseUrl()}/jwks",
                        jwtIssuer = jwtIssuer
                    )
                    vedtaksfeed(
                        testEnv,
                        JwkProviderBuilder(URL(testEnv.jwksUrl)).build(),
                        loadTestConfig().toProducerConfig()
                    )
                }
            }
        }.apply {
            start()
            val internVedtakProducer = KafkaProducer<String, Vedtak>(loadTestConfig().toProducerConfig())
            UtbetaltRiverV1(this, internVedtakProducer, internTopic)
            UtbetaltRiverV2(this, internVedtakProducer, internTopic)
        }

        repeat(99) {
            rapid.sendToListeners(
                vedtakMedUtbetalingslinjernøkkel(
                    LocalDate.of(2018, 1, 1).plusDays(it.toLong()),
                    LocalDate.of(2018, 1, 1).plusDays(it.toLong())
                )
            )
        }
        rapid.sendToListeners(
            vedtakForQuickFix(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 31))
        )
        rapid.sendToListeners(
            vedtakMedFlereLinjer(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 31))
        )
        rapid.sendToListeners(
            vedtakMedUtbetalingnøkkel(LocalDate.of(2019, 3, 1), LocalDate.of(2019, 3, 31))
        )
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }

    @Test
    fun `får tilbake elementer fra feed`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertTrue(feed.elementer.isNotEmpty())
            }
        }
    }

    @Test
    fun `får tilbake elementer fra feed med antall`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0&maxAntall=10".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(10, feed.elementer.size)
                assertEquals(0, feed.elementer.first().sekvensId)
                assertEquals(123, feed.elementer.first().innhold.forbrukteStoenadsdager)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }
        }

        "/feed?sistLesteSekvensId=9&maxAntall=10".httpGet {
            val feed = objectMapper.readValue<Feed>(this)
            assertEquals(10, feed.elementer.size)
            assertEquals(10, feed.elementer.first().sekvensId)
            assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
        }

        "/feed?sistLesteSekvensId=200".httpGet {
            val feed = objectMapper.readValue<Feed>(this)
            assertTrue(feed.elementer.isEmpty())
        }

        "/feed?sistLesteSekvensId=81&maxAntall=50".httpGet {
            val feed = objectMapper.readValue<Feed>(this)
            assertEquals(20, feed.elementer.size)
        }
    }

    @Test
    fun `kan spørre flere ganger og få samme resultat`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            val url = "/feed?sistLesteSekvensId=0&maxAntall=10"
            url.httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertTrue(feed.elementer.isNotEmpty())
                val content = this

                url.httpGet {
                    assertEquals(content, this)
                }
            }
        }
    }

    @Test
    fun `setter foersteStoenadsdag til første fom lik eller etter førsteFraværsdag`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=99&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(LocalDate.of(2019, 1, 1), feed.elementer.first().innhold.foersteStoenadsdag)
            }
        }
    }

    @Test
    fun `takler alle meldingsformater`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=99&maxAntall=1000".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(LocalDate.of(2019, 3, 1), feed.elementer.last().innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 3, 31), feed.elementer.last().innhold.sisteStoenadsdag)
            }
        }
    }

    private fun loadTestConfig(): Properties = Properties().also {
        it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
        it.remove("security.protocol")
        it.remove("sasl.mechanism")
        it["bootstrap.servers"] = embeddedKafkaEnvironment.brokersURL
    }

    private fun String.httpGet(
        expectedStatus: HttpStatusCode = HttpStatusCode.OK,
        testBlock: String.() -> Unit = {}
    ) {
        val token = jwtStub.createTokenFor(
            subject = "srvInfot",
            audience = "spokelse_azure_ad_app_id"
        )

        val connection = appBaseUrl.handleRequest(HttpMethod.Get, this,
            builder = {
                setRequestProperty(HttpHeaders.Authorization, "Bearer $token")
            })

        assertEquals(expectedStatus.value, connection.responseCode)
        connection.responseBody.testBlock()
    }

    private fun String.handleRequest(
        method: HttpMethod,
        path: String,
        builder: HttpURLConnection.() -> Unit = {}
    ): HttpURLConnection {
        val url = URL("$this$path")
        val con = url.openConnection() as HttpURLConnection
        con.requestMethod = method.value

        con.builder()

        con.connectTimeout = 1000
        con.readTimeout = 5000

        return con
    }

    private val HttpURLConnection.responseBody: String
        get() {
            val stream: InputStream? = if (responseCode in 200..299) {
                inputStream
            } else {
                errorStream
            }

            return stream?.use { it.bufferedReader().readText() } ?: ""
        }

}

private fun vedtakForQuickFix(fom: LocalDate, tom: LocalDate) = """
    {
      "@event_name": "utbetalt",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "førsteFraværsdag": "${fom.plusDays(1)}",
      "vedtaksperiodeId": "a91a95b2-1e7c-42c4-b584-2d58c728f5b5",
      "utbetaling": [
        {
          "utbetalingsreferanse": "WKOZJT3JYNB3VNT5CE5U54R3Y4",
          "utbetalingslinjer": [
            {
              "fom": "$fom",
              "tom": "$tom",
              "dagsats": 1000,
              "grad": 100.0
            }
          ]
        }
      ],
      "forbrukteSykedager": 123,
      "opprettet": "2018-01-01T12:00:00",
      "system_read_count": 0
    }
"""

private fun vedtakMedUtbetalingnøkkel(fom: LocalDate, tom: LocalDate) = """
    {
      "@event_name": "utbetalt",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "førsteFraværsdag": "$fom",
      "vedtaksperiodeId": "a91a95b2-1e7c-42c4-b584-2d58c728f5b5",
      "utbetaling": [
        {
          "utbetalingsreferanse": "WKOZJT3JYNB3VNT5CE5U54R3Y4",
          "utbetalingslinjer": [
            {
              "fom": "$fom",
              "tom": "$tom",
              "dagsats": 1000,
              "grad": 100.0
            }
          ]
        }
      ],
      "forbrukteSykedager": 123,
      "opprettet": "2018-01-01T12:00:00",
      "system_read_count": 0
    }
"""

private fun vedtakMedFlereLinjer(fom: LocalDate, tom: LocalDate) = """
    {
      "@event_name": "utbetalt",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "førsteFraværsdag": "$fom",
      "vedtaksperiodeId": "a91a95b2-1e7c-42c4-b584-2d58c728f5b5",
      "utbetaling": [
        {
          "utbetalingsreferanse": "WKOZJT3JYNB3VNT5CE5U54R3Y4",
          "utbetalingslinjer": [
            {
              "fom": "${fom.minusMonths(1)}",
              "tom": "${tom.minusMonths(1)}",
              "dagsats": 1000,
              "grad": 100.0
            }, {
              "fom": "$fom",
              "tom": "$tom",
              "dagsats": 1000,
              "grad": 100.0
            }
          ]
        }
      ],
      "forbrukteSykedager": 123,
      "opprettet": "2018-01-01T12:00:00",
      "system_read_count": 0
    }
"""

private fun vedtakMedUtbetalingslinjernøkkel(fom: LocalDate, tom: LocalDate) = """
    {
      "@event_name": "utbetalt",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "førsteFraværsdag": "$fom",
      "vedtaksperiodeId": "a91a95b2-1e7c-42c4-b584-2d58c728f5b5",
      "utbetalingslinjer": [
        {
          "fom": "$fom",
          "tom": "$tom",
          "dagsats": 1000,
          "grad": 100.0
        }
      ],
      "forbrukteSykedager": 123,
      "opprettet": "2018-01-01T12:00:00",
      "system_read_count": 0
    }
"""
