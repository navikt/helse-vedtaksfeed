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
import org.intellij.lang.annotations.Language
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
import java.time.DayOfWeek
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.streams.asSequence
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
            UtbetaltRiverV3(this, internVedtakProducer, internTopic)
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
            vedtakMedFlereLinjer(LocalDate.of(2019, 2, 1), LocalDate.of(2019, 2, 20))
        )
        rapid.sendToListeners(
            vedtakMedUtbetalingnøkkel(LocalDate.of(2019, 3, 1), LocalDate.of(2019, 3, 31))
        )
        rapid.sendToListeners(vedtakV3(LocalDate.of(2019, 4, 1), LocalDate.of(2019, 4, 20), 40))
        rapid.sendToListeners(
            LocalDate.of(2019, 5, 1).let { førsteFom ->
                vedtakV3MedFlereLinjer(
                    førsteFom to førsteFom.plusDays(19),
                    førsteFom.plusDays(20) to førsteFom.plusDays(30),
                    60
                )
            }
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
            assertEquals(22, feed.elementer.size)
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
                assertEquals(LocalDate.of(2019, 2, 1), feed.elementer.first().innhold.foersteStoenadsdag)
            }
        }
    }

    @Test
    fun `takler alle meldingsformater`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=98&maxAntall=4".httpGet {
                val feed = objectMapper.readValue<Feed>(this)

                assertEquals(LocalDate.of(2019, 1, 1), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 1, 31), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals(LocalDate.of(2019, 2, 1), feed.elementer[1].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 2, 20), feed.elementer[1].innhold.sisteStoenadsdag)
                assertEquals(LocalDate.of(2019, 3, 1), feed.elementer[2].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 3, 31), feed.elementer[2].innhold.sisteStoenadsdag)
                assertEquals(LocalDate.of(2019, 4, 1), feed.elementer[3].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 4, 20), feed.elementer[3].innhold.sisteStoenadsdag)
            }
        }
    }

    @Test
    fun `flere utbetalingslinjer slås sammen`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=102&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)

                assertEquals(LocalDate.of(2019, 5, 1), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 5, 31), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals(23, feed.elementer[0].innhold.forbrukteStoenadsdager)
            }
        }
    }

    @Test
    fun `sjekker innhold i vedtak fra utbetalingv3`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=101&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)

                assertEquals(LocalDate.of(2019, 4, 1), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2019, 4, 20), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("aktørId", feed.elementer[0].innhold.aktoerId)
                assertEquals(15, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("77ATRH3QENHB5K4XUY4LQ7HRTY", feed.elementer[0].innhold.utbetalingsreferanse)
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

@Language("JSON")
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

@Language("JSON")
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

@Language("JSON")
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

@Language("JSON")
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

@Language("JSON")
private fun vedtakV3(fom: LocalDate, tom: LocalDate, tidligereBrukteSykedager: Int) = """{
    "aktørId": "aktørId",
    "fødselsnummer": "fnr",
    "organisasjonsnummer": "orgnummer",
    "hendelser": [
        "7c1a1edb-60b9-4a1f-b976-ef39d4d5021c",
        "798f60a1-6f6f-4d07-a036-1f89bd36baca",
        "ee8bc585-e898-4f4c-8662-f2a9b394896e"
    ],
    "utbetalt": [
        {
            "mottaker": "orgnummer",
            "fagområde": "SPREF",
            "fagsystemId": "77ATRH3QENHB5K4XUY4LQ7HRTY",
            "førsteSykepengedag": "",
            "totalbeløp": 8586,
            "utbetalingslinjer": [
                {
                    "fom": "$fom",
                    "tom": "$tom",
                    "dagsats": 1431,
                    "beløp": 1431,
                    "grad": 100.0,
                    "sykedager": ${sykedager(fom, tom)}
                }
            ]
        },
        {
            "mottaker": "fnr",
            "fagområde": "SP",
            "fagsystemId": "353OZWEIBBAYZPKU6WYKTC54SE",
            "totalbeløp": 0,
            "utbetalingslinjer": []
        }
    ],
    "fom": "$fom",
    "tom": "$tom",
    "forbrukteSykedager": ${tidligereBrukteSykedager + sykedager(fom, tom)},
    "gjenståendeSykedager": ${248 - tidligereBrukteSykedager - sykedager(fom, tom)},
    "opprettet": "2020-05-04T11:26:30.23846",
    "system_read_count": 0,
    "@event_name": "utbetalt",
    "@id": "e8eb9ffa-57b7-4fe0-b44c-471b2b306bb6",
    "@opprettet": "2020-05-04T11:27:13.521398",
    "@forårsaket_av": {
        "event_name": "behov",
        "id": "cf28fbba-562e-4841-b366-be1456fdccee",
        "opprettet": "2020-05-04T11:26:47.088455"
    }
}
"""

@Language("JSON")
private fun vedtakV3MedFlereLinjer(
    førsteLine: Pair<LocalDate, LocalDate>,
    andreLinje: Pair<LocalDate, LocalDate>,
    tidligereBrukteSykedager: Int
) = """{
    "aktørId": "aktørId",
    "fødselsnummer": "fnr",
    "organisasjonsnummer": "orgnummer",
    "hendelser": [
        "7c1a1edb-60b9-4a1f-b976-ef39d4d5021c",
        "798f60a1-6f6f-4d07-a036-1f89bd36baca",
        "ee8bc585-e898-4f4c-8662-f2a9b394896e"
    ],
    "utbetalt": [
        {
            "mottaker": "orgnummer",
            "fagområde": "SPREF",
            "fagsystemId": "77ATRH3QENHB5K4XUY4LQ7HRTY",
            "førsteSykepengedag": "",
            "totalbeløp": 8586,
            "utbetalingslinjer": [
                {
                    "fom": "${førsteLine.first}",
                    "tom": "${førsteLine.second}",
                    "dagsats": 1431,
                    "beløp": 1431,
                    "grad": 100.0,
                    "sykedager": ${sykedager(førsteLine.first, førsteLine.second)}
                },
                {
                    "fom": "${andreLinje.first}",
                    "tom": "${andreLinje.second}",
                    "dagsats": 1431,
                    "beløp": 1431,
                    "grad": 100.0,
                    "sykedager": ${sykedager(andreLinje.first, andreLinje.second)}
                }
            ]
        },
        {
            "mottaker": "fnr",
            "fagområde": "SP",
            "fagsystemId": "353OZWEIBBAYZPKU6WYKTC54SE",
            "totalbeløp": 0,
            "utbetalingslinjer": []
        }
    ],
    "fom": "${førsteLine.first}",
    "tom": "${andreLinje.second}",
    "forbrukteSykedager": ${tidligereBrukteSykedager + sykedager(førsteLine.first, andreLinje.second)},
    "gjenståendeSykedager": ${248 - tidligereBrukteSykedager - sykedager(førsteLine.first, andreLinje.second)},
    "opprettet": "2020-05-04T11:26:30.23846",
    "system_read_count": 0,
    "@event_name": "utbetalt",
    "@id": "e8eb9ffa-57b7-4fe0-b44c-471b2b306bb6",
    "@opprettet": "2020-05-04T11:27:13.521398",
    "@forårsaket_av": {
        "event_name": "behov",
        "id": "cf28fbba-562e-4841-b366-be1456fdccee",
        "opprettet": "2020-05-04T11:26:47.088455"
    }
}
"""

private fun sykedager(fom: LocalDate, tom: LocalDate) =
    fom.datesUntil(tom.plusDays(1)).asSequence()
        .filter { it.dayOfWeek !in arrayOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) }.count()
