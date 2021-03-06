package no.nav.helse

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.ktor.http.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import no.nav.common.KafkaEnvironment
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.apache.kafka.clients.producer.KafkaProducer
import org.awaitility.Awaitility.await
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class EndToEndTest {

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
                assertEquals(80, feed.elementer.first().innhold.forbrukteStoenadsdager)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
                assertTrue(feed.inneholderFlereElementer)
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
            assertFalse(feed.inneholderFlereElementer)
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
    fun annulleringV1() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=99&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)

                assertEquals("SykepengerAnnullert", feed.elementer[0].type)
                assertEquals(LocalDate.of(2018, 1, 1), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2018, 2, 1), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("aktørId", feed.elementer[0].innhold.aktoerId)
                assertEquals(0, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("3333JT3JYNB3VNT5CE5U54R3Y4", feed.elementer[0].innhold.utbetalingsreferanse)
            }
        }
    }

    @Test
    fun utbetalingUtbetaltTest() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=1&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2020, 8, 9), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2020, 8, 24), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("1111110000000", feed.elementer[0].innhold.aktoerId)
                assertEquals(80, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("C6GNAZID6IFNURRWEJ6WP3IE5D", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(LocalDate.of(2020, 12, 14), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }

    @Test
    fun `utbetaling utbetalt med et hint av revurdering`() {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=100&maxAntall=1".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2020, 8, 9), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2020, 8, 24), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("1111110000000", feed.elementer[0].innhold.aktoerId)
                assertEquals(79, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("C6GNAZID6IFNURRWEJ6WP3IE5D", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(LocalDate.of(2020, 12, 14), feed.elementer[0].metadata.opprettetDato)
            }
        }
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

    private lateinit var appBaseUrl: String
    private val wireMockServer: WireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())
    private lateinit var jwtStub: JwtStub

    private val randomPort = ServerSocket(0).use { it.localPort }
    private val jwtIssuer = mockAuthentication()

    private val ktor: ApplicationEngine = setupKtor()

    private val internTopic = "privat-helse-vedtaksfeed-infotrygd"
    private val topicInfos = listOf(KafkaEnvironment.TopicInfo(internTopic, partitions = 1))
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    private fun setupKtor() = embeddedServer(Netty, applicationEngineEnvironment {
        connector {
            port = randomPort
        }
        module {
            val testEnv = Environment(
                kafkaBootstrapServers = "",
                jwksUrl = "${wireMockServer.baseUrl()}/jwks",
                jwtIssuer = jwtIssuer,
                truststorePassword = null,
                truststorePath = null
            )
            vedtaksfeed(
                testEnv,
                JwkProviderBuilder(URL(testEnv.jwksUrl)).build(),
                loadTestConfig().toProducerConfig()
            )
        }
    })

    private fun mockAuthentication(): String {
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
        return jwtIssuer
    }

    private fun loadTestConfig(): Properties = Properties().also {
        it["bootstrap.servers"] = embeddedKafkaEnvironment.brokersURL
    }

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        appBaseUrl = "http://localhost:$randomPort"

        val rapid = TestRapid().apply {
            val internVedtakProducer = KafkaProducer<String, Vedtak>(loadTestConfig().toProducerConfig())
            UtbetalingUtbetaltRiver(this, internVedtakProducer, internTopic)
            AnnullertRiverV1(this, internVedtakProducer, internTopic)
        }

        ktor.start(false)
        repeat(100) {
            rapid.sendTestMessage(utbetalingUtbetalt())
        }
        rapid.sendTestMessage(annullering)
        rapid.sendTestMessage(utbetalingUtbetalt("REVURDERING", stønadsdager = 79))
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
        ktor.stop(5000, 5000)
    }
}

@Language("JSON")
private val annullering = """{
    "@event_name": "utbetaling_annullert",
    "aktørId": "aktørId",
    "fødselsnummer": "fnr",
    "organisasjonsnummer": "999263550",
    "fagsystemId": "3333JT3JYNB3VNT5CE5U54R3Y4",
    "utbetalingId": "bf1d3157-5cfa-4b37-8faa-63cd3c3f314f",
    "annullertAvSaksbehandler" : "2018-01-01T12:00:00",
    "utbetalingslinjer": [
        {
            "fom": "2018-01-01",
            "tom": "2018-02-01",
            "beløp": 1000,
            "grad": 100.0
        }
    ],
    "@opprettet": "2018-01-01T12:00:00",
    "system_read_count": 0
}
"""

@Language("JSON")
private fun utbetalingUtbetalt(utbetalingtype: String = "UTBETALING", stønadsdager: Int = 80) = """{
  "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
  "type": "$utbetalingtype",
  "maksdato": "2021-03-17",
  "forbrukteSykedager": 180,
  "gjenståendeSykedager": 68,
  "ident": "Automatisk behandlet",
  "epost": "tbd@nav.no",
  "tidspunkt": "2020-12-14T15:38:10.479991",
  "automatiskBehandling": true,
  "arbeidsgiverOppdrag": {
    "mottaker": "999999999",
    "fagområde": "SPREF",
    "linjer": [
      {
        "fom": "2020-08-09",
        "tom": "2020-08-24",
        "dagsats": 1623,
        "lønn": 2029,
        "grad": 80.0,
        "stønadsdager": 11,
        "totalbeløp": 17853,
        "endringskode": "UEND",
        "delytelseId": 1,
        "refDelytelseId": null,
        "refFagsystemId": null,
        "statuskode": null,
        "datoStatusFom": null,
        "klassekode": "SPREFAG-IOP"
      }
    ],
    "fagsystemId": "C6GNAZID6IFNURRWEJ6WP3IE5D",
    "endringskode": "ENDR",
    "sisteArbeidsgiverdag": null,
    "tidsstempel": "2020-12-14T15:36:32.932737",
    "nettoBeløp": 15525,
    "stønadsdager": $stønadsdager,
    "fom": "2020-08-09",
    "tom": "2020-08-24"
  },
  "personOppdrag": {
    "mottaker": "11111100000",
    "fagområde": "SP",
    "linjer": [],
    "fagsystemId": "C6GNAZID6IFNURRWEJ6WP3IE5D",
    "endringskode": "NY",
    "sisteArbeidsgiverdag": null,
    "tidsstempel": "2020-12-14T15:36:32.932944",
    "nettoBeløp": 0,
    "stønadsdager": 0,
    "fom": "-999999999-01-01",
    "tom": "-999999999-01-01"
  },
  "@event_name": "utbetaling_utbetalt",
  "@id": "d65f35dc-df67-4143-923f-d005075b0ee3",
  "@opprettet": "2020-12-14T15:38:14.419655",
  "aktørId": "1111110000000",
  "fødselsnummer": "11111100000",
  "organisasjonsnummer": "999999999"
}
"""
