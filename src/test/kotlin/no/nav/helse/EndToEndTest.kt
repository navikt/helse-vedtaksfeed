package no.nav.helse

import com.github.navikt.tbd_libs.naisful.test.TestContext
import com.github.navikt.tbd_libs.naisful.test.naisfulTestApp
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.result_object.ok
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.ktor.client.call.body
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.get
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.*
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class EndToEndTest {
    private lateinit var internVedtakProducer: KafkaProducer<String, Vedtak>
    private val rapid = TestRapid().apply {
        setupRivers { fødselsnummer, vedtak ->
            internVedtakProducer.send(ProducerRecord(internTopic, fødselsnummer, vedtak)).get().offset()
        }
    }

    @Test
    fun `får tilbake elementer fra feed`() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 0, maxAntall = 1)
                assertTrue(feed.elementer.isNotEmpty())
            }
        }
    }

    @Test
    fun `får tilbake elementer fra feed med antall`() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 0, maxAntall = 10)
                assertEquals(10, feed.elementer.size)
                assertEquals(0, feed.elementer.first().sekvensId)
                assertEquals(80, feed.elementer.first().innhold.forbrukteStoenadsdager)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
                assertTrue(feed.inneholderFlereElementer)
            }
        }

        runBlocking {
            val feed = feedRequest(sistLesteSekvensId = 9, maxAntall = 10)
            assertEquals(10, feed.elementer.size)
            assertEquals(10, feed.elementer.first().sekvensId)
            assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
        }

        runBlocking {
            val feed = feedRequest(sistLesteSekvensId = 200, maxAntall = 1)
            assertTrue(feed.elementer.isEmpty())
        }

        runBlocking {
            val feed = feedRequest(sistLesteSekvensId = 81, maxAntall = 50)
            assertEquals(21, feed.elementer.size)
            assertFalse(feed.inneholderFlereElementer)
        }
    }

    @Test
    fun `kan spørre flere ganger og få samme resultat`() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 0, maxAntall = 10)
                assertTrue(feed.elementer.isNotEmpty())
            }
        }
    }

    @Test
    fun annulleringV1() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 99, maxAntall = 1)
                assertEquals("SykepengerAnnullert", feed.elementer[0].type)
                assertEquals(LocalDate.of(2018, 1, 1), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2018, 2, 1), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("2336848909974", feed.elementer[0].innhold.aktoerId)
                assertEquals(0, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("3333JT3JYNB3VNT5CE5U54R3Y4", feed.elementer[0].innhold.utbetalingsreferanse)
            }
        }
    }

    @Test
    fun utbetalingUtbetaltTest() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 11, maxAntall = 1)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2020, 8, 9), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2020, 9, 1), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("2336848909974", feed.elementer[0].innhold.aktoerId)
                assertEquals(80, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("E6TEDJJKBVEYBCEZV73WRJPGAA", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(LocalDate.of(2020, 12, 14), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }

    @Test
    fun `utbetaling utbetalt med et hint av revurdering`() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 100, maxAntall = 1)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2020, 8, 9), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2020, 9, 1), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("2336848909974", feed.elementer[0].innhold.aktoerId)
                assertEquals(79, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("E6TEDJJKBVEYBCEZV73WRJPGAA", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(LocalDate.of(2020, 12, 14), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }

    @Test
    fun `les ut utbetaling til bruker`() = e2e {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val feed = feedRequest(sistLesteSekvensId = 101, maxAntall = 1)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2021, 8, 17), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2021, 9, 1), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("2336848909974", feed.elementer[0].innhold.aktoerId)
                assertEquals(11, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("WHQQO7GYFZBMPBVJU7H2B2BWTA", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(LocalDate.of(2021, 11, 11), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }

    private suspend fun TestContext.feedRequest(sistLesteSekvensId: Int, maxAntall: Int): Feed {
        return client.get("/feed?sistLesteSekvensId=$sistLesteSekvensId&maxAntall=$maxAntall") {
            val token = jwtStub.createTokenFor(
                subject = infotrygClientId,
                audience = vedtaksfeedAudience
            )
            bearerAuth(token)
        }.body<Feed>()
    }

    private lateinit var appBaseUrl: String
    private val wireMockServer: WireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())
    private lateinit var jwtStub: JwtStub

    private val infotrygClientId = "my_cool_client_id"
    private val vedtaksfeedAudience = "vedtaksfeed_client_id"
    private val randomPort = ServerSocket(0).use { it.localPort }
    private val jwtIssuer = mockAuthentication()

    private val speedClient = mockk<SpeedClient> {
        every { hentFødselsnummerOgAktørId(any(), any()) } returns IdentResponse(
            fødselsnummer = "fnr",
            aktørId = "2336848909974",
            npid = null,
            kilde = IdentResponse.KildeResponse.PDL
        ).ok()
    }

    private val internTopic = "tbd.infotrygd.vedtaksfeed.v1"
    private val topicInfos = listOf(KafkaEnvironment.TopicInfo(internTopic, partitions = 1))
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    private fun e2e(testblokk: suspend TestContext.() -> Unit) = naisfulTestApp(
        testApplicationModule = {
            val azureConfig = AzureAdAppConfig(
                clientId = vedtaksfeedAudience,
                configurationUrl = "${wireMockServer.baseUrl()}/config"
            )
            vedtaksfeed(
                internTopic,
                KafkaConsumer(loadTestConfig().toSeekingConsumer(), StringDeserializer(), VedtakDeserializer()),
                azureConfig,
                speedClient
            )
        },
        objectMapper = objectMapper,
        meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
        testblokk = testblokk
    )

    private fun Properties.toSeekingConsumer() = Properties().also {
        it.putAll(this)
        it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
        it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
        it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
    }

    private fun mockAuthentication(): String {
        wireMockServer.start()
        await("vent på WireMockServer har startet")
            .atMost(5, TimeUnit.SECONDS)
            .until {
                try {
                    Socket("localhost", wireMockServer.port()).use { it.isConnected }
                } catch (_: Exception) {
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

    private fun Properties.toProducerConfig(): Properties = Properties().also {
        it.putAll(this)
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = VedtakSerializer::class.java
        put(ProducerConfig.ACKS_CONFIG, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    }

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        appBaseUrl = "http://localhost:$randomPort"
        internVedtakProducer = KafkaProducer<String, Vedtak>(loadTestConfig().toProducerConfig())
    }

    @AfterEach
    fun reset() {
        rapid.reset()
        embeddedKafkaEnvironment.adminClient?.use { adminClient ->

            adminClient.deleteTopics(topicInfos.map { it.name })
            adminClient.createTopics(topicInfos.map {
                NewTopic(it.name, it.partitions, 1)
            })
        }
    }

    private fun setupTestData(rapid: TestRapid) {
        repeat(100) {
            rapid.sendTestMessage(utbetalingUtbetalt())
        }
        rapid.sendTestMessage(annullering)
        rapid.sendTestMessage(utbetalingUtbetalt("REVURDERING", stønadsdager = 79))
        rapid.sendTestMessage(utbetalingTilBruker())
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }
}

@Language("JSON")
private val annullering = """{
    "@event_name": "utbetaling_annullert",
    "fødselsnummer": "fnr",
    "organisasjonsnummer": "999263550",
    "korrelasjonsId": "def7b4cf-69c3-43ba-b67d-113b4ef23bc7",
    "utbetalingId": "bf1d3157-5cfa-4b37-8faa-63cd3c3f314f",
    "fom": "2018-01-01",
    "tom": "2018-02-01",
    "@opprettet": "2018-01-01T12:00:00",
    "system_read_count": 0
}
"""

@Language("JSON")
private fun utbetalingUtbetalt(utbetalingtype: String = "UTBETALING", stønadsdager: Int = 80) = """
    {
      "utbetalingId": "b440fa98-3e1a-11eb-b378-0242ac130002",
      "type": "$utbetalingtype",
      "fom": "2020-08-01",
      "tom": "2020-08-24",
      "maksdato": "2020-12-20",
      "gjenståendeSykedager": ${248 - stønadsdager},
      "stønadsdager": $stønadsdager,
      "tidspunkt": "2020-12-14T15:38:10.479991",
      "korrelasjonsId": "27a641a5-2a0d-4980-8899-aff768a5e600",
      "@event_name": "utbetaling_utbetalt",
      "@id": "d65f35dc-df67-4143-923f-d005075b0ee3",
      "@opprettet": "2020-12-14T15:38:14.419655",
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
        "linjer": [
          {
            "fom": "2020-08-09",
            "tom": "2020-09-01"
          }
        ]
      }
  }
"""

@Language("JSON")
private fun utbetalingTilBruker() = """
    {
      "utbetalingId": "96e819b7-7dc8-4125-8a5d-b5b6d7f5bc5f",
      "type": "UTBETALING",
      "fom": "2021-08-16",
      "tom": "2021-08-31",
      "maksdato": "2021-12-20",
      "gjenståendeSykedager": 237,
      "stønadsdager": 11,
      "tidspunkt": "2021-11-11T10:56:08.464312658",
      "korrelasjonsId": "b1e1077c-d82e-42c7-86a9-a7cfa0e83698",
      "@event_name": "utbetaling_utbetalt",
      "@id": "25f8a9a0-3034-457d-9976-6e2575970c1f",
      "@opprettet": "2021-11-11T10:56:09.746062805",
      "fødselsnummer": "09047606370",
      "organisasjonsnummer": "972674818",
      "arbeidsgiverOppdrag": {
        "linjer": [
          {
            "fom": "2021-08-17",
            "tom": "2021-09-01"
          }
        ]
      },
      "personOppdrag": {
        "linjer": []
      }
    }
"""
