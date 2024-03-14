package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import io.ktor.http.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import no.nav.common.KafkaEnvironment
import no.nav.helse.rapids_rivers.testsupport.TestRapid
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
import java.io.InputStream
import java.net.*
import java.time.LocalDate
import java.time.LocalDate.EPOCH
import java.time.LocalDate.now
import java.time.LocalDateTime
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
    fun `får tilbake elementer fra feed`() {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertTrue(feed.elementer.isNotEmpty())
            }
        }
    }

    @Test
    fun `får tilbake elementer fra feed med antall`() {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0&maxAntall=10".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(10, feed.elementer.size)
                assertEquals(0, feed.elementer.first().sekvensId)
                assertEquals(0, feed.elementer.first().innhold.forbrukteStoenadsdager)
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
            assertEquals(18, feed.elementer.size)
            assertFalse(feed.inneholderFlereElementer)
        }
    }

    @Test
    fun `kan spørre flere ganger og få samme resultat`() {
        setupTestData(rapid)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
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
    fun `behandling opprettet fører til ny linje`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val opprettet = LocalDateTime.now()

        sendBehandlingOpprettet(vedtaksperiodeId1, opprettet)
        sendBehandlingOpprettet(vedtaksperiodeId2, opprettet)

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0&maxAntall=2".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(2, feed.elementer.size)
                assertEquals("SykepengerUtbetalt_v1", feed.elementer[0].type)
                assertEquals(LocalDate.of(2024, 2, 12), feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(LocalDate.of(2024, 2, 16), feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("aktørid", feed.elementer[0].innhold.aktoerId)
                assertEquals(0, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("$vedtaksperiodeId1", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(opprettet.toLocalDate(), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }
    @Test
    fun `behandling forkastet fører til fjerning av linje`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val opprettet = LocalDateTime.now()

        sendBehandlingForkastet(vedtaksperiodeId1, opprettet)
        sendBehandlingForkastet(vedtaksperiodeId2, opprettet)

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            "/feed?sistLesteSekvensId=0&maxAntall=2".httpGet {
                val feed = objectMapper.readValue<Feed>(this)
                assertEquals(2, feed.elementer.size)
                assertEquals("SykepengerAnnullert", feed.elementer[0].type)
                assertEquals(EPOCH, feed.elementer[0].innhold.foersteStoenadsdag)
                assertEquals(EPOCH, feed.elementer[0].innhold.sisteStoenadsdag)
                assertEquals("aktørid", feed.elementer[0].innhold.aktoerId)
                assertEquals(0, feed.elementer[0].innhold.forbrukteStoenadsdager)
                assertEquals("$vedtaksperiodeId1", feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(opprettet.toLocalDate(), feed.elementer[0].metadata.opprettetDato)
            }
        }
    }

    private fun sendBehandlingOpprettet(vedtaksperiodeId: UUID, opprettet: LocalDateTime) {
        //language=JSON
        val behandlingOpprettet = """{
          "@event_name": "behandling_opprettet",
          "organisasjonsnummer": "orgnr",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "behandlingId": "${UUID.randomUUID()}",
          "fom": "2024-02-12",
          "tom": "2024-02-16",
          "@id": "${UUID.randomUUID()}",
          "@opprettet": "$opprettet",
          "aktørId": "aktørid",
          "fødselsnummer": "fnr"
        }
        """.trimIndent()
        rapid.sendTestMessage(behandlingOpprettet)
    }

    private fun sendAvsluttetMedVedtak(vedtaksperiodeId: UUID, opprettet: LocalDateTime) {
        //language=JSON
        val melding = """{
          "@event_name": "avsluttet_med_vedtak",
          "organisasjonsnummer": "orgnr",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "fom": "2024-01-31",
          "tom": "2024-02-17",
          "@id": "${UUID.randomUUID()}",
          "@opprettet": "$opprettet",
          "aktørId": "aktørid",
          "fødselsnummer": "fnr"
        }
        """.trimIndent()
        rapid.sendTestMessage(melding)
    }

    private fun sendBehandlingForkastet(vedtaksperiodeId: UUID, opprettet: LocalDateTime) {
        //language=JSON
        val melding = """{
          "@event_name": "behandling_forkastet",
          "organisasjonsnummer": "orgnr",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "behandlingId": "${UUID.randomUUID()}",
          "@id": "${UUID.randomUUID()}",
          "@opprettet": "$opprettet",
          "aktørId": "aktørid",
          "fødselsnummer": "fnr"
        }
        """.trimIndent()
        rapid.sendTestMessage(melding)
    }

    private fun String.httpGet(
        expectedStatus: HttpStatusCode = HttpStatusCode.OK,
        testBlock: String.() -> Unit = {}
    ) {
        val token = jwtStub.createTokenFor(
            subject = infotrygClientId,
            audience = vedtaksfeedAudience
        )

        log.info("sender request til $this med token=Bearer $token")

        val connection = appBaseUrl.handleRequest(HttpMethod.Get, this,
            builder = {
                setRequestProperty(HttpHeaders.Authorization, "Bearer $token")
                readTimeout = 10000
            })

        assertEquals(expectedStatus.value, connection.responseCode)
        connection.responseBody.testBlock()
    }

    private fun String.handleRequest(
        method: HttpMethod,
        path: String,
        builder: HttpURLConnection.() -> Unit = {}
    ): HttpURLConnection {
        val url = URI("$this$path").toURL()
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

    private val infotrygClientId = "my_cool_client_id"
    private val vedtaksfeedAudience = "vedtaksfeed_client_id"
    private val randomPort = ServerSocket(0).use { it.localPort }
    private val jwtIssuer = mockAuthentication()

    private val ktor: ApplicationEngine = setupKtor()

    private val internTopic = "tbd.infotrygd.vedtaksfeed.v1"
    private val topicInfos = listOf(KafkaEnvironment.TopicInfo(internTopic, partitions = 1))
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    private fun setupKtor() = embeddedServer(CIO, applicationEngineEnvironment {
        connector {
            port = randomPort
        }
        module {
            val azureConfig = AzureAdAppConfig(
                clientId = vedtaksfeedAudience,
                configurationUrl = "${wireMockServer.baseUrl()}/config"
            )
            vedtaksfeed(
                internTopic,
                KafkaConsumer(loadTestConfig().toSeekingConsumer(), StringDeserializer(), VedtakDeserializer()),
                azureConfig
            )
        }
    })

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

        ktor.start(false)
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
            sendBehandlingOpprettet(UUID.randomUUID(), LocalDateTime.now())
        }
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
        ktor.stop(5000, 5000)
    }
}

