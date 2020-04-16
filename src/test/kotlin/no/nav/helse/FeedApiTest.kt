package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpMethod
import io.ktor.routing.routing
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.withTestApplication
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.util.*
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class FeedApiTest {

    private val testTopic = "yes"
    private val topicInfos = listOf(
        KafkaEnvironment.TopicInfo(testTopic, partitions = 1)
    )
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    lateinit var consumer: KafkaConsumer<String, Vedtak>

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        val testKafkaProperties = loadTestConfig().also {
            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        }

        KafkaProducer<String, String>(testKafkaProperties).use { testproducer ->
            repeat(1000) {
                testproducer.send(
                    ProducerRecord(
                        testTopic,
                        it.toString(),
                        vedtak(LocalDate.of(2018, 1, 1).plusDays(it.toLong()), LocalDate.of(2018, 1, 1).plusDays(it.toLong()))
                    )
                )
            }
            testproducer.send(
                ProducerRecord(
                    testTopic,
                    "1000",
                    vedtakMedFlereLinjer(LocalDate.of(2019, 1, 1), LocalDate.of(2019, 1, 31))
                )
            )
        }
        consumer = KafkaConsumer(loadTestConfig().toSeekingConsumer())
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }

    @Test
    fun `får tilbake elementer fra feed`() {
        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }
        }
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isNotEmpty())
            }
        }
    }

    @Test
    fun `får tilbake elementer fra feed med antall`() {
        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }
        }
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(10, feed.elementer.size)
                assertEquals(0, feed.elementer.first().sekvensId)
                assertEquals(123, feed.elementer.first().innhold.forbrukteStoenadsdager)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=9&maxAntall=10")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(10, feed.elementer.size)
                assertEquals(10, feed.elementer.first().sekvensId)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=2000")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isEmpty())
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=980&maxAntall=50")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(20, feed.elementer.size)
                assertEquals(981, feed.elementer.first().sekvensId)
                assertEquals(19, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }
        }
    }

    @Test
    fun `kan spørre flere ganger og få samme resultat`() {
        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }
        }
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")) {
                val content = response.content!!

                with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")) {
                    assertEquals(content, response.content!!)
                }
            }
        }
    }

    @Test
    fun `setter foersteStoenadsdag til første fom lik eller etter førsteFraværsdag`() {
        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }
        }) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=999&maxAntall=1")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(LocalDate.of(2019, 1, 1), feed.elementer.first().innhold.foersteStoenadsdag)
            }
        }
    }

    private fun loadTestConfig(): Properties = Properties().also {
        it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
        it.remove("security.protocol")
        it.remove("sasl.mechanism")
        it["bootstrap.servers"] = embeddedKafkaEnvironment.brokersURL
    }
}

private fun vedtak(fom: LocalDate, tom: LocalDate) = """
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
      "opprettet": "2018-01-01",
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
      "opprettet": "2018-01-01",
      "system_read_count": 0
    }
"""
