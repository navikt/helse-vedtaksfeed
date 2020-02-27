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
import java.util.*
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class FeedApiKtTest {

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
                testproducer.send(ProducerRecord(testTopic, it.toString(), vedtak(it)))
            }
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
            }}
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")){
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
            }}
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")){
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(10, feed.elementer.size)
                assertEquals(0, feed.elementer.first().sekvensId)
                assertEquals(123, feed.elementer.first().innhold.forbrukteStoenadsdager)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=9&maxAntall=10")){
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(10, feed.elementer.size)
                assertEquals(10, feed.elementer.first().sekvensId)
                assertEquals(9, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=2000")){
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isEmpty())
            }

            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=980&maxAntall=50")){
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertEquals(19, feed.elementer.size)
                assertEquals(981, feed.elementer.first().sekvensId)
                assertEquals(18, feed.elementer.last().sekvensId - feed.elementer.first().sekvensId)
            }
        }
    }

    @Test
    fun `kan spørre flere ganger og få samme resultat`() {
        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }}
        ) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")){
                val content = response.content!!

                with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0&maxAntall=10")){
                    assertEquals(content, response.content!!)
                }
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

fun vedtak(id: Int) = """
    {
        "@event_name": "utbetalt",
        "aktørId": "aktørId",
        "utbetalingsreferanse": "$id",
        "utbetalingslinjer": [{ "fom": "2018-01-01", "tom": "2018-01-10", "dagsats": 1000 }],
        "opprettet": "2018-01-01",
        "fødselsnummer": "fnr",
        "forbrukteSykedager": 123
    }
"""


