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
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class FeedApiNulldagTest {

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

    @Test
    fun `får tilbake elementer fra feed`() {
        embeddedKafkaEnvironment.start()
        val testKafkaProperties = loadTestConfig().also {
            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        }

        val kafkaProducer = KafkaProducer<String, String>(testKafkaProperties)
        consumer = KafkaConsumer(loadTestConfig().toSeekingConsumer())

        withTestApplication({
            installJacksonFeature()
            routing {
                feedApi(testTopic, consumer)
            }
        }) {
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic er tom")
            }

            val (fom1, tom1) = LocalDate.of(2020, 3, 1) to LocalDate.of(2020, 3, 15)
            kafkaProducer.send(ProducerRecord(testTopic, "0", vedtak(fom1, tom1)))
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic bare har ett element")
            }

            val (fom2, tom2) = LocalDate.of(2020, 4, 1) to LocalDate.of(2020, 4, 15)
            kafkaProducer.send(ProducerRecord(testTopic, "1", vedtak(fom2, tom2)))
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(
                    feed.elementer.isNotEmpty(),
                    "Feed skal ha elementer når det er mer enn ett element på topic"
                )
                assertEquals(2, feed.elementer.size)
                assertEquals(fom1.toString(), feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(fom2.toString(), feed.elementer[1].innhold.utbetalingsreferanse)
            }
        }
        embeddedKafkaEnvironment.close()
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
      "type": "SykepengerUtbetalt_v1",
      "opprettet": "2018-01-01T12:00:00",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "førsteStønadsdag": "$fom",
      "sisteStønadsdag": "$tom",
      "førsteFraværsdag": "$fom",
      "forbrukteStønadsdager": 123
    }
"""
