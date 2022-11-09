package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.server.testing.*
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*
import kotlin.collections.set

internal class FeedApiNulldagTest {

    private val testTopic = "yes"
    private val topicInfos = listOf(
        KafkaEnvironment.TopicInfo(testTopic, partitions = 1)
    )
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false, noOfBrokers = 1, topicInfos = topicInfos, withSchemaRegistry = false, withSecurity = false
    )

    private fun Properties.toSeekingConsumer() = Properties().also {
        it.putAll(this)
        it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
        it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
        it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
    }

    @Test
    fun `får tilbake elementer fra feed`() {
        embeddedKafkaEnvironment.start()
        val testKafkaProperties = loadTestConfig().also {
            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        }

        val kafkaProducer = KafkaProducer<String, String>(testKafkaProperties)
        val consumer = KafkaConsumer<String, Vedtak>(loadTestConfig().toSeekingConsumer())

        testApplication {
            application { installJacksonFeature() }
            routing { feedApi(testTopic, consumer) }

            client.get("/feed?sistLesteSekvensId=0").let { response ->
                val feed = objectMapper.readValue<Feed>(response.bodyAsText())
                assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic er tom")
            }

            val (fom1, tom1) = LocalDate.of(2020, 3, 1) to LocalDate.of(2020, 3, 15)
            kafkaProducer.send(ProducerRecord(testTopic, "0", vedtak(fom1, tom1)))
            client.get("/feed?sistLesteSekvensId=0").let { response ->
                val feed = objectMapper.readValue<Feed>(response.bodyAsText())
                assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic bare har ett element")
            }

            val (fom2, tom2) = LocalDate.of(2020, 4, 1) to LocalDate.of(2020, 4, 15)
            kafkaProducer.send(ProducerRecord(testTopic, "1", vedtak(fom2, tom2)))
            client.get("/feed?sistLesteSekvensId=0").let { response ->
                val feed = objectMapper.readValue<Feed>(response.bodyAsText())
                assertTrue(
                    feed.elementer.isNotEmpty(), "Feed skal ha elementer når det er mer enn ett element på topic"
                )
                assertEquals(2, feed.elementer.size)
                assertEquals(fom1.toString(), feed.elementer[0].innhold.utbetalingsreferanse)
                assertEquals(fom2.toString(), feed.elementer[1].innhold.utbetalingsreferanse)
            }
        }
        embeddedKafkaEnvironment.close()
    }

    private fun loadTestConfig(): Properties = Properties().also {
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
