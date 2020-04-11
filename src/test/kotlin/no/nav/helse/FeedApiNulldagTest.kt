package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpMethod
import io.ktor.routing.routing
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.withTestApplication
import no.nav.common.KafkaEnvironment
import org.apache.commons.codec.binary.Base32
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
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

            val gruppeId1 = UUID.randomUUID()
            kafkaProducer.send(ProducerRecord(testTopic, "0", vedtak(gruppeId1)))
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(feed.elementer.isEmpty(), "Feed skal være tom når topic bare har ett element")
            }

            val gruppeId2 = UUID.randomUUID()
            kafkaProducer.send(ProducerRecord(testTopic, "1", vedtak(gruppeId2)))
            with(handleRequest(HttpMethod.Get, "/feed?sistLesteSekvensId=0")) {
                val feed = objectMapper.readValue<Feed>(response.content!!)
                assertTrue(
                    feed.elementer.isNotEmpty(),
                    "Feed skal ha elementer når det er mer enn ett element på topic"
                )
                assertEquals(2, feed.elementer.size)
                assertEquals(gruppeId1, feed.elementer[0].innhold.utbetalingsreferanse.decodeGruppeId())
                assertEquals(gruppeId2, feed.elementer[1].innhold.utbetalingsreferanse.decodeGruppeId())
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

private fun String.decodeGruppeId() = ByteBuffer.wrap(Base32('='.toByte()).decode(this.toByteArray()))
    .let { UUID(it.getLong(0), it.getLong(8)) }

private fun vedtak(id: UUID) = """
    {
      "@event_name": "utbetalt",
      "aktørId": "aktørId",
      "fødselsnummer": "fnr",
      "gruppeId": "$id",
      "vedtaksperiodeId": "a91a95b2-1e7c-42c4-b584-2d58c728f5b5",
      "utbetaling": [
        {
          "utbetalingsreferanse": "WKOZJT3JYNB3VNT5CE5U54R3Y4",
          "utbetalingslinjer": [
            {
              "fom": "2018-01-01",
              "tom": "2018-01-10",
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
