package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Properties

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class VedtakskonsumentTest {
    private val testTopic = "privat-helse-sykepenger-behov"
    private val topicInfos = listOf(
        KafkaEnvironment.TopicInfo(testTopic)
    )
    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    private val environment = Environment(
        kafkaBootstrapServers = embeddedKafkaEnvironment.brokersURL,
        vedtakstopic = testTopic,
        jwksUrl = "http://jwks.url",
        jwtIssuer = "http://jwtissuer.url"
    )

    private val testVedtakskonsumentBuilder = VedtakskonsumentBuilder<String, JsonNode>(environment)

    internal class JacksonKafkaSerializer : Serializer<JsonNode> {
        override fun serialize(topic: String?, data: JsonNode?): ByteArray = objectMapper.writeValueAsBytes(data)
    }

    private val testKafkaProperties = Properties().also {
        it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
        it["bootstrap.servers"] = environment.kafkaBootstrapServers
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JacksonKafkaSerializer::class.java
        it[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "PLAINTEXT"
        it[SaslConfigs.SASL_MECHANISM] = "PLAIN"
    }

    private val testProducer = KafkaProducer<String, JsonNode>(testKafkaProperties)

    private lateinit var vedtakskonsument: Vedtakskonsument

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        vedtakskonsument = Vedtakskonsument(testVedtakskonsumentBuilder)
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }

    @Test
    internal fun `skal returnere tom liste hvis det ikke finnes noen vedtak`() {
        assertEquals(emptyList<Any>(), vedtakskonsument.hentVedtak(100, 1))
    }

    @Test
    internal fun `skal returnere liste med ett vedtak når det finnes ett vedtak`() {
        testProducer.send(ProducerRecord(testTopic, objectMapper.valueToTree("{}")))
        assertEquals(1, vedtakskonsument.hentVedtak(100, 100).size)
    }

    @Test
    internal fun `skal returnere liste med ett vedtak når det finnes ett vedtak igjen`() {
        testProducer.send(ProducerRecord(testTopic, objectMapper.valueToTree("{}")))
        assertEquals(1, vedtakskonsument.hentVedtak(100, 100).size)
    }
}
