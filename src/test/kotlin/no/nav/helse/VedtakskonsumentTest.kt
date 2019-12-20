package no.nav.helse

import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.util.Properties
import java.util.concurrent.TimeUnit

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

    private val testVedtakskonsumentBuilder = VedtakskonsumentBuilder(environment)

    internal class JacksonKafkaSerializer : Serializer<Vedtak> {
        override fun serialize(topic: String?, data: Vedtak?): ByteArray = objectMapper.writeValueAsBytes(data)
    }

    private val testKafkaProperties = Properties().also {
        it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
        it["bootstrap.servers"] = environment.kafkaBootstrapServers
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JacksonKafkaSerializer::class.java
        it[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "PLAINTEXT"
        it[SaslConfigs.SASL_MECHANISM] = "PLAIN"
    }

    private val testProducer = KafkaProducer<String, Vedtak>(testKafkaProperties)

    private lateinit var vedtakskonsument: Vedtakskonsument

    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        vedtakskonsument = Vedtakskonsument(testVedtakskonsumentBuilder.build())
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }

    @Test
    internal fun `skal returnere tom liste hvis det ikke finnes noen vedtak`() {
        assertEquals(emptyList<Vedtak>(), vedtakskonsument.hentVedtak(100, 0))
    }

    @Test
    internal fun `skal returnere liste med ett vedtak når det finnes ett vedtak`() {
        testProducer.send(ProducerRecord(testTopic, Vedtak("aktørId", "utbetalingsreferanse", emptyList(), LocalDate.now())))
        assertEquals(1, ventPåVedtak().size)
    }

    private fun ventPåVedtak(): List<Vedtak> {
        var vedtak: List<Vedtak>? = null

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until {
                vedtak = vedtakskonsument.hentVedtak(100, 0)
                vedtak?.size != 0
            }

        return vedtak!!
    }
}
