package no.nav.helse.komponent

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.routing.routing
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.withTestApplication
import no.nav.common.KafkaEnvironment
import no.nav.helse.Environment
import no.nav.helse.Feed
import no.nav.helse.Utbetalingslinje
import no.nav.helse.Vedtak
import no.nav.helse.Vedtakskonsument
import no.nav.helse.VedtakskonsumentBuilder
import no.nav.helse.feedApi
import no.nav.helse.objectMapper
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
import java.time.LocalDate
import java.util.Properties
import kotlin.test.assertFalse

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class FeedTest {
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
        vedtakskonsument = Vedtakskonsument(testVedtakskonsumentBuilder)
    }

    @AfterAll
    fun tearDown() {
        embeddedKafkaEnvironment.close()
    }

    @Test
    internal fun `skal returnere liste med ett vedtak når det finnes ett vedtak`() {
        testProducer.send(
            ProducerRecord(
                testTopic,
                Vedtak(
                    "aktørId",
                    "utbetalingsreferanse",
                    listOf(
                        Utbetalingslinje(
                            LocalDate.of(2019, 12, 1),
                            LocalDate.of(2019, 12, 10),
                            42
                        )
                    ),
                    LocalDate.of(2019, 12, 10)
                )
            )
        )
        withTestApplication({
            routing { feedApi(vedtakskonsument) }
        }) {
            with(handleRequest(HttpMethod.Get, "/feed")) {
                assertEquals(HttpStatusCode.OK, response.status())
                assertFalse(response.content.isNullOrBlank())

                val feed = objectMapper.readValue<Feed>(response.content!!)
                kotlin.test.assertEquals(LocalDate.of(2019, 12, 1), feed.elementer.first().innhold.foersteStoenadsdag)
                kotlin.test.assertEquals(LocalDate.of(2019, 12, 10), feed.elementer.first().innhold.sisteStoenadsdag)
            }
        }
    }
}
