package no.nav.helse

import com.auth0.jwk.JwkProvider
import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.serialization.jackson.*
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.routing.*
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("vedtaksfeed")

fun main() {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    val jwkProvider = JwkProviderBuilder(URL(environment.jwksUrl))
        .cached(10, 24, TimeUnit.HOURS)
        .rateLimited(10, 1, TimeUnit.MINUTES)
        .build()

    val vedtakonpremProducer by lazy { KafkaProducer<String, Vedtak>(loadBaseConfig(environment, serviceUser).toProducerConfig()) }

    val env = System.getenv()
    val aivenConfig = KafkaConfig(
        bootstrapServers = env.getValue("KAFKA_BROKERS"),
        truststore = env.getValue("KAFKA_TRUSTSTORE_PATH"),
        truststorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD"),
        keystoreLocation = env["KAFKA_KEYSTORE_PATH"],
        keystorePassword = env["KAFKA_CREDSTORE_PASSWORD"],
    )
    val vedtakaivenProducer = KafkaProducer(aivenConfig.producerConfig(), StringSerializer(), VedtakSerializer())

    RapidApplication.Builder(
        RapidApplication.RapidApplicationConfig.fromEnv(System.getenv())
    ).withKtorModule {
        vedtaksfeed(environment, jwkProvider, loadBaseConfig(environment, serviceUser))
    }.build().apply {
        register(object : RapidsConnection.StatusListener {
            override fun onReady(rapidsConnection: RapidsConnection) {
                val onpremProps = loadBaseConfig(environment, serviceUser)
                val onpremConsumer = KafkaConsumer<String, Vedtak>(onpremProps.toSeekingConsumer())
                val topicPartition = TopicPartition(environment.onpremVedtaksfeedtopic, 0)
                onpremConsumer.assign(listOf(topicPartition))
                onpremConsumer.seekToEnd(listOf(topicPartition))
                val sisteOffset = onpremConsumer.position(topicPartition) - 1 // subtraherer med 1 fordi vi får offset til _neste_ record, ikke siste
                onpremConsumer.seekToBeginning(listOf(topicPartition))
                var nåværendeOffset = KafkaConsumer(aivenConfig.consumerConfig(), StringDeserializer(), VedtakDeserializer()).use {
                    val aivenTopicPartition = TopicPartition(environment.aivenVedtaksfeedtopic, 0)
                    it.assign(listOf(aivenTopicPartition))
                    it.seekToEnd(listOf(aivenTopicPartition))
                    it.position(aivenTopicPartition) - 1
                }
                while (nåværendeOffset < sisteOffset) {
                    onpremConsumer.poll(Duration.ofSeconds(1))
                        .map { record ->
                            vedtakaivenProducer.send(ProducerRecord(environment.aivenVedtaksfeedtopic, record.key(), record.value()))
                        }
                        .lastOrNull()
                        ?.get()
                        ?.offset()
                        ?.also { nåværendeOffset = it }
                    log.info("sisteOffset=$sisteOffset, nåværendeOffset=$nåværendeOffset, gjenstående=${sisteOffset - nåværendeOffset}")
                }
                onpremConsumer.seekToEnd(listOf(topicPartition))
                val oppdatertSisteOffset = onpremConsumer.position(topicPartition) - 1
                log.info("sisteOffset=$sisteOffset, oppdatertSisteOffset=$oppdatertSisteOffset")
                exitProcess(0)
            }
        })
        setupRivers { fødselsnummer, vedtak ->
            log.info("publiserer vedtak på feed-topic")
            val offsetOnprem = vedtakonpremProducer.send(ProducerRecord(environment.onpremVedtaksfeedtopic, 0, fødselsnummer, vedtak)).get().offset()
            vedtakaivenProducer.send(ProducerRecord(environment.onpremVedtaksfeedtopic, fødselsnummer, vedtak)).get().offset().also { offsetAiven ->
                check (offsetAiven == offsetOnprem)
            }
        }
    }
}

internal fun RapidsConnection.setupRivers(publisher: Publisher) {
    UtbetalingUtbetaltRiver(this, publisher)
    AnnullertRiverV1(this, publisher)
    start()
}

internal fun Application.vedtaksfeed(
    environment: Environment,
    jwkProvider: JwkProvider,
    vedtaksfeedConsumerProps: Properties
) {
    val authenticatedUsers = listOf("srvvedtaksfeed", "srvInfot")

    installJacksonFeature()

    install(Authentication) {
        jwt {
            verifier(jwkProvider, environment.jwtIssuer)
            realm = "Vedtaksfeed"
            validate { credentials ->
                if (credentials.payload.subject in authenticatedUsers) {
                    JWTPrincipal(credentials.payload)
                } else {
                    log.info("${credentials.payload.subject} is not authorized to use this app, denying access")
                    null
                }
            }
        }
    }

    val vedtaksfeedconsumer =
        KafkaConsumer<String, Vedtak>(vedtaksfeedConsumerProps.toSeekingConsumer())

    routing {
        authenticate {
            feedApi(environment.onpremVedtaksfeedtopic, vedtaksfeedconsumer)
        }
    }
}

internal fun Application.installJacksonFeature() {
    install(ContentNegotiation) {
        jackson {
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            registerModule(JavaTimeModule())
        }
    }
}
