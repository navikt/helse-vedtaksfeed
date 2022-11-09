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
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.concurrent.TimeUnit

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("vedtaksfeed")

fun main() {
    val environment = setUpEnvironment()

    val jwkProvider = JwkProviderBuilder(URL(environment.jwksUrl))
        .cached(10, 24, TimeUnit.HOURS)
        .rateLimited(10, 1, TimeUnit.MINUTES)
        .build()

    val env = System.getenv()
    val aivenConfig = KafkaConfig(
        bootstrapServers = env.getValue("KAFKA_BROKERS"),
        truststore = env.getValue("KAFKA_TRUSTSTORE_PATH"),
        truststorePassword = env.getValue("KAFKA_CREDSTORE_PASSWORD"),
        keystoreLocation = env["KAFKA_KEYSTORE_PATH"],
        keystorePassword = env["KAFKA_CREDSTORE_PASSWORD"],
    )

    RapidApplication.Builder(
        RapidApplication.RapidApplicationConfig.fromEnv(System.getenv())
    ).withKtorModule {
        vedtaksfeed(environment, jwkProvider, KafkaConsumer(aivenConfig.consumerConfig(), StringDeserializer(), VedtakDeserializer()))
    }.build().apply {
        val vedtakaivenProducer = KafkaProducer(aivenConfig.producerConfig(), StringSerializer(), VedtakSerializer())
        setupRivers { fødselsnummer, vedtak ->
            log.info("publiserer vedtak på feed-topic")
            vedtakaivenProducer.send(ProducerRecord(environment.vedtaksfeedtopic, fødselsnummer, vedtak)).get().offset()
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
    consumer: KafkaConsumer<String, Vedtak>
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

    routing {
        authenticate {
            feedApi(environment.vedtaksfeedtopic, consumer)
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
