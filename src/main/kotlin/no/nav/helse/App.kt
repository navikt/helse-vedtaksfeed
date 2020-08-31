package no.nav.helse

import com.auth0.jwk.JwkProvider
import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.Application
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.auth.authenticate
import io.ktor.auth.jwt.JWTPrincipal
import io.ktor.auth.jwt.jwt
import io.ktor.features.ContentNegotiation
import io.ktor.jackson.jackson
import io.ktor.routing.routing
import no.nav.helse.rapids_rivers.RapidApplication
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.*
import java.util.concurrent.TimeUnit

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

    val vedtakproducer = KafkaProducer<String, Vedtak>(loadBaseConfig(environment, serviceUser).toProducerConfig())

    RapidApplication.Builder(
        RapidApplication.RapidApplicationConfig.fromEnv(System.getenv())
    ).withKtorModule {
        vedtaksfeed(environment, jwkProvider, loadBaseConfig(environment, serviceUser))
    }.build().apply {
        UtbetaltRiverV1(this, vedtakproducer, environment.vedtaksfeedtopic)
        UtbetaltRiverV2(this, vedtakproducer, environment.vedtaksfeedtopic)
        UtbetaltRiverV3(this, vedtakproducer, environment.vedtaksfeedtopic)
        if (environment.enableAnnullering) AnnullertRiverV1(this, vedtakproducer, environment.vedtaksfeedtopic)
        start()
    }
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
            feedApi(environment.vedtaksfeedtopic, vedtaksfeedconsumer)
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
