package no.nav.helse

import com.auth0.jwk.JwkProviderBuilder
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
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("vedtaksfeed")

@FlowPreview
fun main() = runBlocking {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    val jwkProvider = JwkProviderBuilder(URL(environment.jwksUrl))
        .cached(10, 24, TimeUnit.HOURS)
        .rateLimited(10, 1, TimeUnit.MINUTES)
        .build()
    val authenticatedUsers = listOf("srvinfotrygd") // TODO: Finn servicebruker
    val server = embeddedServer(Netty, 8080) {
        installJacksonFeature()
        install(MicrometerMetrics) {
            registry = meterRegistry
        }

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
            KafkaConsumer<String, Vedtak>(loadBaseConfig(environment, serviceUser).toSeekingConsumer())

        routing {
            registerHealthApi({ true }, { true }, meterRegistry)
            authenticate {
                feedApi(environment.vedtaksfeedtopic, vedtaksfeedconsumer)
            }
        }
    }.start(wait = false)

    val vedtakconsumer =
        KafkaConsumer<ByteArray, ByteArray>(loadBaseConfig(environment, serviceUser).toConsumerConfig())
    val vedtakproducer =
        KafkaProducer<ByteArray, ByteArray>(loadBaseConfig(environment, serviceUser).toProducerConfig())

    vedtakconsumer
        .subscribe(listOf(environment.vedtakstopic))

    vedtakconsumer.asFlow()
        .collect { (key, value) -> vedtakproducer.send(ProducerRecord(environment.vedtaksfeedtopic, key, value)) }

    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(10, 10, TimeUnit.SECONDS)
    })
}

internal fun Application.installJacksonFeature() {
    install(ContentNegotiation) {
        jackson {
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            registerModule(JavaTimeModule())
        }
    }
}
