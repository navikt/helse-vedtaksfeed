package no.nav.helse

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.auth.authenticate
import io.ktor.auth.jwt.JWTPrincipal
import io.ktor.auth.jwt.jwt
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
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
    val authenticatedUsers = listOf("srvinfotrygd") // TODO: Finn servicebruker
    val server = embeddedServer(Netty, 8080) {
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

        val konsument = Vedtakskonsument(VedtakskonsumentBuilder(environment, serviceUser).build())

        routing {
            registerHealthApi({ true }, { true }, meterRegistry)
            authenticate {
                feedApi(konsument)
            }
        }
    }.start(wait = false)
    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(10, 10, TimeUnit.SECONDS)
    })
}
