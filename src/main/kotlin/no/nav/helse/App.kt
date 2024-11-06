package no.nav.helse

import com.auth0.jwk.JwkProvider
import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.kafka.AivenConfig
import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers.createDefaultKafkaRapidFromEnv
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.routing.*
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import no.nav.helse.rapids_rivers.RapidApplication.Builder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.URI
import java.util.*

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("vedtaksfeed")
private val httpTraceLog = LoggerFactory.getLogger("tjenestekall")

fun main() {
    val vedtaksfeedtopic = "tbd.infotrygd.vedtaksfeed.v1"
    val env = System.getenv()

    val config = AivenConfig.default
    val factory = ConsumerProducerFactory(config)
    val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    val vedtaksfeedProducer = KafkaProducer(config.producerConfig(Properties()), StringSerializer(), VedtakSerializer())
    val vedtaksfeedConsumer = KafkaConsumer(config.consumerConfig("vedtaksfeed", Properties()), StringDeserializer(), VedtakDeserializer())

    val kafkaRapid = createDefaultKafkaRapidFromEnv(
        factory = factory,
        meterRegistry = meterRegistry,
        env = env
    )

    Builder(
        appName = env["RAPID_APP_NAME"] ?: generateAppName(env),
        instanceId = generateInstanceId(env),
        rapid = kafkaRapid,
        meterRegistry = meterRegistry
    )
        .withKtorModule {
            val azureConfig = AzureAdAppConfig(
                clientId = env.getValue("AZURE_APP_CLIENT_ID"),
                configurationUrl = env.getValue("AZURE_APP_WELL_KNOWN_URL")
            )
            vedtaksfeed(vedtaksfeedtopic, vedtaksfeedConsumer, azureConfig)
        }
        .build()
        .setupRivers { fødselsnummer, vedtak ->
            log.info("publiserer vedtak på feed-topic")
            vedtaksfeedProducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, vedtak)).get().offset()
        }
}

internal fun RapidsConnection.setupRivers(publisher: Publisher) {
    UtbetalingUtbetaltRiver(this, publisher)
    AnnullertRiverV1(this, publisher)
    start()
}

private fun generateInstanceId(env: Map<String, String>): String {
    if (env.containsKey("NAIS_APP_NAME")) return InetAddress.getLocalHost().hostName
    return UUID.randomUUID().toString()
}

private fun generateAppName(env: Map<String, String>): String? {
    val appName = env["NAIS_APP_NAME"] ?: return null
    val namespace = env["NAIS_NAMESPACE"] ?: return null
    val cluster = env["NAIS_CLUSTER_NAME"] ?: return null
    return "$appName-$cluster-$namespace"
}

internal fun Application.vedtaksfeed(
    topic: String,
    consumer: KafkaConsumer<String, Vedtak>,
    azureConfig: AzureAdAppConfig,
) {
    installJacksonFeature()
    install(CallId) {
        header("callId")
        verify { it.isNotEmpty() }
        generate { UUID.randomUUID().toString() }
    }
    install(CallLogging) {
        logger = httpTraceLog
        level = Level.INFO
        disableDefaultColors()
        callIdMdc("callId")
        filter { call -> call.request.path().startsWith("/feed") }
    }
    install(Authentication) {
        jwt {
            azureConfig.configureVerification(this)
        }
    }
    routing {
        authenticate {
            feedApi(topic, consumer)
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

internal class AzureAdAppConfig(private val clientId: String, configurationUrl: String) {
    private val issuer: String
    private val jwkProvider: JwkProvider
    private val jwksUri: String

    init {
        configurationUrl.getJson().also {
            this.issuer = it["issuer"].textValue()
            this.jwksUri = it["jwks_uri"].textValue()
        }

        jwkProvider = JwkProviderBuilder(URI(this.jwksUri).toURL()).build()
    }

    fun configureVerification(configuration: JWTAuthenticationProvider.Config) {
        configuration.verifier(jwkProvider, issuer) {
            withAudience(clientId)
        }
        configuration.validate { credentials -> JWTPrincipal(credentials.payload) }
    }

    private fun String.getJson(): JsonNode {
        val (responseCode, responseBody) = this.fetchUrl()
        if (responseCode >= 300 || responseBody == null) throw RuntimeException("got status $responseCode from ${this}.")
        return jacksonObjectMapper().readTree(responseBody)
    }

    private fun String.fetchUrl() = with(URI(this).toURL().openConnection() as HttpURLConnection) {
        requestMethod = "GET"
        val stream: InputStream? = if (responseCode < 300) this.inputStream else this.errorStream
        responseCode to stream?.bufferedReader()?.readText()
    }
}
