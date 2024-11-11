package no.nav.helse

import com.auth0.jwk.JwkProvider
import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.kafka.AivenConfig
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.routing.*
import no.nav.helse.rapids_rivers.RapidApplication
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URI
import java.net.http.HttpClient
import java.util.*

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("vedtaksfeed")

fun main() {
    val vedtaksfeedtopic = "tbd.infotrygd.vedtaksfeed.v1"
    val env = System.getenv()

    val config = AivenConfig.default

    val vedtaksfeedProducer = KafkaProducer(config.producerConfig(Properties()), StringSerializer(), VedtakSerializer())
    val vedtaksfeedConsumer = KafkaConsumer(config.consumerConfig("vedtaksfeed", Properties()), StringDeserializer(), VedtakDeserializer())

    val azureClient = createAzureTokenClientFromEnvironment(env)
    val speedClient = SpeedClient(HttpClient.newHttpClient(), objectMapper, azureClient)

    RapidApplication.create(
        env = env,
        builder = {
            withKtorModule {
                val azureConfig = AzureAdAppConfig(
                    clientId = env.getValue("AZURE_APP_CLIENT_ID"),
                    configurationUrl = env.getValue("AZURE_APP_WELL_KNOWN_URL")
                )
                vedtaksfeed(vedtaksfeedtopic, vedtaksfeedConsumer, azureConfig, speedClient)
            }
        }
    )
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

internal fun Application.vedtaksfeed(
    topic: String,
    consumer: KafkaConsumer<String, Vedtak>,
    azureConfig: AzureAdAppConfig,
    speedClient: SpeedClient
) {
    install(Authentication) {
        jwt {
            azureConfig.configureVerification(this)
        }
    }
    routing {
        authenticate {
            feedApi(topic, consumer, speedClient)
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
