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
import io.ktor.auth.*
import io.ktor.auth.jwt.*
import io.ktor.features.*
import io.ktor.jackson.*
import io.ktor.routing.*
import no.nav.helse.rapids_rivers.RapidApplication
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.time.Duration
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

    if (System.getenv("NAIS_CLUSTER_NAME") == "prod-fss") {
        seekToCheckpoint(environment, serviceUser)
        printProducerOffset(environment, serviceUser)
    }

    val vedtakproducer = KafkaProducer<String, Vedtak>(loadBaseConfig(environment, serviceUser).toProducerConfig())

    RapidApplication.Builder(
        RapidApplication.RapidApplicationConfig.fromEnv(System.getenv())
    ).withKtorModule {
        vedtaksfeed(environment, jwkProvider, loadBaseConfig(environment, serviceUser))
    }.build().apply {
        UtbetaltRiverV1(this, vedtakproducer, environment.vedtaksfeedtopic)
        UtbetaltRiverV2(this, vedtakproducer, environment.vedtaksfeedtopic)
        UtbetaltRiverV3(this, vedtakproducer, environment.vedtaksfeedtopic)
        AnnullertRiverV1(this, vedtakproducer, environment.vedtaksfeedtopic)
        start()
    }
}

private fun seekToCheckpoint(environment: Environment, serviceUser: ServiceUser) {
    val rapidTopic = System.getenv("KAFKA_RAPID_TOPIC") ?: error("Kunne ikke finne kafka rapid topic")
    val topicPartitions = mapOf(
        TopicPartition(rapidTopic, 5) to 55471202L,
        TopicPartition(rapidTopic, 3) to 55986254L,
        TopicPartition(rapidTopic, 4) to 56135690L,
        TopicPartition(rapidTopic, 2) to 56098871L,
        TopicPartition(rapidTopic, 1) to 56354532L,
        TopicPartition(rapidTopic, 0) to 85757832L
    )
    KafkaConsumer<ByteArray, ByteArray>(loadBaseConfig(environment, serviceUser).toConsumer()).use { consumer ->
        consumer.subscribe(listOf(rapidTopic))
        while (consumer.poll(Duration.ofSeconds(1)).count() == 0) {
            log.info("Poll returnerte ingen elementer, venter...")
        }
        topicPartitions.forEach { (topicPartition, offset) ->
            consumer.seek(topicPartition, offset)
            log.info("Seeker frem til $offset for ${topicPartition.topic()}-${topicPartition.partition()}")
        }
    }
}

private fun printProducerOffset(environment: Environment, serviceUser: ServiceUser) {
    KafkaConsumer<String, Vedtak>(loadBaseConfig(environment, serviceUser).toSeekingConsumer()).use { consumer ->
        val partitions = consumer.partitionsFor(environment.vedtaksfeedtopic).map {
            TopicPartition(environment.vedtaksfeedtopic, it.partition())
        }
        consumer.endOffsets(partitions)
            .forEach { (partition, offset) ->
                log.info("Current end offset for topic=${partition.topic()}, partition=${partition.partition()} is $offset")
            }
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
