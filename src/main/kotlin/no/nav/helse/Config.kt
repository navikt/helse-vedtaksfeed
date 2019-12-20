package no.nav.helse

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import kotlin.reflect.KClass

const val vaultBase = "/var/run/secrets/nais.io/vault"
val vaultBasePath: Path = Paths.get(vaultBase)

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(vaultBasePath.resolve("username")),
    password = Files.readString(vaultBasePath.resolve("password"))
)

fun setUpEnvironment() =
    Environment(
        kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        jwksUrl = System.getenv("JWKS_URL"),
        jwtIssuer = System.getenv("JWT_ISSUER")
    )

data class Environment(
    val jwksUrl: String,
    val jwtIssuer: String,
    val kafkaBootstrapServers: String,
    val vedtakstopic: String = "privat-helse-sykepenger-vedtak",
    val vedtaksfeedtopic: String = "privat-helse-vedtaksfeed-infotrygd"
)

data class ServiceUser(
    val username: String,
    val password: String
)

@FlowPreview
fun <K, V> KafkaConsumer<K, V>.asFlow(): Flow<Pair<K, V>> = flow { while (true) emit(poll(Duration.ZERO)) }
    .onEach { if (it.isEmpty) delay(100) }
    .flatMapConcat { it.asFlow() }
    .map { it.key() to it.value() }


fun loadBaseConfig(env: Environment, serviceUser: ServiceUser): Properties = Properties().also {
    it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
    it["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
    it["bootstrap.servers"] = env.kafkaBootstrapServers
}

fun Properties.toConsumerConfig(): Properties = Properties().also {
    it.putAll(this)
    it[ConsumerConfig.GROUP_ID_CONFIG] = "vedtaksfeed-consumer"
    it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
    it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
}

fun Properties.toSeekingConsumer() = Properties().also {
    it.putAll(this)
    it.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
}

fun Properties.toProducerConfig(): Properties = Properties().also {
    it.putAll(this)
    it[ConsumerConfig.GROUP_ID_CONFIG] = "vedtaksfeed-producer"
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
}
