package no.nav.helse

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

const val vaultBase = "/var/run/secrets/nais.io/service_user"
val vaultBasePath: Path = Paths.get(vaultBase)

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(vaultBasePath.resolve("username")),
    password = Files.readString(vaultBasePath.resolve("password"))
)

fun setUpEnvironment() =
    Environment(
        kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        jwksUrl = System.getenv("JWKS_URL"),
        jwtIssuer = System.getenv("JWT_ISSUER"),
        enableAnnullering = System.getenv("NAIS_CLUSTER_NAME") == "dev-fss"
    )

data class Environment(
    val jwksUrl: String,
    val jwtIssuer: String,
    val kafkaBootstrapServers: String,
    val vedtaksfeedtopic: String = "privat-helse-vedtaksfeed-infotrygd",
    val enableAnnullering: Boolean
)

data class ServiceUser(
    val username: String,
    val password: String
)

fun loadBaseConfig(env: Environment, serviceUser: ServiceUser): Properties = Properties().also {
    it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
    it["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
    it["bootstrap.servers"] = env.kafkaBootstrapServers
}

fun Properties.toSeekingConsumer() = Properties().also {
    it.putAll(this)
    it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
    it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
    it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
}

fun Properties.toProducerConfig(): Properties = Properties().also {
    it.putAll(this)
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = VedtakSerializer::class.java
}
