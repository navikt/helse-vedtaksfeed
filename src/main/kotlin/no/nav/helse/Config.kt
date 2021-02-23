package no.nav.helse

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
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
        truststorePath = System.getenv("NAV_TRUSTSTORE_PATH"),
        truststorePassword = System.getenv("NAV_TRUSTSTORE_PASSWORD")
    )

data class Environment(
    val jwksUrl: String,
    val jwtIssuer: String,
    val kafkaBootstrapServers: String,
    val vedtaksfeedtopic: String = "privat-helse-vedtaksfeed-infotrygd",
    val truststorePath: String?,
    val truststorePassword: String?
)

data class ServiceUser(
    val username: String,
    val password: String
)

fun loadBaseConfig(env: Environment, serviceUser: ServiceUser): Properties = Properties().apply {
    put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.kafkaBootstrapServers)
    put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
    put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
    )
    put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    env.truststorePath?.also { put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, File(env.truststorePath).absolutePath) }
    env.truststorePassword?.also { put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.truststorePassword) }
}

fun Properties.toSeekingConsumer() = Properties().also {
    it.putAll(this)
    it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "1000"
    it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
    it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
}

fun Properties.toProducerConfig(): Properties = Properties().also {
    it.putAll(this)
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = VedtakSerializer::class.java
    put(ProducerConfig.ACKS_CONFIG, "1")
    put(ProducerConfig.LINGER_MS_CONFIG, "0")
    put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
}
