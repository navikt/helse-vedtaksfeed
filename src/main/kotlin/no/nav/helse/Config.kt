package no.nav.helse

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

fun setUpEnvironment() =
    Environment(
        jwksUrl = System.getenv("JWKS_URL"),
        jwtIssuer = System.getenv("JWT_ISSUER")
    )

data class Environment(
    val jwksUrl: String,
    val jwtIssuer: String,
    val vedtaksfeedtopic: String = "tbd.infotrygd.vedtaksfeed.v1"
)

internal class KafkaConfig(
    private val bootstrapServers: String,
    private val truststore: String? = null,
    private val truststorePassword: String? = null,
    private val keystoreLocation: String? = null,
    private val keystorePassword: String? = null,
) {

    internal fun producerConfig() = Properties().apply {
        putAll(kafkaBaseConfig())
        put(ProducerConfig.ACKS_CONFIG, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    }

    internal fun consumerConfig() = Properties().apply {
        putAll(kafkaBaseConfig())
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore)
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword)
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation)
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword)
    }
}
