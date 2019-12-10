package no.nav.helse

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties


internal class Vedtakskonsument(private val kafkakonsumentBuilder: VedtakskonsumentBuilder) {
    fun hentVedtak(antall: Int, poll: Int): List<Vedtak> =
        kafkakonsumentBuilder.maxPollRecords(antall).build().use { kafkaConsumer ->
            repeat(poll) {
                kafkaConsumer.poll(Duration.ofMillis(100))
                    .map { record -> record.value() }
                    .takeIf { it.isNotEmpty() }
                    ?.apply { return this }
            }
            emptyList()
        }
}

internal class VedtakskonsumentBuilder {
    private val env: Environment
    private val properties: Properties

    constructor(env: Environment) {
        this.env = env
        this.properties = Properties().also {
            it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
            it["bootstrap.servers"] = env.kafkaBootstrapServers
            it[ConsumerConfig.GROUP_ID_CONFIG] = "vedtaksfeed-consumer"
            it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = VedtakDeserializer::class.java
            it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "100"
            it[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "PLAINTEXT"
            it[SaslConfigs.SASL_MECHANISM] = "PLAIN"
        }
    }

    constructor(env: Environment, serviceUser: ServiceUser) : this(env) {
        this.properties["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
        this.properties[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
    }

    private fun Properties.setMaxPollRecords(maxPollRecords: Int) {
        this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "$maxPollRecords"
    }

    internal fun maxPollRecords(antall: Int): VedtakskonsumentBuilder {
        this.properties.setMaxPollRecords(antall)
        return this
    }

    internal fun build() = KafkaConsumer<String, Vedtak>(properties).also { it.subscribe(listOf(env.vedtakstopic)) }
}
