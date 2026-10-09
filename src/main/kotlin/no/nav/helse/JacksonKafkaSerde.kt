package no.nav.helse

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import tools.jackson.module.kotlin.readValue

class VedtakDeserializer : Deserializer<Vedtak> {
    override fun deserialize(
        topic: String,
        data: ByteArray,
    ) = objectMapper.readValue<Vedtak>(data)
}

class VedtakSerializer : Serializer<Vedtak> {
    override fun serialize(
        topic: String,
        data: Vedtak,
    ): ByteArray = objectMapper.writeValueAsBytes(data)
}
