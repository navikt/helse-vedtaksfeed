package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class VedtakDeserializer : Deserializer<Vedtak> {
    override fun deserialize(topic: String, data: ByteArray) =
        objectMapper.readValue<Vedtak>(data)
}

class VedtakSerializer : Serializer<Vedtak> {
    override fun serialize(topic: String, data: Vedtak): ByteArray =
        objectMapper.writeValueAsBytes(data)
}
