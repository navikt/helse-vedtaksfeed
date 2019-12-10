package no.nav.helse

import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer

class VedtakDeserializer : Deserializer<Vedtak> {
    override fun deserialize(topic: String?, data: ByteArray) = objectMapper.readValue<Vedtak>(data)
}
