package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.serialization.Deserializer

class JacksonKafkaDeserializer : Deserializer<JsonNode> {
    override fun deserialize(topic: String?, data: ByteArray): JsonNode = objectMapper.readTree(data)
}
