package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer

class VedtakDeserializer : Deserializer<Vedtak> {
    override fun deserialize(topic: String?, data: ByteArray): Vedtak {
        val json = objectMapper.readValue<JsonNode>(data)
        return when {
            json.has("utbetaling") -> objectMapper.readValue<Vedtak.VedtakV1>(data)
            json.has("utbetalingslinjer") -> objectMapper.readValue<Vedtak.VedtakV2>(data)
            else -> throw RuntimeException("ukjent meldingstype på topic")
        }
    }
}
