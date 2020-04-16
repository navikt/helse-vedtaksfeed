package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class UtbetaltRiver(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, String>,
    private val vedtaksfeedtopic: String
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("fødselsnummer")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        vedtakproducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, packet.toJson())).get()
            .also { log.info("Republiserer vedtak med key:$fødselsnummer på intern topic") }
    }

}
