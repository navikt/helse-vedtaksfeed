package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class UtbetaltRiverV1(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, String>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("fødselsnummer")
                it.requireArray("utbetaling") {
                    requireArray("utbetalingslinjer") {
                        requireKey("fom", "tom", "grad", "dagsats")
                    }
                }
            }
        }.register(this)
    }
    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) =
        packet.republish(vedtakproducer, vedtaksfeedTopic)
}

class UtbetaltRiverV2(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, String>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("fødselsnummer")
                it.requireArray("utbetalingslinjer") { requireKey("fom", "tom", "grad", "dagsats") }
            }
        }.register(this)
    }
    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) =
        packet.republish(vedtakproducer, vedtaksfeedTopic)
}

private fun JsonMessage.republish(
    vedtakproducer: KafkaProducer<String, String>,
    vedtaksfeedtopic: String
) {
    val fødselsnummer = this["fødselsnummer"].asText()
    vedtakproducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, this.toJson())).get()
        .also { log.info("Republiserer vedtak på intern topic") }
}
