package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

class UtbetalingUtbetaltRiver(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, Vedtak>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "utbetaling_utbetalt")
                it.requireAny("type", listOf("UTBETALING", "REVURDERING"))
                it.require("tidspunkt", JsonNode::asLocalDateTime)
                it.requireKey(
                    "fødselsnummer",
                    "aktørId",
                    "organisasjonsnummer",
                    "utbetalingId",
                    "arbeidsgiverOppdrag",
                    "arbeidsgiverOppdrag.fagsystemId",
                    "arbeidsgiverOppdrag.fom",
                    "arbeidsgiverOppdrag.tom",
                    "arbeidsgiverOppdrag.stønadsdager"
                )
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val utbetalingId = packet["utbetalingId"].asText()
            packet["arbeidsgiverOppdrag"]
                .let { oppdrag ->
                    Vedtak(
                        type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
                        opprettet = packet["tidspunkt"].asLocalDateTime(),
                        aktørId = packet["aktørId"].textValue(),
                        fødselsnummer = packet["fødselsnummer"].asText(),
                        førsteStønadsdag = oppdrag["fom"].asLocalDate(),
                        sisteStønadsdag = oppdrag["tom"].asLocalDate(),
                        førsteFraværsdag = oppdrag["fagsystemId"].textValue(),
                        forbrukteStønadsdager = oppdrag["stønadsdager"].intValue()
                    ).republish(vedtakproducer, vedtaksfeedTopic)
                        .also { log.info("Republiserer vedtak for utbetalingId=${utbetalingId} på intern topic med offset ${it.offset()}") }
                }
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }
}

class AnnullertRiverV1(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, Vedtak>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetaling_annullert")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.requireKey(
                    "fødselsnummer",
                    "aktørId",
                    "organisasjonsnummer",
                    "utbetalingId",
                    "fagsystemId"
                )
                it.requireArray("utbetalingslinjer") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val fagsystemId = packet["fagsystemId"].textValue()
            val utbetalingslinjer = packet["utbetalingslinjer"]
            val fom = requireNotNull(utbetalingslinjer.map { it["fom"].asLocalDate() }.minOrNull())
            val tom = requireNotNull(utbetalingslinjer.map { it["tom"].asLocalDate() }.maxOrNull())
            val utbetalingId = packet["utbetalingId"].asText()
            Vedtak(
                type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
                opprettet = packet["@opprettet"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].asText(),
                førsteStønadsdag = fom,
                sisteStønadsdag = tom,
                førsteFraværsdag = fagsystemId,
                forbrukteStønadsdager = 0
            ).republish(vedtakproducer, vedtaksfeedTopic)
                .also { log.info("Republiserer annullering for utbetalingId=${utbetalingId} på intern topic med offset ${it.offset()}") }
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }
}

private fun Vedtak.republish(vedtakproducer: KafkaProducer<String, Vedtak>, vedtaksfeedtopic: String) =
    vedtakproducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, this)).get()
