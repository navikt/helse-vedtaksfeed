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
                    "fom",
                    "tom",
                    "stønadsdager",
                    "arbeidsgiverOppdrag",
                    "arbeidsgiverOppdrag.fagsystemId",
                )
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (utbetaling_utbetalt): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (utbetaling_utbetalt): $problems")
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
                        førsteStønadsdag = packet["fom"].asLocalDate(),
                        sisteStønadsdag = packet["tom"].asLocalDate(),
                        // I påvente av at utbetalingen har sin egen "fagsystenId" bruker vi den fra arbeidsgiveroppdraget
                        førsteFraværsdag = oppdrag["fagsystemId"].textValue(),
                        forbrukteStønadsdager = packet["stønadsdager"].intValue()
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
                    "arbeidsgiverFagsystemId",
                    "fom",
                    "tom"
                )
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val arbeidsgiverFagsystemId = packet["arbeidsgiverFagsystemId"].textValue()
            val fom = packet["fom"].asLocalDate()
            val tom = packet["tom"].asLocalDate()
            val utbetalingId = packet["utbetalingId"].asText()
            Vedtak(
                type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
                opprettet = packet["@opprettet"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].asText(),
                førsteStønadsdag = fom,
                sisteStønadsdag = tom,
                førsteFraværsdag = arbeidsgiverFagsystemId,
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
