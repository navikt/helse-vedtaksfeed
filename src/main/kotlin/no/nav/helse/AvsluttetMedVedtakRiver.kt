package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.*

class AvsluttetMedVedtakRiver(rapidsConnection: RapidsConnection, private val vedtaksfeedPublisher: Publisher) :
    River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "avsluttet_med_vedtak")
                it.requireKey("vedtaksperiodeId", "aktørId", "fødselsnummer")
                it.require("fom", JsonNode::asLocalDate)
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("tom", JsonNode::asLocalDate)
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (avsluttet_med_vedtak): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (avsluttet_med_vedtak): $problems")

    }
    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText()
        val fødselsnummer = packet["fødselsnummer"].asText()
        Vedtak(
            type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
            opprettet = packet["@opprettet"].asLocalDateTime(),
            aktørId = packet["aktørId"].textValue(),
            fødselsnummer = fødselsnummer,
            førsteStønadsdag = packet["fom"].asLocalDate(),
            sisteStønadsdag = packet["tom"].asLocalDate(),
            førsteFraværsdag = vedtaksperiodeId,
            forbrukteStønadsdager = 0
        )
            .republish(vedtaksfeedPublisher)
            .also { offset->
                "Republiserer vedtaksperiodeForkastet for vedtaksperiodeId=$vedtaksperiodeId på intern topic med offset $offset".also {
                    log.info(it)
                    tjenestekallLog.info(it, StructuredArguments.kv("fødselsnummer", fødselsnummer))
                }
            }

    }
}
