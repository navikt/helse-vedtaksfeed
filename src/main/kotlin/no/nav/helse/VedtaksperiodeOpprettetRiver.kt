package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

class VedtaksperiodeOpprettetRiver(rapidsConnection: RapidsConnection, private val vedtaksfeedPublisher: Publisher) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_opprettet")
                it.requireKey("vedtaksperiodeId", "aktørId", "fødselsnummer")
                it.require("fom", JsonNode::asLocalDate)
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("tom", JsonNode::asLocalDate)
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (vedtaksperiode_opprettet): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (vedtaksperiode_opprettet): $problems")

    }
    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText()
        val fødselsnummer = packet["fødselsnummer"].asText()
        Vedtak(
            type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
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
                "Republiserer vedtaksperiodeOpprettet for vedtaksperiodeId=$vedtaksperiodeId på intern topic med offset $offset".also {
                    log.info(it)
                    tjenestekallLog.info(it, kv("fødselsnummer", fødselsnummer))
                }
            }

    }
}
