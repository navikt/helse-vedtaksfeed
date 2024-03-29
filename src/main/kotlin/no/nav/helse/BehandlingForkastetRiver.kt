package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.*
import java.time.LocalDate

class BehandlingForkastetRiver(rapidsConnection: RapidsConnection, private val vedtaksfeedPublisher: Publisher) :
    River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behandling_forkastet")
                it.requireKey("vedtaksperiodeId", "aktørId", "fødselsnummer")
                it.require("@opprettet", JsonNode::asLocalDateTime)
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (behandling_forkastet): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (behandling_forkastet): $problems")

    }
    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText()
        val fødselsnummer = packet["fødselsnummer"].asText()
        Vedtak(
            type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
            opprettet = packet["@opprettet"].asLocalDateTime(),
            aktørId = packet["aktørId"].textValue(),
            fødselsnummer = fødselsnummer,
            førsteStønadsdag = LocalDate.EPOCH,
            sisteStønadsdag = LocalDate.EPOCH,
            førsteFraværsdag = vedtaksperiodeId,
            forbrukteStønadsdager = 0
        )
            .republish(vedtaksfeedPublisher)
            .also { offset->
                "Republiserer behandling_forkastet for vedtaksperiodeId=$vedtaksperiodeId på intern topic med offset $offset".also {
                    log.info(it)
                    tjenestekallLog.info(it, StructuredArguments.kv("fødselsnummer", fødselsnummer))
                }
            }

    }
}
