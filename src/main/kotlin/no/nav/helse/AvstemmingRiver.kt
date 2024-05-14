package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.*
import java.time.LocalDate

class AvstemmingRiver(rapidsConnection: RapidsConnection, private val vedtaksfeedPublisher: Publisher) :
    River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "person_avstemt")
                it.requireKey("aktørId", "fødselsnummer")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.requireArray("arbeidsgivere") {
                    requireArray("vedtaksperioder") {
                        requireKey("id")
                        require("fom", JsonNode::asLocalDate)
                        require("tom", JsonNode::asLocalDate)
                    }
                }
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (person_avstemt): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (person_avstemt): $problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].textValue()
        val opprettet = packet["@opprettet"].asLocalDateTime()

        if (opprettet.toLocalDate() > LocalDate.of(2024, 6, 17)) {
            return log.warn("Remove this code, there is no need for it as all persons should've been ofvoted by now.")
        }

        packet["arbeidsgivere"].forEach { arbeidsgiver ->
            arbeidsgiver.path("vedtaksperioder").forEach { vedtaksperiode ->
                val vedtaksperiodeId = vedtaksperiode["id"].asText()
                Vedtak(
                    type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
                    opprettet = opprettet,
                    aktørId = aktørId,
                    fødselsnummer = fødselsnummer,
                    førsteStønadsdag = vedtaksperiode["fom"].asLocalDate(),
                    sisteStønadsdag = vedtaksperiode["tom"].asLocalDate(),
                    førsteFraværsdag = vedtaksperiodeId,
                    forbrukteStønadsdager = 0
                ).republish(vedtaksfeedPublisher).also { offset->
                    "Republiserer person_avstemt for vedtaksperiodeId=$vedtaksperiodeId på intern topic med offset $offset".also {
                        log.info(it)
                        tjenestekallLog.info(it, StructuredArguments.kv("fødselsnummer", fødselsnummer))
                    }
                }
            }
        }
    }
}
