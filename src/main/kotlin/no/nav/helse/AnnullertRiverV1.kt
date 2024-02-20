package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*

private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

class AnnullertRiverV1(
    rapidsConnection: RapidsConnection,
    private val vedtaksfeedPublisher: Publisher
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "utbetaling_annullert")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.requireKey(
                    "fødselsnummer",
                    "aktørId",
                    "organisasjonsnummer",
                    "utbetalingId",
                    "korrelasjonsId",
                    "fom",
                    "tom"
                )
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.error("Forstod ikke innkommende melding (utbetaling_annullert): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (utbetaling_annullert): $problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val utbetalingId = packet["utbetalingId"].asText()
            val (korrelasjonsId, base32EncodedKorrelasjonsId) = packet.korrelasjonsId()
            val fom = packet["fom"].asLocalDate()
            val tom = packet["tom"].asLocalDate()
            val offset = Vedtak(
                type = Vedtak.Vedtakstype.SykepengerAnnullert_v1,
                opprettet = packet["@opprettet"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].asText(),
                førsteStønadsdag = fom,
                sisteStønadsdag = tom,
                førsteFraværsdag = base32EncodedKorrelasjonsId,
                forbrukteStønadsdager = 0
            ).republish(vedtaksfeedPublisher)
            "Republiserer annullering for utbetalingId=$utbetalingId og korrelasjonsId=$korrelasjonsId ($base32EncodedKorrelasjonsId) på intern topic med offset $offset".also {
                log.info(it)
                tjenestekallLog.info(it)
            }
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }
}

private fun JsonMessage.korrelasjonsId() = UUID.fromString(get("korrelasjonsId").textValue()).let { korrelasjonsId ->
    korrelasjonsId to korrelasjonsId.base32Encode()
}

private fun Vedtak.republish(publisher: Publisher) = publisher(fødselsnummer, this)
