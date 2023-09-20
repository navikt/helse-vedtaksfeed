package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*

private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

internal typealias Publisher = (String, Vedtak) -> Long

class UtbetalingUtbetaltRiver(
    rapidsConnection: RapidsConnection,
    private val vedtaksfeedPublisher: Publisher,
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
                    "korrelasjonsId",
                    "maksdato",
                    "gjenståendeSykedager"
                )
                it.requireArray("arbeidsgiverOppdrag.linjer") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
                it.requireArray( "personOppdrag.linjer") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
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
            val (korrelasjonsId, base32EncodedKorrelasjonsId) = packet.korrelasjonsId()

            Vedtak(
                type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
                opprettet = packet["tidspunkt"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].asText(),
                førsteStønadsdag = packet.førsteStønadsdag,
                sisteStønadsdag = packet.sisteStønadsdag,
                førsteFraværsdag = base32EncodedKorrelasjonsId,
                forbrukteStønadsdager = packet.forbrukteStønadsdager()
            )
                .also { if (it.forbrukteStønadsdager > 5_000) log.info("Utbetalt til maksdato i ny løsning for ${it.aktørId}") }
                .republish(vedtaksfeedPublisher)
                .also { offset->
                    "Republiserer vedtak for utbetalingId=$utbetalingId og korrelasjonsId=$korrelasjonsId ($base32EncodedKorrelasjonsId) på intern topic med offset $offset".also {
                        log.info(it)
                        tjenestekallLog.info(it)
                    }
                }
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }

    private val JsonMessage.førsteStønadsdag get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].firstOrNull()?.path("fom")?.asLocalDate(),
        this["personOppdrag.linjer"].firstOrNull()?.path("fom")?.asLocalDate()
    ).min()

    private val JsonMessage.sisteStønadsdag get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].lastOrNull()?.path("tom")?.asLocalDate(),
        this["personOppdrag.linjer"].lastOrNull()?.path("tom")?.asLocalDate()
    ).max()

}

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

/**
 * Infotrygd har implementert visning av "utbetalt til maksdato i ny løsning" i skjermbildet sitt (GE VL) på følgende måte:
 * hvis de mottar et tall som er over 5 000 flagger de "nådd maksdato" (og trekker selvsagt fra 5 000 på forbrukte dager).
 */
private fun JsonMessage.forbrukteStønadsdager(): Int {
    val stønadsdagerPåEkte = this["stønadsdager"].intValue()
    val gjenståendeSykedager = this["gjenståendeSykedager"].intValue()
    return if (gjenståendeSykedager > 0) stønadsdagerPåEkte else stønadsdagerPåEkte + 5_000
}

private fun Vedtak.republish(publisher: Publisher) = publisher(fødselsnummer, this)


