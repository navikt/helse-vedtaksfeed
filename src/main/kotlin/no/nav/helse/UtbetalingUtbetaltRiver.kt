package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import java.time.LocalDate
import org.slf4j.LoggerFactory
import tools.jackson.databind.JsonNode
import java.util.*

internal val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

internal typealias Publisher = (String, Vedtak) -> Long
internal fun Vedtak.republish(publisher: Publisher) = publisher(fødselsnummer, this)

class UtbetalingUtbetaltRiver(
    rapidsConnection: RapidsConnection,
    private val vedtaksfeedPublisher: Publisher,
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "utbetaling_utbetalt") }
            validate {
                it.requireAny("type", listOf("UTBETALING", "REVURDERING"))
                it.require("tidspunkt", JsonNode::asLocalDateTime)
                it.requireKey(
                    "fødselsnummer",
                    "utbetalingId",
                    "stønadsdager",
                    "korrelasjonsId",
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

    override fun onError(problems: MessageProblems, context: MessageContext, messageMetadata: MessageMetadata) {
        tjenestekallLog.error("Forstod ikke innkommende melding (utbetaling_utbetalt): ${problems.toExtendedReport()}")
        log.error("Forstod ikke innkommende melding (utbetaling_utbetalt): $problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        try {
            val utbetalingId = packet["utbetalingId"].asString()
            val (korrelasjonsId, base32EncodedKorrelasjonsId) = packet.korrelasjonsId()

            val (type, fom: LocalDate, tom: LocalDate) = packet.førsteStønadsdagIkkeOpphørt.let { fomIkkeOpphørt ->
                val fomUavhengigAvOpphør = packet.førsteStønadsdagUavhengigAvOpphør
                val tomUavhengigAvOpphør = packet.sisteStønadsdagUavhengigAvOpphør

                if (fomIkkeOpphørt == null) {
                    tjenestekallLog.info(
                        "Sender SykepengerAnnullert_v1 i stedet for SykepengerUtbetalt_v1 for {} med fom/tom {}/{}",
                        packet.korrelasjonsId(),
                        fomUavhengigAvOpphør,
                        tomUavhengigAvOpphør
                    )
                    Triple(Vedtak.Vedtakstype.SykepengerAnnullert_v1, fomUavhengigAvOpphør, tomUavhengigAvOpphør)
                } else {
                    val tomIkkeOpphørt = packet.sisteStønadsdagIkkeOpphørt
                    val endretFom = (fomIkkeOpphørt != fomUavhengigAvOpphør)
                    val endretTom = (tomIkkeOpphørt != tomUavhengigAvOpphør)
                    if (endretFom || endretTom) {
                        tjenestekallLog.info(
                            "Endret fom/tom for SykepengerUtbetalt_v1 for {} var {}/{} ble {}/{}",
                            packet.korrelasjonsId(),
                            fomUavhengigAvOpphør,
                            tomUavhengigAvOpphør,
                            fomIkkeOpphørt,
                            tomIkkeOpphørt
                        )
                    }
                    Triple(Vedtak.Vedtakstype.SykepengerUtbetalt_v1, fomIkkeOpphørt, tomIkkeOpphørt)
                }
            }

            Vedtak(
                type = type,
                opprettet = packet["tidspunkt"].asLocalDateTime(),
                fødselsnummer = packet["fødselsnummer"].asString(),
                førsteStønadsdag = fom,
                sisteStønadsdag = tom,
                førsteFraværsdag = base32EncodedKorrelasjonsId, // dette har blitt nøkkelen som beskriver VL-linja i Infotrygd. Kan ikke endre på kontrakten nå.
                forbrukteStønadsdager = packet.forbrukteStønadsdager()
            )
                .also {
                    if (it.forbrukteStønadsdager > 5_000) {
                        log.info("Utbetalt til maksdato i ny løsning for utbetalingId=$utbetalingId")
                        sikkerlogg.info("Utbetalt til maksdato i ny løsning for utbetalingId=$utbetalingId, fnr=${it.fødselsnummer}")
                    }
                }
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

    private val JsonMessage.førsteStønadsdagIkkeOpphørt get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].firstOrNull { !it.erOpphørt() }?.path("fom")?.asLocalDate(),
        this["personOppdrag.linjer"].firstOrNull { !it.erOpphørt() }?.path("fom")?.asLocalDate()
    ).minOrNull()

    private val JsonMessage.sisteStønadsdagIkkeOpphørt get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].lastOrNull { !it.erOpphørt() }?.path("tom")?.asLocalDate(),
        this["personOppdrag.linjer"].lastOrNull { !it.erOpphørt() }?.path("tom")?.asLocalDate()
    ).max()

    private val JsonMessage.førsteStønadsdagUavhengigAvOpphør get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].values().map { it.path("fom").asLocalDate() },
        this["personOppdrag.linjer"].values().map { it.path("fom").asLocalDate() }
    ).flatten().min()

    private val JsonMessage.sisteStønadsdagUavhengigAvOpphør get() = listOfNotNull(
        this["arbeidsgiverOppdrag.linjer"].values().map { it.path("tom").asLocalDate() },
        this["personOppdrag.linjer"].values().map { it.path("tom").asLocalDate() }
    ).flatten().max()

    private fun JsonNode.erOpphørt():Boolean {
        if (!this.has("statuskode")) return false
        if (!this["statuskode"].isString) return false
        return this["statuskode"].asString() == "OPPH"
    }
}

private fun JsonMessage.korrelasjonsId() = UUID.fromString(get("korrelasjonsId").stringValue()).let { korrelasjonsId ->
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


