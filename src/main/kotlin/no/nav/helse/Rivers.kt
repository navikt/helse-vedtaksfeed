package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.time.LocalDate

private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

class UtbetaltRiverV1(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, Vedtak>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("opprettet", "aktørId", "fødselsnummer", "førsteFraværsdag", "forbrukteSykedager")
                it.requireArray("utbetaling") {
                    requireArray("utbetalingslinjer") {
                        requireKey("fom", "tom", "grad", "dagsats")
                    }
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val førsteFraværsdag = packet["førsteFraværsdag"].asLocalDate()
            val (førsteStønadsdag, sisteStønadsdag) =
                packet["utbetaling"].flatMap { it["utbetalingslinjer"] }.stønadsdager(førsteFraværsdag)
            Vedtak(
                type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
                opprettet = packet["opprettet"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].textValue(),
                førsteStønadsdag = førsteStønadsdag,
                sisteStønadsdag = sisteStønadsdag,
                førsteFraværsdag = førsteFraværsdag.toString(),
                forbrukteStønadsdager = packet["forbrukteSykedager"].intValue()
            )
                .republish(vedtakproducer, vedtaksfeedTopic)
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }
}

class UtbetaltRiverV2(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, Vedtak>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("opprettet", "aktørId", "fødselsnummer", "førsteFraværsdag", "forbrukteSykedager")
                it.requireArray("utbetalingslinjer") { requireKey("fom", "tom", "grad", "dagsats") }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val førsteFraværsdag = packet["førsteFraværsdag"].asLocalDate()
            val (førsteStønadsdag, sisteStønadsdag) =
                packet["utbetalingslinjer"].toList().stønadsdager(førsteFraværsdag)
            Vedtak(
                type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
                opprettet = packet["opprettet"].asLocalDateTime(),
                aktørId = packet["aktørId"].textValue(),
                fødselsnummer = packet["fødselsnummer"].textValue(),
                førsteStønadsdag = førsteStønadsdag,
                sisteStønadsdag = sisteStønadsdag,
                førsteFraværsdag = førsteFraværsdag.toString(),
                forbrukteStønadsdager = packet["forbrukteSykedager"].intValue()
            )
                .republish(vedtakproducer, vedtaksfeedTopic)
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }

}

class UtbetaltRiverV3(
    rapidsConnection: RapidsConnection,
    private val vedtakproducer: KafkaProducer<String, Vedtak>,
    private val vedtaksfeedTopic: String
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey("opprettet", "aktørId", "fødselsnummer")
                it.requireArray("utbetalt") {
                    requireArray("utbetalingslinjer") {
                        requireKey("fom", "tom", "sykedager")
                    }
                    requireKey("fagsystemId", "totalbeløp")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            packet["utbetalt"].forEach { utbetaling ->
                val fagsystemId = utbetaling["fagsystemId"].textValue()
                utbetaling["utbetalingslinjer"].forEach { linje ->
                    Vedtak(
                        type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
                        opprettet = packet["opprettet"].asLocalDateTime(),
                        aktørId = packet["aktørId"].textValue(),
                        fødselsnummer = packet["fødselsnummer"].asText(),
                        førsteStønadsdag = LocalDate.parse(linje["fom"].asText()),
                        sisteStønadsdag = LocalDate.parse(linje["tom"].asText()),
                        førsteFraværsdag = fagsystemId,
                        forbrukteStønadsdager = linje["sykedager"].intValue()
                    ).republish(vedtakproducer, vedtaksfeedTopic)
                }
            }
        } catch (e: Exception) {
            tjenestekallLog.error("Melding feilet ved konvertering til internt format:\n${packet.toJson()}")
            throw e
        }
    }

}

private fun List<JsonNode>.stønadsdager(førsteFraværsdag: LocalDate): Pair<LocalDate, LocalDate> {
    if (size == 1) return first()["fom"].asLocalDate() to first()["tom"].asLocalDate()
    val førsteStønadsdag = map { it["fom"].asLocalDate() }.filter { it >= førsteFraværsdag }.min().requireNotNull()
    val sisteStønadsdag = map { it["tom"].asLocalDate() }.max().requireNotNull()
    return førsteStønadsdag to sisteStønadsdag
}

private fun LocalDate?.requireNotNull() = requireNotNull(this) { "Ingen utbetalinger i vedtak" }

private fun Vedtak.republish(
    vedtakproducer: KafkaProducer<String, Vedtak>,
    vedtaksfeedtopic: String
) {
    vedtakproducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, this)).get()
        .also { log.info("Republiserer vedtak på intern topic med offset ${it.offset()}") }
}
