package no.nav.helse

import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDate

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
        val førsteFraværsdag = packet["førsteFraværsdag"].asLocalDate()
        Vedtak(
            type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
            opprettet = packet["opprettet"].asLocalDateTime(),
            aktørId = packet["aktørId"].textValue(),
            fødselsnummer = packet["fødselsnummer"].textValue(),
            førsteStønadsdag = packet["utbetaling"].flatMap { it["utbetalingslinjer"] }
                .map { it["fom"].asLocalDate() }.filter { it >= førsteFraværsdag }.min().requireNotNull(),
            sisteStønadsdag = packet["utbetaling"].flatMap { it["utbetalingslinjer"] }
                .map { it["tom"].asLocalDate() }.max().requireNotNull(),
            førsteFraværsdag = førsteFraværsdag,
            forbrukteStønadsdager = packet["forbrukteSykedager"].intValue()
        )
            .republish(vedtakproducer, vedtaksfeedTopic)
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
        val førsteFraværsdag = packet["førsteFraværsdag"].asLocalDate()
        Vedtak(
            type = Vedtak.Vedtakstype.SykepengerUtbetalt_v1,
            opprettet = packet["opprettet"].asLocalDateTime(),
            aktørId = packet["aktørId"].textValue(),
            fødselsnummer = packet["fødselsnummer"].textValue(),
            førsteStønadsdag = packet["utbetalingslinjer"].map { it["fom"].asLocalDate() }.filter { it >= førsteFraværsdag }.min().requireNotNull(),
            sisteStønadsdag = packet["utbetalingslinjer"].map { it["tom"].asLocalDate() }.max().requireNotNull(),
            førsteFraværsdag = førsteFraværsdag,
            forbrukteStønadsdager = packet["forbrukteSykedager"].intValue()
        )
            .republish(vedtakproducer, vedtaksfeedTopic)
    }
}

private fun LocalDate?.requireNotNull() = requireNotNull(this) { "Ingen utbetalinger i vedtak" }

private fun Vedtak.republish(
    vedtakproducer: KafkaProducer<String, Vedtak>,
    vedtaksfeedtopic: String
) {
    vedtakproducer.send(ProducerRecord(vedtaksfeedtopic, fødselsnummer, this)).get()
        .also { log.info("Republiserer vedtak på intern topic") }
}
