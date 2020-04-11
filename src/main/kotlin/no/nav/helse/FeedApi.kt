package no.nav.helse

import io.ktor.response.respond
import io.ktor.routing.Route
import io.ktor.routing.get
import org.apache.commons.codec.binary.Base32
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.nio.ByteBuffer
import java.time.Duration
import java.time.LocalDate
import java.util.*

internal fun Route.feedApi(topic: String, consumer: KafkaConsumer<String, Vedtak>) {
    val topicPartition = TopicPartition(topic, 0)
    consumer.assign(listOf(topicPartition))

    get("/feed") {
        val maksAntall = this.context.parameters["maxAntall"]?.toInt() ?: 100
        val sisteLest = this.context.parameters["sistLesteSekvensId"]?.toLong()
            ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")

        consumer.poll(Duration.ofMillis(1000))
        val seekTil = if (sisteLest == 0L) 0L else sisteLest + 1L
        consumer.seek(topicPartition, seekTil)
        val records = consumer.poll(Duration.ofMillis(1000))
        val feed = (records
            .takeUnless { records.count() == 1 && sisteLest == 0L && records.first().offset() == 0L }
            ?: ConsumerRecords.empty())
            .take(maksAntall)
            .map { record -> record.toFeedElement() }
            .toFeed(maksAntall)

        context.respond(feed)
            .also { log.info("Retunerer ${feed.elementer.size} elementer på feed fra sekvensnr: $sisteLest") }
    }
}

fun List<FeedElement>.toFeed(maksAntall: Int) = Feed(
    tittel = "SykepengerVedtaksperioder",
    inneholderFlereElementer = maksAntall == size,
    elementer = this
)

fun ConsumerRecord<String, Vedtak>.toFeedElement() =
    this.value().let { vedtak ->
        FeedElement(
            type = Vedtakstype.SykepengerUtbetalt_v1.name,
            sekvensId = this.offset(),
            metadata = FeedElementMetadata(opprettetDato = vedtak.opprettet),
            innhold = FeedElementInnhold(
                aktoerId = vedtak.aktørId,
                foersteStoenadsdag = vedtak.utbetaling.flatMap { it.utbetalingslinjer }.map { it.fom }.min()
                    .requireNotNull(),
                sisteStoenadsdag = vedtak.utbetaling.flatMap { it.utbetalingslinjer }.map { it.tom }.max()
                    .requireNotNull(),
                utbetalingsreferanse = vedtak.gruppeId.base32Encode(),
                forbrukteStoenadsdager = vedtak.forbrukteSykedager
            )
        )
    }

private fun LocalDate?.requireNotNull() = requireNotNull(this) { "Ingen utbetalinger i vedtak" }

private fun UUID.base32Encode(): String {
    val pad = '='
    return Base32(pad.toByte())
        .encodeAsString(this.byteArray())
        .replace(pad.toString(), "")
}

private fun UUID.byteArray() = ByteBuffer.allocate(Long.SIZE_BYTES * 2).apply {
    putLong(this@byteArray.mostSignificantBits)
    putLong(this@byteArray.leastSignificantBits)
}.array()

enum class Vedtakstype {
    SykepengerUtbetalt_v1, SykepengerAnnullert_v1
}
