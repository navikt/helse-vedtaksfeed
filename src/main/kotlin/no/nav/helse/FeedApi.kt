package no.nav.helse

import io.ktor.response.respond
import io.ktor.routing.Route
import io.ktor.routing.get
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.time.LocalDate

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

private fun List<FeedElement>.toFeed(maksAntall: Int) = Feed(
    tittel = "SykepengerVedtaksperioder",
    inneholderFlereElementer = maksAntall == size,
    elementer = this
)

private fun ConsumerRecord<String, Vedtak>.toFeedElement() =
    this.value().let { vedtak ->
        val førsteStønadsdag: LocalDate
        val sisteStønadsdag: LocalDate
        when (vedtak) {
            is Vedtak.VedtakV1 -> {
                førsteStønadsdag = førsteStønadsdag(
                    vedtak.utbetaling.flatMap { it.utbetalingslinjer },
                    vedtak.førsteFraværsdag
                )
                sisteStønadsdag = sisteStønadsdag(vedtak.utbetaling.flatMap { it.utbetalingslinjer })
            }

            is Vedtak.VedtakV2 -> {
                førsteStønadsdag = førsteStønadsdag(
                    vedtak.utbetalingslinjer,
                    vedtak.førsteFraværsdag
                )
                sisteStønadsdag = sisteStønadsdag(vedtak.utbetalingslinjer)
            }
        }

        FeedElement(
            type = Vedtakstype.SykepengerUtbetalt_v1.name,
            sekvensId = this.offset(),
            metadata = FeedElementMetadata(opprettetDato = vedtak.opprettet),
            innhold = FeedElementInnhold(
                aktoerId = vedtak.aktørId,
                foersteStoenadsdag = førsteStønadsdag,
                sisteStoenadsdag = sisteStønadsdag,
                utbetalingsreferanse = vedtak.førsteFraværsdag,
                forbrukteStoenadsdager = vedtak.forbrukteSykedager
            )
        )
    }

private fun sisteStønadsdag(list: List<Utbetalingslinje>) = list.map { it.tom }.max().requireNotNull()

private fun førsteStønadsdag(list: List<Utbetalingslinje>, førsteFraværsdag: LocalDate): LocalDate =
    list.map { it.fom }.filter { it >= førsteFraværsdag }.min().requireNotNull()

private fun LocalDate?.requireNotNull() = requireNotNull(this) { "Ingen utbetalinger i vedtak" }

enum class Vedtakstype {
    SykepengerUtbetalt_v1, SykepengerAnnullert_v1
}
