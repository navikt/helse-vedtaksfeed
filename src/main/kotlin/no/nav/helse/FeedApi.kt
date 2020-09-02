package no.nav.helse

import io.ktor.response.*
import io.ktor.routing.*
import no.nav.helse.Vedtak.Vedtakstype.SykepengerUtbetalt_v1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

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
            .filter { it.value().type == SykepengerUtbetalt_v1 }
            .take(maksAntall)
            .map { record -> record.toFeedElement() }
            .toFeed(maksAntall)

        context.respond(feed)
            .also { log.info("Returnerer ${feed.elementer.size} elementer på feed fra sekvensnr: $sisteLest") }
    }
}

private fun List<Feed.Element>.toFeed(maksAntall: Int) = Feed(
    tittel = "SykepengerVedtaksperioder",
    inneholderFlereElementer = maksAntall == size,
    elementer = this
)

private fun ConsumerRecord<String, Vedtak>.toFeedElement() =
    this.value().let { vedtak ->
        Feed.Element(
            type = vedtak.type.name,
            sekvensId = this.offset(),
            metadata = Feed.Element.Metadata(opprettetDato = vedtak.opprettet.toLocalDate()),
            innhold = Feed.Element.Innhold(
                aktoerId = vedtak.aktørId,
                foersteStoenadsdag = vedtak.førsteStønadsdag,
                sisteStoenadsdag = vedtak.sisteStønadsdag,
                utbetalingsreferanse = vedtak.førsteFraværsdag,
                forbrukteStoenadsdager = vedtak.forbrukteStønadsdager
            )
        )
    }
