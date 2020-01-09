package no.nav.helse

import io.ktor.response.respond
import io.ktor.routing.Route
import io.ktor.routing.get
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

internal fun Route.feedApi(topic: String, consumer: KafkaConsumer<String, Vedtak>) {
    val topicPartition = TopicPartition(topic, 0)
    consumer.assign(listOf(topicPartition))

    get("/feed") {
        val maksAntall = this.context.parameters["maxAntall"]?.toInt() ?: 100
        val sekvensNr = this.context.parameters["sekvensNr"]?.toLong()
            ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")


        consumer.seek(topicPartition, sekvensNr)
        val records = consumer.poll(Duration.ofMillis(1000))
        val feed = records
            .take(maksAntall)
            .map { record -> record.toFeedElement() }
            .toFeed(maksAntall)

        context.respond(feed)
            .also { log.info("Retunerer ${feed.elementer.size} elementer på feed fra sekvensnr: $sekvensNr") }
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
            type = "SykepengerUtbetalt_v1",
            sekvensId = this.offset(),
            metadata = FeedElementMetadata(opprettetDato = vedtak.opprettet),
            innhold = FeedElementInnhold(
                aktoerId = vedtak.aktørId,
                foersteStoenadsdag = vedtak.utbetalingslinjer.first().fom,
                sisteStoenadsdag = vedtak.utbetalingslinjer.last().tom,
                gsakId = null
            )
        )
    }
