package no.nav.helse

import io.ktor.response.respond
import io.ktor.response.respondBytes
import io.ktor.routing.Route
import io.ktor.routing.get
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
        val sekvensNr = this.context.parameters["sekvensNr"]?.toLong()
            ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")


        consumer.seek(topicPartition, sekvensNr)
        val records = consumer.poll(Duration.ofMillis(1000))
        val feed = records
            .take(maksAntall)
            .map { record -> record.toFeedElement() }
            .toFeed(maksAntall)

        context.respond(feed)
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

/*
{
  "tittel": "ForeldrepengerVedtak_v1",
  "inneholderFlereElementer": false,
  "elementer": [
    {
      "type": "ForeldrepengerInnvilget_v1",
      "sekvensId": 11839,
      "innhold": {
        "aktoerId": "1000065039586",
        "foersteStoenadsdag": "2019-04-10",
        "sisteStoenadsdag": "2020-01-06",
        "gsakId": "138504202"
      },
      "metadata": {
        "opprettetDato": "2019-03-18T09:14:09.474+01:00"
      }
    }
  ]
}
 */

/*
{
  "tittel": "string",
  "inneholderFlereElementer": true,
  "elementer": [
    {
      "type": "string",
      "sekvensId": 0,
      "innhold": {},
      "metadata": {}
    }
  ]
}
 */
