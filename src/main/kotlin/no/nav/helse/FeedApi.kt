package no.nav.helse

import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.ktor.server.plugins.callid.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import no.nav.helse.Vedtak.Vedtakstype.SykepengerAnnullert_v1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.UUID
import kotlin.ranges.until

val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
private const val ANTALL_POLL = 5
private const val STANDARD_ANTALL = 100

interface VedtaksfeedConsumer {
    fun lesFeed(sistLesteSekvensId: Long, antall: Int): List<Pair<Long, Vedtak>>

    class KafkaVedtaksfeedConsumer(private val topic: String, private val consumer: KafkaConsumer<String, Vedtak>) : VedtaksfeedConsumer {
        override fun lesFeed(sistLesteSekvensId: Long, antall: Int): List<Pair<Long, Vedtak>> {
            val topicPartition = TopicPartition(topic, 0)
            try {
                consumer.assign(listOf(topicPartition))
                val seekTil = if (sistLesteSekvensId == 0L) 0L else sistLesteSekvensId + 1L
                consumer.seek(topicPartition, seekTil)

                return 0.until(ANTALL_POLL)
                    .flatMap { index ->
                        consumer.poll(Duration.ofMillis(500))
                            .also {
                                sikkerlogg.info("fikk ${it.count()} meldinger på poll nr ${index + 1}")
                                log.info("fikk ${it.count()} meldinger på poll nr ${index + 1}")
                            }
                    }
                    .map { it.offset() to it.value() }
            } finally {
                consumer.assign(emptyList())
            }
        }
    }
}

internal fun Route.feedApi(consumer: VedtaksfeedConsumer, speedClient: SpeedClient) {
    get("/feed") {
        val maksAntall = this.call.parameters["maxAntall"]?.toInt() ?: STANDARD_ANTALL
        val sisteLest = this.call.parameters["sistLesteSekvensId"]?.toLong()
            ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")
        val callId = call.callId ?: UUID.randomUUID().toString()

        val records = consumer.lesFeed(sisteLest, maksAntall)
            .takeUnless { sisteLest == 0L && detErBareEnMeldingEnnåOgDetErDenFørstePåTopic(it) }
            ?: emptyList()
        val feed = records
            .take(maksAntall)
            .map { record -> record.toFeedElement(speedClient, callId) }
            .toFeed(maksAntall)

        "Returnerer ${feed.elementer.size} elementer på feed fra sekvensnr: $sisteLest. Siste sendte sekvensnummer er ${feed.elementer.lastOrNull()?.sekvensId ?: "N/A"} callId=${call.callId}".also {
            log.info(it)
            sikkerlogg.info(it)
        }
        call.respond(feed)
    }
}

/**
 * Polling med sisteLest = 0 når det er én melding der (med offset = 0) vil føre til
 * at man kan polle uendelig mange ganger og få den første meldingen igjen og igjen (vi ser ikke forskjell på
 * første poll med ingen meldinger og første poll med én melding siden Infotrygd i begge tilfeller poller fra 0).
 * Løsningen er at vi ikke leverer noen meldinger før det er (minst) to meldinger på topic-en,
 * slik at neste poll vil lese fra sisteLest = 1 (eller mer).
 */
private fun detErBareEnMeldingEnnåOgDetErDenFørstePåTopic(meldinger: List<Pair<Long, *>>) =
    meldinger.size == 1 && meldinger.first().first == 0L


private fun List<Feed.Element>.toFeed(maksAntall: Int) = Feed(
    tittel = "SykepengerVedtaksperioder",
    inneholderFlereElementer = maksAntall == size,
    elementer = this
)

private fun Pair<Long, Vedtak>.toFeedElement(speedClient: SpeedClient, callId: String): Feed.Element {
    val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(second.fødselsnummer, callId).getOrThrow() }
    return Feed.Element(
        type = toExternalName(second.type),
        sekvensId = first,
        metadata = Feed.Element.Metadata(opprettetDato = second.opprettet.toLocalDate()),
        innhold = Feed.Element.Innhold(
            aktoerId = identer.aktørId,
            fnr = identer.fødselsnummer,
            foersteStoenadsdag = second.førsteStønadsdag,
            sisteStoenadsdag = second.sisteStønadsdag,
            utbetalingsreferanse = second.førsteFraværsdag,
            forbrukteStoenadsdager = second.forbrukteStønadsdager
        )
    )
}

/*
Infotrygd har en lengdebegrensning på 21 tegn på type-feltet. For å slippe konflikter med data som allerede ligger på
den interne topic-en beholder vi enum-verdien og tilpasser lengden bare på utgående data.
 */
fun toExternalName(type: Vedtak.Vedtakstype):String =
    if (type == SykepengerAnnullert_v1) "SykepengerAnnullert" else type.name
