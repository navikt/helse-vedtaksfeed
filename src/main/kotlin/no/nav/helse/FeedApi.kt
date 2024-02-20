package no.nav.helse

import io.ktor.server.application.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import no.nav.helse.Vedtak.Vedtakstype.SykepengerAnnullert_v1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration

private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
private const val ANTALL_POLL = 5

internal fun Route.feedApi(topic: String, consumer: KafkaConsumer<String, Vedtak>) {
    val topicPartition = TopicPartition(topic, 0)
    consumer.assign(listOf(topicPartition))

    get("/feed") {
        val maksAntall = this.context.parameters["maxAntall"]?.toInt() ?: 100
        val sisteLest = this.context.parameters["sistLesteSekvensId"]?.toLong()
            ?: throw IllegalArgumentException("Parameter sekvensNr cannot be empty")

        val seekTil = if (sisteLest == 0L) 0L else sisteLest + 1L
        consumer.seek(topicPartition, seekTil)

        val records = 0.until(ANTALL_POLL)
            .flatMap { index -> consumer.poll(Duration.ofMillis(500)).also {
                sikkerlogg.info("callId=${call.callId} fikk ${it.count()} meldinger på poll nr ${index + 1}")
                log.info("callId=${call.callId} fikk ${it.count()} meldinger på poll nr ${index + 1}")
            } }
            .takeUnless { sisteLest == 0L && detErBareEnMeldingEnnåOgDetErDenFørstePåTopic(it) }
            ?: emptyList()
        val feed = records
            .take(maksAntall)
            .map { record -> record.toFeedElement() }
            .toFeed(maksAntall)

        "Returnerer ${feed.elementer.size} elementer på feed fra sekvensnr: $sisteLest. Siste sendte sekvensnummer er ${feed.elementer.lastOrNull()?.sekvensId ?: "N/A"} callId=${call.callId}".also {
            log.info(it)
            sikkerlogg.info(it)
        }
        context.respond(feed)
    }
}

/**
 * Polling med sisteLest = 0 når det er én melding der (med offset = 0) vil føre til
 * at man kan polle uendelig mange ganger og få den første meldingen igjen og igjen (vi ser ikke forskjell på
 * første poll med ingen meldinger og første poll med én melding siden Infotrygd i begge tilfeller poller fra 0).
 * Løsningen er at vi ikke leverer noen meldinger før det er (minst) to meldinger på topic-en,
 * slik at neste poll vil lese fra sisteLest = 1 (eller mer).
 */
private fun detErBareEnMeldingEnnåOgDetErDenFørstePåTopic(meldinger: List<ConsumerRecord<String, Vedtak>>) =
    meldinger.size == 1 && meldinger.first().offset() == 0L


private fun List<Feed.Element>.toFeed(maksAntall: Int) = Feed(
    tittel = "SykepengerVedtaksperioder",
    inneholderFlereElementer = maksAntall == size,
    elementer = this
)

private fun ConsumerRecord<String, Vedtak>.toFeedElement() =
    this.value().let { vedtak ->
        Feed.Element(
            type = toExternalName(vedtak.type),
            sekvensId = this.offset(),
            metadata = Feed.Element.Metadata(opprettetDato = vedtak.opprettet.toLocalDate()),
            innhold = Feed.Element.Innhold(
                aktoerId = vedtak.aktørId,
                fnr = vedtak.fødselsnummer,
                foersteStoenadsdag = vedtak.førsteStønadsdag,
                sisteStoenadsdag = vedtak.sisteStønadsdag,
                utbetalingsreferanse = vedtak.førsteFraværsdag,
                forbrukteStoenadsdager = vedtak.forbrukteStønadsdager
            )
        )
    }

/*
Infotrygd har en lengdebegrensning på 21 tegn på type-feltet. For å slippe konflikter med data som allerede ligger på
den interne topic-en beholder vi enum-verdien og tilpasser lengden bare på utgående data.
 */
fun toExternalName(type: Vedtak.Vedtakstype):String =
    if (type == SykepengerAnnullert_v1) "SykepengerAnnullert" else type.name
