package no.nav.helse

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

const val vaultBase = "/var/run/secrets/nais.io/vault"
val vaultBasePath: Path = Paths.get(vaultBase)

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(vaultBasePath.resolve("username")),
    password = Files.readString(vaultBasePath.resolve("password"))
)

fun setUpEnvironment() =
    Environment(
        kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        jwksUrl = System.getenv("JWKS_URL"),
        jwtIssuer = System.getenv("JWT_ISSUER")
    )

data class Environment(
    val jwksUrl: String,
    val jwtIssuer: String,
    val kafkaBootstrapServers: String,
    val vedtakstopic: String = "privat-helse-sykepenger-vedtak"
)

data class ServiceUser(
    val username: String,
    val password: String
)
