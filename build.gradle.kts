plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.AppKt"
}

dependencies {
    implementation(libs.tbdLibs.naisfulApp)
    implementation(libs.ktor.server.auth.jwt)
    implementation(libs.commons.codec)

    implementation(libs.rapidsAndRivers)
    implementation(libs.tbdLibs.azureTokenClientDefault)
    implementation(libs.tbdLibs.retry)
    implementation(libs.tbdLibs.speedClient)

    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
    testImplementation(libs.tbdLibs.naisfulTestApp)
    testImplementation(libs.tbdLibs.kafkaTest)
    testImplementation(libs.mockk)
    testImplementation(libs.awaitility)
    testImplementation(libs.tbdLibs.signedJwtIssuerTest) {
        // Jetty-BOM-en fra no.nav.sykepenger.kotlin er Jetty 12, mens org.wiremock:wiremock bruker Jetty 11
        exclude(group = "org.wiremock", module = "wiremock")
    }
    testImplementation(libs.wiremock)
}

tasks {
    named<Test>("test") {
        val parallellDisabled = System.getenv("CI") == "true"
        systemProperty("junit.jupiter.execution.parallel.enabled", parallellDisabled.not().toString())
        systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
        systemProperty("junit.jupiter.execution.parallel.config.strategy", "fixed")
        systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", "8")
    }
}
