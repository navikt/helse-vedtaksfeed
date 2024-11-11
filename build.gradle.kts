val junitJupiterVersion = "5.10.2"
val ktorVersion = "3.0.1"
val rapidsAndRiversVersion = "2024111021211731270108.8f718ca4927d"
val tbdLibsVersion = "2024.11.10-21.15-45a429b5"
val wireMockVersion = "3.0.3"
val mockkVersion = "1.13.13"
plugins {
    kotlin("jvm") version "2.0.20"
}

repositories {
    val githubPassword: String? by project
    mavenCentral()
    /* ihht. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
        så plasseres github-maven-repo (med autentisering) før nav-mirror slik at github actions kan anvende førstnevnte.
        Det er fordi nav-mirroret kjører i Google Cloud og da ville man ellers fått unødvendige utgifter til datatrafikk mellom Google Cloud og GitHub
     */
    maven {
        url = uri("https://maven.pkg.github.com/navikt/maven-release")
        credentials {
            username = "x-access-token"
            password = githubPassword
        }
    }
    maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("io.ktor:ktor-server-auth-jwt:$ktorVersion")
    implementation("commons-codec:commons-codec:1.15")

    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")
    implementation("com.github.navikt.tbd-libs:azure-token-client-default:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:retry:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:speed-client:$tbdLibsVersion")

    testImplementation("com.github.navikt.tbd-libs:rapids-and-rivers-test:$tbdLibsVersion")
    testImplementation("com.github.navikt.tbd-libs:naisful-test-app:$tbdLibsVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation("no.nav:kafka-embedded-env:3.2.5") {
        constraints {
            implementation("org.apache.commons:commons-compress") {
                version { require("1.24.0") }
                because("no.nav:kafka-embedded-env:3.2.5 drar inn sårbar versjon 1.22")
            }
        }
    }
    testImplementation("org.awaitility:awaitility:4.0.3")

    testImplementation("org.wiremock:wiremock:$wireMockVersion") {
        constraints {
            implementation("com.jayway.jsonpath:json-path") {
                version { require("2.9.0") }
                because("wiremock 3.0.3 drar inn sårbar versjon 2.8.0 av json-path")
            }
        }
    }
}

tasks {
    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    withType<Jar> {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.AppKt"
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists()) it.copyTo(file)
            }
        }
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}
