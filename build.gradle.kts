val junitJupiterVersion = "5.10.0"
val ktorVersion = "2.3.4"
val rapidsAndRiversVersion = "2023093008351696055717.ffdec6aede3d"
val wireMockVersion = "2.33.2"

plugins {
    kotlin("jvm") version "1.9.10"
}

group = "no.nav.helse"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("io.ktor:ktor-server-auth-jwt:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")
    implementation("io.ktor:ktor-server-call-id:$ktorVersion")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.5.21")
    implementation("commons-codec:commons-codec:1.15")

    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("no.nav:kafka-embedded-env:3.2.1") {
        constraints {
            implementation("io.netty:netty-common") {
                version { require("4.1.80.Final") }
                because("no.nav:kafka-embedded-env:3.2.1 drar inn sårbar versjon 4.1.77.Final")
            }
        }
        exclude(group = "log4j", module = "log4j")
    }
    testImplementation("org.awaitility:awaitility:4.0.3")

    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-client-mock-jvm:$ktorVersion")

    testImplementation("com.github.tomakehurst:wiremock-jre8:$wireMockVersion") {
        constraints {
            implementation("org.eclipse.jetty:jetty-bom") {
                version { require("9.4.48.v20220622") }
                because("wiremock v2.33.2 drar inn sårbar versjon 9.4.46.v20220331 av jetty-proxy")
            }
        }
    }
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "17"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "17"
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
