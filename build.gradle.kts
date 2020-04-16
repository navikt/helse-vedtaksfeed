import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val junitJupiterVersion = "5.5.2"
val ktorVersion = "1.3.1"

val kafkaVersion = "2.3.1"
val rapidsAndRiversVersion = "1.35001d7"

val wireMockVersion = "2.19.0"

plugins {
    kotlin("jvm") version "1.3.61"
}

group = "no.nav.helse"

val githubUser: String by project
val githubPassword: String by project

repositories {
    mavenCentral()
    maven("http://packages.confluent.io/maven/")
    maven("https://dl.bintray.com/kotlin/kotlinx/")
    maven {
        url = uri("https://maven.pkg.github.com/navikt/rapids-and-rivers")
        credentials {
            username = githubUser
            password = githubPassword
        }
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    implementation("io.ktor:ktor-auth-jwt:$ktorVersion")
    implementation("io.ktor:ktor-jackson:$ktorVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.10")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.9.10")

    implementation("org.slf4j:slf4j-api:1.7.29")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("net.logstash.logback:logstash-logback-encoder:6.2")

    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("no.nav:kafka-embedded-env:2.3.0")
    testImplementation("org.awaitility:awaitility:4.0.1")

    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-client-mock-jvm:$ktorVersion")
    testImplementation ("com.nimbusds:nimbus-jose-jwt:7.5.1")
    testImplementation("com.github.tomakehurst:wiremock:$wireMockVersion") {
        exclude(group = "junit")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_12
    targetCompatibility = JavaVersion.VERSION_12
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("app")

    manifest {
        attributes["Main-Class"] = "no.nav.helse.AppKt"
        attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
            it.name
        }
    }

    doLast {
        configurations.runtimeClasspath.get().forEach {
            val file = File("$buildDir/libs/${it.name}")
            if (!file.exists())
                it.copyTo(file)
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
