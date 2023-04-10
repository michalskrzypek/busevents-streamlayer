import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.21"
}

group = "pl.michalskrzypek"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.code.gson:gson:2.9.0")

    testImplementation("io.projectreactor:reactor-test:3.4.24")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("io.dropwizard.metrics:metrics-core:4.1.12.1")
    implementation("org.apache.storm:storm-core:2.4.0") {
        exclude("io.dropwizard.metrics", "metrics-core")
    }
    implementation("org.springframework.kafka:spring-kafka:2.7.7")
    implementation("org.apache.storm:storm-cassandra:2.4.0")
    implementation("com.datastax.cassandra:cassandra-driver-core:3.11.0")
    implementation("com.codahale.metrics:metrics-core:3.0.2")
    implementation("org.apache.kafka:kafka_2.13:3.3.1")
    implementation("org.apache.kafka:kafka-clients:3.3.1")
    implementation("org.apache.storm:storm-kafka-client:2.4.0")
    implementation("com.twitter:carbonite:1.5.0") {
        repositories {
            maven {
                setUrl("https://clojars.org/repo/")
            }
        }
    }


}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
