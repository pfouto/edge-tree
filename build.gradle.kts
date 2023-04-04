plugins {
    kotlin("jvm") version "1.8.10"
    kotlin("plugin.serialization") version "1.8.10"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "me.pfouto"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://jitpack.io")
    }
}
dependencies {
    testImplementation(kotlin("test"))

    implementation("com.charleskorn.kaml:kaml:0.53.0")

    implementation("com.github.pfouto:babel-core:0.4.47")
    implementation("org.apache.cassandra:cassandra-all:4.1.0"){
        exclude(group = "org.slf4j", module = "log4j-over-slf4j")
        exclude(group = "org.slf4j", module = "jcl-over-slf4j")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.logback", module = "logback-classic")
        exclude(group = "ch.qos.logback", module = "logback-core")
    }
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("io.netty:netty-tcnative-boringssl-static:2.0.36.Final")
    //implementation("org.slf4j:slf4j-log4j12:2.0.6")
}

tasks.test {
    useJUnitPlatform()
}


application {
    mainClass.set("MainKt")
}

kotlin { // Extension for easy setup
    jvmToolchain(8) // Target version of generated JVM bytecode. See 7️⃣
}

tasks {
    shadowJar {
        // defaults to project.name
        archiveBaseName.set("../../deploy/${project.name}-fat")
        // defaults to all, so removing this overrides the normal, non-fat jar
        archiveClassifier.set("")
    }
}