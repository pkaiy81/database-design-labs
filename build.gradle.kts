plugins {
    id("java")
    application
}

group = "app"
version = "0.1.0"

repositories { mavenCentral() }

dependencies {
    implementation("com.h2database:h2:2.2.224")
    implementation("org.apache.derby:derbyclient:10.17.1.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

application {
    // mainClass.set("app.example.ExampleMain")
    // mainClass.set("app.example.MemoryDemo")
    // mainClass.set("app.example.TxDemo")
    // mainClass.set("app.example.RecordDemo")
    // mainClass.set("app.example.MetadataDemo")
    // mainClass.set("app.example.IndexDemo")
    // mainClass.set("app.example.QueryDemo")
    // mainClass.set("app.example.SqlDemo")
    // mainClass.set("app.example.OptimizerDemo")
    mainClass.set("app.example.IndexJoinDemo")
}

tasks.test { useJUnitPlatform() }

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}
