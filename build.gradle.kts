plugins {
    id("java")
    application
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
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
    // mainClass.set("app.example.IndexJoinDemo")
    // mainClass.set("app.example.OrderLimitDemo")
    // mainClass.set("app.example.GroupByDemo") 
    // mainClass.set("app.example.DistinctHavingDemo")
    // Interactive SQL CLI
    mainClass.set("app.cli.SimpleIJ")
    applicationName = "minidb"
}

//tasks.test { useJUnitPlatform() }
tasks.test {
  useJUnitPlatform()
  testLogging {
    events("passed", "failed", "skipped")
    showStandardStreams = true
  }
}


tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}
