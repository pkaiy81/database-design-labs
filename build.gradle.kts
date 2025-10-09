plugins {
    id("java")
    application
}

group = "app"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    // デモ実行用（組み込みDB）：外部サーバ不要で即動く
    implementation("com.h2database:h2:2.2.224")

    // 書籍準拠の Derby（サーバ/クライアント型）：必要になったら有効化
    implementation("org.apache.derby:derbyclient:10.17.1.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

application {
    mainClass.set("app.example.ExampleMain")
}

tasks.test {
    useJUnitPlatform()
}
