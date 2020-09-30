import com.adarshr.gradle.testlogger.theme.ThemeType

plugins {
    java
    maven
    `maven-publish`
    jacoco
    signing
    idea

    id("com.palantir.git-version") version "0.11.0"
    id("com.adarshr.test-logger") version "1.6.0"

    id("org.springframework.boot") version "2.3.3.RELEASE"
    id("io.spring.dependency-management") version "1.0.10.RELEASE"

    kotlin("jvm")
    kotlin("plugin.spring") version Versions.kotlinVersion
}

apply {
    plugin("kotlin")
}

val gitVersion: groovy.lang.Closure<Any> by extra
version = gitVersion().toString().replaceFirst("([0-9]+\\.[0-9]+\\.[0-9](\\..*)?)".toRegex(), "$1");

configure<JavaPluginConvention> {
    group = "im.toss.cache"
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    implementation(project(":cache-core"))

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Versions.kotlinVersion}")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${Versions.kotlinCoroutinesVersion}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:${Versions.kotlinCoroutinesVersion}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:${Versions.kotlinCoroutinesVersion}")

    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation("org.springframework.boot:spring-boot-actuator-autoconfigure")
    implementation("io.micrometer:micrometer-core")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation("io.github.microutils:kotlin-logging:1.5.9")

    testImplementation("org.springframework.boot:spring-boot-starter-test") { exclude(group = "org.junit.vintage", module = "junit-vintage-engine") }
    testImplementation("com.github.toss:assert-extensions:0.2.0")

}

jacoco {
    toolVersion = "0.8.5"
}

tasks {
    jacocoTestReport {
        executionData.setFrom(
            fileTree("build/jacoco") {
                include("**/*.exec")
            }
        )

        reports {
            xml.isEnabled = true
            html.isEnabled = true
        }
    }

    jacocoTestCoverageVerification {
        dependsOn(setOf(jacocoTestReport))
        violationRules {
            rule {
                limit {
                    counter = "INSTRUCTION"
                    minimum = "1.000000000".toBigDecimal()
                }

                limit {
                    counter = "BRANCH"
                    minimum = "1.000000000".toBigDecimal()
                }
            }
        }
    }
}

testlogger {
    theme = ThemeType.STANDARD_PARALLEL
    showExceptions = true
    slowThreshold = 2000
    showSummary = true
    showPassed = true
    showSkipped = true
    showFailed = true
    showStandardStreams = false
    showPassedStandardStreams = false
    showSkippedStandardStreams = false
    showFailedStandardStreams = false
}

tasks.register<Jar>("sourcesJar") {
    from(sourceSets.main.get().allJava)
    archiveClassifier.set("sources")
}

tasks.register<Jar>("javadocJar") {
    from(tasks.javadoc)
    archiveClassifier.set("javadoc")
}

tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}

tasks.getByName("jar") {
    enabled = true
}

tasks.getByName("bootJar") {
    enabled = false
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "im.toss.cache"
            artifactId = "cache-spring"
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])
            pom {
                name.set("cache-core")
                description.set("Cache library")
                url.set("https://github.com/toss/cache")
                scm {
                    url.set("git@github.com:toss/cache.git")
                    connection.set("scm:git:git@github.com:toss/cache.git")
                    developerConnection.set("scm:git:git@github.com:toss/cache.git")
                }
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        name.set("Jinsung Oh")
                        email.set("econquer@toss.im")
                        organizationUrl.set("https://toss.im/")
                    }
                }
            }
        }
    }
}

signing {
    sign(publishing.publications["maven"])
}
