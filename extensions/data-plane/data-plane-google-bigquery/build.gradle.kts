import org.gradle.internal.classpath.Instrumented
import org.gradle.internal.classpath.Instrumented.systemProperties
import org.gradle.internal.classpath.Instrumented.systemProperty

/*
 *  License here.
 */

plugins {
    `java-library`
    // `java-test-fixtures`
}

dependencies {
    implementation(project(":spi:common:bigquery-spi"))
    implementation(project(":spi:data-plane:data-plane-bigquery-spi"))
    // api(project(":spi:data-plane:data-plane-spi"))
    implementation(libs.edc.spi.dataplane)
    implementation(libs.googlecloud.bigquery)
    // api(project(":spi:data-plane:data-plane-http-spi"))
    // api(project(":spi:common:http-spi"))
    // implementation(project(":core:common:util"))
    // implementation(project(":core:data-plane:data-plane-util"))

    // testImplementation(project(":core:common:junit"))
    // testImplementation(project(":core:data-plane:data-plane-core"))
    // testImplementation(libs.restAssured)
    // testImplementation(libs.mockserver.netty)

    // testImplementation(testFixtures(project(":spi:data-plane:data-plane-spi")))
}
