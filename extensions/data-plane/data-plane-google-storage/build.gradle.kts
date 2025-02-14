/*
 *  Copyright (c) 2022 T-Systems International GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       T-Systems International GmbH
 *
 */
 
plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.dataplane)
    implementation(libs.edc.util)
    implementation(project(":extensions:common:gcp:gcp-core"))
    implementation(libs.edc.core.dataplane.util)

    implementation(libs.googlecloud.storage)

    testImplementation(libs.edc.core.dataplane)
    testImplementation(libs.edc.junit)
}
