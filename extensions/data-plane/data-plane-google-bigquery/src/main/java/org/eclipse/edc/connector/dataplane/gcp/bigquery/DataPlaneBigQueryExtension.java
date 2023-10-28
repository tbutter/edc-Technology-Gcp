/*
 *  Copyright (c) 2023 Google LLC
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.gcp.bigquery;

import org.eclipse.edc.connector.dataplane.bigquery.params.BigQueryRequestParamsProviderImpl;
import org.eclipse.edc.connector.dataplane.bigquery.pipeline.BigQueryDataSinkFactory;
import org.eclipse.edc.connector.dataplane.bigquery.pipeline.BigQueryDataSourceFactory;
import org.eclipse.edc.connector.dataplane.bigquery.spi.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
// import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

/**
 * Provides support for reading data from an BigQuery endpoint and sending data to an BigQuery endpoint.
 */
@Provides(BigQueryRequestParamsProvider.class)
@Extension(value = DataPlaneBigQueryExtension.NAME)
public class DataPlaneBigQueryExtension implements ServiceExtension {
    public static final String NAME = "Data Plane BigQuery";

    @Inject
    private PipelineService pipelineService;

    @Inject
    private DataTransferExecutorServiceContainer executorContainer;

    @Inject
    private Vault vault;

    @Inject
    private TypeManager typeManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        monitor.info("BigQuery Extension initialize");

        var paramsProvider = new BigQueryRequestParamsProviderImpl(vault, typeManager);
        context.registerService(BigQueryRequestParamsProvider.class, paramsProvider);

        var sourceFactory = new BigQueryDataSourceFactory(monitor, paramsProvider);
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new BigQueryDataSinkFactory(executorContainer.getExecutorService(), monitor, paramsProvider);
        pipelineService.registerFactory(sinkFactory);
    }
}
