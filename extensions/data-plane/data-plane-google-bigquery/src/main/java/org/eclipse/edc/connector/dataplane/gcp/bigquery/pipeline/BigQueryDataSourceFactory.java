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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline;

import org.eclipse.edc.connector.dataplane.bigquery.spi.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.BigQueryDataAddress;
import org.eclipse.edc.spi.types.domain.HttpDataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import static org.eclipse.edc.spi.types.domain.BigQueryDataAddress.BIGQUERY_DATA;

/**
 * Instantiates {@link BigQueryDataSource}s for requests whose source data type is {@link HttpDataAddress#HTTP_DATA}.
 */
public class BigQueryDataSourceFactory implements DataSourceFactory {

    private final BigQueryRequestParamsProvider requestParamsProvider;
    private final Monitor monitor;

    public BigQueryDataSourceFactory(Monitor monitor,
                                     BigQueryRequestParamsProvider requestParamsProvider) {
        this.monitor = monitor;
        this.requestParamsProvider = requestParamsProvider;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return BIGQUERY_DATA.equals(request.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        try {
            createSource(request);
        } catch (Exception e) {
            return Result.failure("Failed to build HttpDataSource: " + e.getMessage());
        }
        return Result.success();
    }

    /*
    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        try {
            createSource(request);
        } catch (Exception e) {
            return Result.failure("Failed to build HttpDataSource: " + e.getMessage());
        }
        return Result.success(true);
    }

     */

    @Override
    public DataSource createSource(DataFlowRequest request) {

        var dataAddress = BigQueryDataAddress.Builder.newInstance()
                .copyFrom(request.getSourceDataAddress())
                .build();
        return BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(request.getId())
                .name(dataAddress.getName())
                .params(requestParamsProvider.provideSourceParams(request))
                .build();
    }
}
