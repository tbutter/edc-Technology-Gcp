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
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static org.eclipse.edc.spi.types.domain.BigQueryDataAddress.BIGQUERY_DATA;

/**
 * Instantiates {@link BigQueryDataSink}s
 */
public class BigQueryDataSinkFactory implements DataSinkFactory {
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final BigQueryRequestParamsProvider requestParamsProvider;

    public BigQueryDataSinkFactory(
                               ExecutorService executorService,
                               Monitor monitor,
                               BigQueryRequestParamsProvider requestParamsProvider) {
        this.executorService = executorService;
        this.monitor = monitor;
        this.requestParamsProvider = requestParamsProvider;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return BIGQUERY_DATA.equals(request.getDestinationDataAddress().getType());
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        try {
            createSink(request);
        } catch (Exception e) {
            return Result.failure("Failed to build HttpDataSink: " + e.getMessage());
        }
        return Result.success();
    }

    /*
    @Override
    public @NotNull Result<Boolean> validate(DataFlowRequest request) {
        try {
            createSink(request);
        } catch (Exception e) {
            return Result.failure("Failed to build HttpDataSink: " + e.getMessage());
        }
        return Result.success(true);
    }
    */

    @Override
    public DataSink createSink(DataFlowRequest request) {
        return BigQueryDataSink.Builder.newInstance()
                .requestId(request.getId())
                .executorService(executorService)
                .monitor(monitor)
                .params(requestParamsProvider.provideSinkParams(request))
                .build();
    }
}
