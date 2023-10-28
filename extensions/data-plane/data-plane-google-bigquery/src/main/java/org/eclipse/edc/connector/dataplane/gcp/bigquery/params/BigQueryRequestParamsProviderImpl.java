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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.params;

import org.eclipse.edc.connector.dataplane.bigquery.spi.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.bigquery.spi.BigQueryRequestParamsProvider;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.BigQueryDataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;

public class BigQueryRequestParamsProviderImpl implements BigQueryRequestParamsProvider {


    public BigQueryRequestParamsProviderImpl(Vault vault, TypeManager typeManager) {
    }

    @Override
    public BigQueryRequestParams provideSourceParams(DataFlowRequest request) {
        var params = BigQueryRequestParams.Builder.newInstance();
        params.request(request);
        params.project(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.PROJECT));
        params.dataset(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.DATASET));
        params.table(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.TABLE));
        params.query(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.QUERY));
        params.serviceAccountFile(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.SERVICE_ACCOUNT_FILE));
        params.serviceAccountName(request.getSourceDataAddress().getStringProperty(BigQueryDataAddress.SERVICE_ACCOUNT_NAME));
        var address = BigQueryDataAddress.Builder.newInstance().copyFrom(request.getSourceDataAddress()).build();
        // sourceDecorators.forEach(decorator -> decorator.decorate(request, address, params));
        return params.build();
    }

    @Override
    public BigQueryRequestParams provideSinkParams(DataFlowRequest request) {
        var params = BigQueryRequestParams.Builder.newInstance();
        params.request(request);
        params.project(request.getDestinationDataAddress().getStringProperty(BigQueryDataAddress.PROJECT));
        params.dataset(request.getDestinationDataAddress().getStringProperty(BigQueryDataAddress.DATASET));
        params.table(request.getDestinationDataAddress().getStringProperty(BigQueryDataAddress.TABLE));
        params.serviceAccountFile(request.getDestinationDataAddress().getStringProperty(BigQueryDataAddress.SERVICE_ACCOUNT_FILE));
        params.serviceAccountName(request.getDestinationDataAddress().getStringProperty(BigQueryDataAddress.SERVICE_ACCOUNT_NAME));
        var address = BigQueryDataAddress.Builder.newInstance().copyFrom(request.getDestinationDataAddress()).build();
        // sinkDecorators.forEach(decorator -> decorator.decorate(request, address, params));
        return params.build();
    }
}
