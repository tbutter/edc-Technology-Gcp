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

// import com.fasterxml.jackson.core.JsonGenerator;
// import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.databind.SerializerProvider;
// import com.fasterxml.jackson.databind.module.SimpleModule;
// import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auth.oauth2.ServiceAccountCredentials;
// import com.google.cloud.bigquery.*;
// import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.ImmutableMap;
import org.eclipse.edc.connector.dataplane.bigquery.spi.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
// import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
// import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
// import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.Vector;
import java.util.stream.Stream;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.error;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

public class BigQueryDataSource implements DataSource {

    private static final int FORBIDDEN = 401;
    private static final int NOT_AUTHORIZED = 403;
    private static final int NOT_FOUND = 404;

    private String name;
    private BigQueryRequestParams params;
    private String requestId;
    private Monitor monitor;

    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        monitor.info("BigQueryDataSource openPartStream: project=" + params.getProject());
        try {
            monitor.info("BigQueryDataSource openPartStream: service_account_file=" + params.getServiceAccountFile());
            monitor.info("BigQueryDataSource openPartStream: service_account_name=" + params.getServiceAccountName());
            var bqBuilder = BigQueryOptions.newBuilder().setProjectId(params.getProject());
            if (params.getServiceAccountName() != null && !params.getServiceAccountName().isEmpty()) {
                GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();
                sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/iam"));
                ImpersonatedCredentials targetCredentials = ImpersonatedCredentials.create(sourceCredentials, params.getServiceAccountName(), null,
                    Arrays.asList("https://www.googleapis.com/auth/bigquery"), 300);
                bqBuilder.setCredentials(targetCredentials);
            } else if (params.getServiceAccountFile() != null && !params.getServiceAccountFile().isEmpty()) {                var accountKeyStream = new FileInputStream(params.getServiceAccountFile());
                bqBuilder.setCredentials(ServiceAccountCredentials.fromStream(accountKeyStream));
                accountKeyStream.close();
            }
            BigQuery bigquery = bqBuilder.build().getService();
            monitor.info("BigQueryDataSource openPartStream: bigquery=" + bigquery);
            var query = buildQuery();

            QueryJobConfiguration queryConfig
                    = QueryJobConfiguration.newBuilder(query)
                            // Use standard SQL syntax for queries.
                            // See: https://cloud.google.com/bigquery/sql-reference/
                            .setLabels(ImmutableMap.of("customer", "customer_name"))
                            .setUseLegacySql(false)
                            .build();

            // Create a job ID so that we can safely retry.
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            // Wait for the query to complete.
            queryJob = queryJob.waitFor();

            // Check for errors
            if (queryJob == null) {
                throw new RuntimeException("Job no longer exists");
            } else if (queryJob.getStatus().getError() != null) {
                // You can also look at queryJob.getStatus().getExecutionErrors() for all
                // errors, not just the latest one.
                throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            monitor.info("Query executed: rows = " + queryJob.getQueryResults().getTotalRows());

            ObjectMapper objectMapper = new ObjectMapper();
            List<Part> parts = new ArrayList();

            var schema = queryJob.getQueryResults().getSchema();
            var bqSchema = new BigQuerySchema(schema);
            parts.add(new BigQueryPart("schema", new ByteArrayInputStream(objectMapper.writeValueAsString(bqSchema).getBytes())));

            Vector<String> fieldNames = new Vector<String>();
            for (var schemaItem : schema.getFields()) {
                fieldNames.add(schemaItem.getName());
            }

            int rowIndex = 0;
            for (FieldValueList row : queryJob.getQueryResults().iterateAll()) {
                int index = 0;
                HashMap<String, String> rowMap = new HashMap<String, String>();
                rowMap.put("__row__", "" + rowIndex);
                for (FieldValue value : row) {
                    rowMap.put(fieldNames.elementAt(index++), value.getStringValue());
                }
                parts.add(new BigQueryPart("row " + rowIndex, new ByteArrayInputStream(objectMapper.writeValueAsString(rowMap).getBytes())));
                rowIndex++;
            }

            return success(parts.stream());

        } catch (InterruptedException interruptedException) {
            monitor.warning("Job waitFor raised exception");
            return error(interruptedException.getMessage());
        } catch (IOException ioException) {
            monitor.warning("IoException " + ioException);
            return error(ioException.getMessage());
        }
    }

    private BigQueryDataSource() {
    }

    private String getParameterName(String query, int index) {
        int start = index;
        while (index < query.length() && (query.charAt(index) == '_' || Character.isLetterOrDigit(query.charAt(index)))) {
            ++index;
        }
        return query.substring(start, index);
    }

    private String buildQuery() {
        var query = params.getQuery();
        int index = -1;
        while ((index = query.indexOf("@@")) != -1) {
            String parameterName = getParameterName(query, index + 2);
            monitor.info("Source parameter name: " + parameterName);

            var value = params.getRequest().getDestinationDataAddress().getStringProperty(parameterName);
            if (value != null) {
                query = query.substring(0, index) + value + query.substring(index + 2 + parameterName.length());
            } else {
                // throw error.
            }
        }

        return query;
    }

    public static class Builder {

        private final BigQueryDataSource dataSource;

        public static Builder newInstance() {
            return new Builder();
        }

        private Builder() {
            dataSource = new BigQueryDataSource();
        }

        public Builder name(String name) {
            dataSource.name = name;
            return this;
        }

        public Builder requestId(String requestId) {
            dataSource.requestId = requestId;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            dataSource.monitor = monitor;
            return this;
        }

        public Builder params(BigQueryRequestParams params) {
            dataSource.params = params;
            return this;
        }

        public BigQueryDataSource build() {
            Objects.requireNonNull(dataSource.requestId, "requestId");
            Objects.requireNonNull(dataSource.monitor, "monitor");
            return dataSource;
        }
    }

    // TODO: implement
    private static class BigQueryPart implements Part {

        private final String name;
        private final InputStream content;

        BigQueryPart(String name, InputStream content) {
            this.name = name;
            this.content = content;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long size() {
            return SIZE_UNKNOWN;
        }

        @Override
        public InputStream openStream() {
            return content;
        }

    }
}
