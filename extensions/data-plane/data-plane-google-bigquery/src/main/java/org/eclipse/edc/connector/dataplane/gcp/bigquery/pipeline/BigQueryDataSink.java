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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes data in a streaming fashion to an HTTP endpoint.
 */
public class BigQueryDataSink extends ParallelSink {
    private static final StreamResult<Void> ERROR_WRITING_DATA = StreamResult.error("Error writing data");
    private BigQueryRequestParams params;

    private enum TransferType {
        STREAMING,
        DML
    }
    private Schema schema = null;
    private final List<String> storedParts = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BigQuery bigquery = null;
    private TableId tableId = null;

    synchronized void initBigQuery() {
        if (bigquery != null && tableId != null) {
            // monitor.info("BQ Already Init (" + this +")");
            return;
        }

        try {
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
            bigquery = bqBuilder.build().getService();
            tableId = TableId.of(params.getDataset(), params.getTable());
            monitor.info("BQ Init Done");
        } catch (IOException ioException) {
            monitor.severe((ioException.getMessage()));
        }
    }

    // TODO merge multiple rows and create a single query for them to speed up DML.
    void addRow(String text, TransferType transferType) throws JsonProcessingException, InterruptedException {
        HashMap<String, String> rowMap = new HashMap<String, String>();
        rowMap = objectMapper.readValue(text, HashMap.class);
        rowMap.remove("__row__");

        if (transferType == TransferType.STREAMING) {
            // Streaming request not available for the free tier.
            InsertAllResponse response =
                    bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowMap).build());

            if (response.hasErrors()) {
                monitor.severe("Error while inserting");
            } else {
                monitor.info("Streaming Insert OK");
            }
        } else if (transferType == TransferType.DML) {
            // DML  not available for the free tier.
            String query = String.format("INSERT INTO %s.%s.%s(", params.getProject(), params.getDataset(), params.getTable());
            int index = 0;
            for (Map.Entry<String, String> entry : rowMap.entrySet()) {
                if (index > 0) {
                    query += ",";
                }
                query += entry.getKey();
                ++index;
            }
            query += ") VALUES(";
            index = 0;
            for (Map.Entry<String, String> entry : rowMap.entrySet()) {
                if (index > 0) {
                    query += ",";
                }

                Field field = schema.getFields().get(index);
                if (field.getType() == LegacySQLTypeName.STRING) {
                    query += "\"" + entry.getValue() + "\"";
                } else if (field.getType() == LegacySQLTypeName.INTEGER) {
                    query += entry.getValue();
                } else if (field.getType() == LegacySQLTypeName.TIMESTAMP) {
                    Float timestampValue = Float.parseFloat(entry.getValue());
                    query += "TIMESTAMP_SECONDS(" + timestampValue.intValue() + ")";
                }
                ++index;
            }
            query += ")";
            // monitor.info("INSERT query: " + query);
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(query)
                            // Use standard SQL syntax for queries.
                            // See: https://cloud.google.com/bigquery/sql-reference/
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

            monitor.info("DML Insert OK");
        } else {
            // No transfer.
        }
    }

    @Override
    protected StreamResult<Void> transferParts(List<DataSource.Part> parts) {
        // monitor.info("BigQueryDataSink transferParts (" + params.getRequest().getId() + "): project=" + params.getProject() + ", " + parts.size() + " parts");
        initBigQuery();

        // Check if the table exists.
        try {
            Table table = bigquery.getTable(tableId);
            if (table != null && table.exists()) {
                monitor.warning("Table " + params.getTable() + " exists");
            } else {
                monitor.warning("Table " + params.getTable() + " DON'T exist");
            }
        } catch (BigQueryException bigQueryException) {
            monitor.warning("Table " + params.getTable() + " DON'T exist: " + bigQueryException.getMessage());
        }

        TransferType transferType = TransferType.STREAMING;
        for (DataSource.Part part : parts) {
            try (var inputStream = part.openStream()) {
                String text = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                // monitor.info("BigQueryDataSink transferParts: part '" + part.name() + "' content is " + text);
                if (part.name() == "schema") {
                    // monitor.info("BigQueryDataSink transferParts: schema is " + text);
                    if (schema != null) {
                        monitor.severe("ERROR Schema already set");
                    } else {
                        BigQuerySchema bqSchema = objectMapper.readValue(text, BigQuerySchema.class);
                        schema = bqSchema.getSchema();
                        for (var storedPart : storedParts) {
                            addRow(storedPart, transferType);
                        }
                        storedParts.clear();
                        monitor.info("Schema SET");
                    }
                } else {
                    if (schema == null) {
                        storedParts.add(text);
                    } else {
                        addRow(text, transferType);
                    }
                }
            } catch (IOException e) {
                monitor.severe("Cannot open the input part", e);
                monitor.severe(e.toString());
                return StreamResult.error("An error");
            } catch (Exception e) {
                monitor.severe("Error writing data to the bucket", e);
                return StreamResult.error("An error");
            }
        }
        // monitor.info("BigQueryDataSource transferParts: bigquery=" + bigquery);
        return StreamResult.success();

    }

    private BigQueryDataSink() {
    }

    public static class Builder extends ParallelSink.Builder<Builder, BigQueryDataSink> {

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder params(BigQueryRequestParams params) {
            sink.params = params;
            return this;
        }

        private Builder() {
            super(new BigQueryDataSink());
        }

        @Override
        protected void validate() {

        }
    }
}
