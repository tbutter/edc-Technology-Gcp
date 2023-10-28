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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.cloud.bigquery.Field;
// import com.google.cloud.bigquery.FieldValue;
// import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Schema class for ObjectMapper serialization / deserialization.
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BigQuerySchema {
    public HashMap<String, String> fields = new HashMap<>();

    public BigQuerySchema() {
    }

    public BigQuerySchema(HashMap<String, String> fields) {
        setFields(fields);
    }

    public BigQuerySchema(Schema schema) {
        for (Field field : schema.getFields()) {
            fields.put(field.getName(), field.getType().toString());
        }
    }

    HashMap<String, String> getFields() {
        return fields;
    }

    void setFields(HashMap<String, String> fields) {
        this.fields = fields;
    }

    Schema getSchema() {
        List<Field> sqlFields = new ArrayList<>();
        for (Map.Entry<String, String> item : fields.entrySet()) {
            Field field = Field.of(item.getKey(), LegacySQLTypeName.valueOf(item.getValue()));
            sqlFields.add(field);
        }
        Schema schema = Schema.of(sqlFields);

        return schema;
    }
}
