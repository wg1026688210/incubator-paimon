/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.format.autohome;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * The {@code AutohomeJsonRecordParser} class extends the abstract {@link RecordParser} and is
 * designed to parse records from Autohome's JSON change data capture (CDC) format. Debezium is a
 * CDC solution for MySQL databases that captures row-level changes to database tables and outputs
 * them in JSON format. This parser extracts relevant information from the Autohome-JSON format and
 * converts it into a list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, and DELETE, and creates
 * corresponding {@link RichCdcMultiplexRecord} objects to represent these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields,
 * including the 'before' and 'after' states for UPDATE operations, and the class also supports
 * schema extraction for the Kafka topic. Autohome's specific fields such as 'source', 'op' for
 * operation type, and primary key field names are used to construct the details of each record
 * event.
 */
public class AutohomeRecordParser extends RecordParser {
    // private static final String FIELD_SCHEMA = "schema";
    // private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_PRIMARY = "uniqueKey";
    private static final String FIELD_DB = "databaseName";
    private static final String FIELD_TABLE = "tableName";
    private static final String FIELD_OPT = "opt";
    private static final String OP_INSERT = "i";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";

    private static final String OP_HEART = "h";

    public AutohomeRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        JsonNode node = root.get(FIELD_OPT);
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        if (isNull(node)) {
            return records;
        }
        String operation = node.asText();
        switch (operation) {
            case OP_INSERT:
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(
                        mergeOldRecord(getData(), getBefore(operation)), RowKind.DELETE, records);
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                break;
            case OP_HEART:
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    private JsonNode getData() {
        return getAndCheck(dataField());
    }

    private JsonNode getBefore(String op) {
        return getAndCheck(FIELD_BEFORE, FIELD_OPT, op);
    }

    //    //JSON字符串转换（反序列化）处理，debezium格式中，before和after等字段在payload字段中
    //    @Override
    //    protected void setRoot(String record) {
    //        JsonNode node = JsonSerdeUtil.fromJson(record, JsonNode.class);
    //        if (node.has(FIELD_SCHEMA)) {
    //            root = node.get(FIELD_PAYLOAD);
    //        } else {
    //            root = node;
    //        }
    //    }

    @Override
    protected String primaryField() {
        return FIELD_PRIMARY;
    }

    @Override
    protected String dataField() {
        return FIELD_AFTER;
    }

    @Nullable
    @Override
    protected String getTableName() {
        JsonNode node = root.get(FIELD_TABLE);
        return isNull(node) ? null : node.asText();
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        JsonNode node = root.get(FIELD_DB);
        return isNull(node) ? null : node.asText();
    }

    @Override
    protected String format() {
        return "autohome-json";
    }

    @Nullable
    private String getFromSourceField(String key) {
        JsonNode node = root.get(key);
        if (isNull(node)) {
            return null;
        }

        node = node.get(key);
        return isNull(node) ? null : node.asText();
    }
}
