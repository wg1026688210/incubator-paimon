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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;

/** IT cases for {@link KafkaAutohomeSyncTableActionITCase}. */
public class KafkaAutohomeSyncTableActionITCase extends KafkaActionITCaseBase {

    @Test
    @Timeout(6000)
    public void testDeleteRecordWithNestedDataType() throws Exception {
        baseTest(
                "AH_delete",
                "kafka/debezium/table/nestedtype/autohome-insert-delete-1.txt",
                Collections.singletonList("+I[102, hammer, {\"row_key\":\"value\"}]"));
    }

    @Test
    @Timeout(6000)
    public void testUpdateRecordWithNestedDataType() throws Exception {
        baseTest(
                "AH_update",
                "kafka/debezium/table/nestedtype/autohome-insert-update-1.txt",
                Lists.newArrayList(
                        "+I[101, hammer, {\"row_key\":\"value\"}]",
                        "+I[102, tom, {\"row_key\":\"value1\"}]"));
    }

    private void baseTest(String topic, String file, List<String> expected) throws Exception {
        createTestTopic(topic, 1, 1);

        try {
            writeRecordsToKafka(topic, file);
        } catch (Exception e) {
            throw new Exception("Failed to write autohome-DTS data to Kafka.", e);
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "autohome-json");
        kafkaConfig.put(TOPIC.key(), topic);
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPrimaryKeys("id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING().notNull(), DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"id", "name", "row"});
        List<String> primaryKeys = Collections.singletonList("id");
        waitForResult(expected, table, rowType, primaryKeys);
    }
}
