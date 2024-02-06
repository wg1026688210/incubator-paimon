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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.sink.CompactionTaskTypeInfo;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import java.util.regex.Pattern;

/**
 * It is responsible for the batch compactor source of the table of unaware bucket in combined mode.
 */
public class UnawareTablesBatchCompactorSourceFunction extends UnawareTablesSourceFunction {
    public UnawareTablesBatchCompactorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        super(
                catalogLoader,
                includingPattern,
                excludingPattern,
                databasePattern,
                false,
                monitorInterval);
    }

    @Override
    public void run(SourceContext<AppendOnlyCompactionTask> sourceContext) throws Exception {
        this.batchMonitor(sourceContext);
    }

    public static DataStream<AppendOnlyCompactionTask> buildSource(
            StreamExecutionEnvironment env,
            String name,
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        UnawareTablesBatchCompactorSourceFunction function =
                new UnawareTablesBatchCompactorSourceFunction(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        StreamSource<AppendOnlyCompactionTask, UnawareTablesBatchCompactorSourceFunction>
                sourceOperator = new StreamSource<>(function);
        CompactionTaskTypeInfo compactionTaskTypeInfo = new CompactionTaskTypeInfo();
        SingleOutputStreamOperator<AppendOnlyCompactionTask> source =
                new DataStreamSource<>(
                                env,
                                compactionTaskTypeInfo,
                                sourceOperator,
                                false,
                                name,
                                Boundedness.BOUNDED)
                        .forceNonParallel();

        PartitionTransformation<AppendOnlyCompactionTask> transformation =
                new PartitionTransformation<>(
                        source.getTransformation(), new RebalancePartitioner<>());

        return new DataStream<>(env, transformation);
    }
}
