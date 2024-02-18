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

package org.apache.paimon.flink.source;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.operator.BatchMultiSourceFunction;
import org.apache.paimon.flink.source.operator.BatchUnawareSourceFunction;
import org.apache.paimon.flink.source.operator.StreamingMultiSourceFunction;
import org.apache.paimon.flink.source.operator.StreamingUnawareSourceFunction;
import org.apache.paimon.table.system.BucketsTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.regex.Pattern;

/**
 * source builder to build a Flink compactor source for multi-tables. This is for dedicated
 * compactor jobs in combined mode.
 */
public class CombineCompactorSourceBuilder {
    private final Catalog.Loader catalogLoader;
    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;
    private final long monitorInterval;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;

    public CombineCompactorSourceBuilder(
            Catalog.Loader catalogLoader,
            Pattern databasePattern,
            Pattern includingPattern,
            Pattern excludingPattern,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.monitorInterval = monitorInterval;
    }

    public CombineCompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public CombineCompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public DataStream<RowData> buildForMultiBucketTableSource() {
        Preconditions.checkArgument(env != null, "StreamExecutionEnvironment should not be null.");
        RowType produceType = BucketsTable.getRowType();
        if (isContinuous) {
            return StreamingMultiSourceFunction.buildSource(
                    env,
                    "Combine-MultiBucketTables--StreamingCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        } else {
            return BatchMultiSourceFunction.buildSource(
                    env,
                    "Combine-MultiBucketTables-BatchCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        }
    }

    public DataStream<AppendOnlyCompactionTask> buildForUnawareBucketsTableSource() {
        Preconditions.checkArgument(env != null, "StreamExecutionEnvironment should not be null.");
        if (isContinuous) {
            return StreamingUnawareSourceFunction.buildSource(
                    env,
                    "Combine-UnawareBucketTables-StreamingCompactorSource",
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        } else {
            return BatchUnawareSourceFunction.buildSource(
                    env,
                    "Combine-UnawareBucketTables-BatchCompactorSource",
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        }
    }
}
