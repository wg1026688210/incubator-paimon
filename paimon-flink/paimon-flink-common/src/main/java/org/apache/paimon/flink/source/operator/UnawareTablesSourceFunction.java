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
import org.apache.paimon.append.AppendOnlyTableCompactionCoordinator;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.UnawareCombineCompactionWorkerOperator;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;

import org.apache.flink.configuration.Configuration;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is the single (non-parallel) monitoring task , it is responsible for the dedicated
 * compaction of combine mode job for multi-tables with the unaware bucket.
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table and the new Paimon table.
 *   <li>Creating the {@link AppendOnlyCompactionTask} corresponding to the incremental files.
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The {@link AppendOnlyCompactionTask} to be read are forwarded to the downstream {@link
 * UnawareCombineCompactionWorkerOperator} which can have parallelism greater than one.
 *
 * <p>Currently, only the dedicated compaction of combine mode job for multi-tables with the fix and
 * dynamic bucket rely on this monitor.
 */
public abstract class UnawareTablesSourceFunction
        extends CombineModeMonitorSourceFunction<AppendOnlyCompactionTask> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(UnawareTablesSourceFunction.class);

    protected transient Map<Identifier, AppendOnlyTableCompactionCoordinator> tablesMap;

    public UnawareTablesSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            long monitorInterval) {
        super(
                catalogLoader,
                includingPattern,
                excludingPattern,
                databasePattern,
                isStreaming,
                monitorInterval);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        tablesMap = new HashMap<>();
        updateTableMap();
    }

    @Override
    boolean hasScanned(Identifier identifier) {
        return tablesMap.containsKey(identifier);
    }

    @Override
    void applyFileTable(FileStoreTable fileStoreTable, Identifier identifier) {
        if (fileStoreTable.bucketMode() == BucketMode.UNAWARE) {
            tablesMap.put(
                    identifier,
                    new AppendOnlyTableCompactionCoordinator(fileStoreTable, isStreaming));
        }
    }

    @Nullable
    @Override
    public Boolean execute() throws Exception {
        boolean isEmpty;
        try {
            if (!isRunning) {
                return null;
            }

            updateTableMap();
            // do scan and plan action, emit append-only compaction tasks.
            List<AppendOnlyCompactionTask> tasks = new ArrayList<>();
            for (Map.Entry<Identifier, AppendOnlyTableCompactionCoordinator> tableIdAndCoordinator :
                    tablesMap.entrySet()) {
                Identifier tableId = tableIdAndCoordinator.getKey();
                AppendOnlyTableCompactionCoordinator compactionCoordinator =
                        tableIdAndCoordinator.getValue();
                compactionCoordinator.run().stream()
                        .map(
                                task ->
                                        new AppendOnlyCompactionTask(
                                                task.partition(), task.compactBefore(), tableId))
                        .forEach(tasks::add);
            }

            isEmpty = tasks.isEmpty();
            tasks.forEach(ctx::collect);
        } catch (EndOfScanException esf) {
            LOG.info("Catching EndOfStreamException, the stream is finished.");
            return null;
        }
        return isEmpty;
    }
}
