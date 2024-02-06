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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.system.BucketsTable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table and the new Paimon table
 *   <li>Creating the Tuple2<{@link Split}, String> splits corresponding to the incremental files.
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link MultiTablesReadOperator} which
 * can have parallelism greater than one.
 *
 * <p>Currently, the dedicated combine mode compaction of job for multi-tables with multi bucket rely on this monitor.
 */
public abstract class MultiBucketTablesFunction
        extends CombineModeCompactorSourceFunction<Tuple2<Split, String>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiBucketTablesFunction.class);

    protected transient Map<Identifier, BucketsTable> tablesMap;
    protected transient Map<Identifier, StreamTableScan> scansMap;

    public MultiBucketTablesFunction(
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
        scansMap = new HashMap<>();
    }

    @Override
    boolean hasScanned(Identifier identifier) {
        return tablesMap.containsKey(identifier);
    }

    @Override
    void applyFileTable(FileStoreTable fileStoreTable, Identifier identifier) {
        if (fileStoreTable.bucketMode() == BucketMode.UNAWARE) {
            LOG.info(
                    String.format("the bucket mode of %s is unware. ", identifier.getFullName())
                            + "currently, the table with unware bucket mode is not support in combined mode.");
            return;
        }

        BucketsTable bucketsTable =
                new BucketsTable(fileStoreTable, isStreaming, identifier.getDatabaseName())
                        .copy(compactOptions(isStreaming));
        tablesMap.put(identifier, bucketsTable);
        scansMap.put(identifier, bucketsTable.newReadBuilder().newStreamScan());
    }

    @Nullable
    @Override
    public Boolean execute() throws Exception {
        boolean isEmpty;
        synchronized (ctx.getCheckpointLock()) {
            if (!isRunning) {
                return null;
            }

            // check for new tables
            updateTableMap();

            try {
                // batch mode do not need check for new tables
                List<Tuple2<Split, String>> splits = new ArrayList<>();
                for (Map.Entry<Identifier, StreamTableScan> entry : scansMap.entrySet()) {
                    Identifier identifier = entry.getKey();
                    StreamTableScan scan = entry.getValue();
                    splits.addAll(
                            scan.plan().splits().stream()
                                    .map(split -> new Tuple2<>(split, identifier.getFullName()))
                                    .collect(Collectors.toList()));
                }
                isEmpty = splits.isEmpty();
                splits.forEach(ctx::collect);
            } catch (EndOfScanException esf) {
                LOG.info("Catching EndOfStreamException, the stream is finished.");
                return null;
            }
        }
        return isEmpty;
    }
}
