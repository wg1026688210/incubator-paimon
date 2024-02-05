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
import org.apache.paimon.table.Table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactTable;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for the new Paimon table.
 */
public abstract class CombineModeMonitorSourceFunction<T> extends RichSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(CombineModeMonitorSourceFunction.class);

    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;
    protected final long monitorInterval;
    protected final boolean isStreaming;

    protected transient Catalog catalog;

    public CombineModeMonitorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.monitorInterval = monitorInterval;
        this.isStreaming = isStreaming;
    }

    protected volatile boolean isRunning = true;

    protected transient SourceContext<T> ctx;

    @Override
    public void open(Configuration parameters) throws Exception {
        catalog = catalogLoader.load();
    }

    @Override
    public void cancel() {
        // this is to cover the case where cancel() is called before the run()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    protected void updateTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();

        for (String databaseName : databases) {
            if (databasePattern.matcher(databaseName).matches()) {
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    if (shouldCompactTable(identifier, includingPattern, excludingPattern)
                            && (!hasScanned(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }

                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        if (fileStoreTable.bucketMode() == BucketMode.UNAWARE) {
                            LOG.info(
                                    String.format(
                                                    "the bucket mode of %s is unware. ",
                                                    identifier.getFullName())
                                            + "currently, the table with unware bucket mode is not support in combined mode.");
                            continue;
                        }

                        applyFileTable(fileStoreTable, identifier);
                    }
                }
            }
        }
    }

    abstract boolean hasScanned(Identifier identifier);

    abstract void applyFileTable(FileStoreTable fileStoreTable, Identifier identifier);

    @SuppressWarnings("BusyWait")
    public void incrementMonitor(SourceContext<T> ctx) throws Exception {
        this.ctx = ctx;
        while (isRunning) {
            Boolean isEmpty = execute();
            if (isEmpty == null) return;
            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    public void batchMonitor(SourceContext<T> ctx) throws Exception {
        this.ctx = ctx;
        if (isRunning) {
            Boolean isEmpty = execute();
            if (isEmpty == null) return;
            if (isEmpty) {
                throw new Exception(
                        "No file were collected. Please ensure there are tables detected after pattern matching");
            }
        }
    }

    abstract Boolean execute() throws Exception;
}
