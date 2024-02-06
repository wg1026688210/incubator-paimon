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

package org.apache.paimon.flink.compact;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

public class StreamingTableScanner<T> implements CompactionTableScanner<T> {
    private final AtomicBoolean isRunning;

    private final long monitorInterval;

    private final TableScanLogic<T> tableScanLogic;

    public StreamingTableScanner(
            long monitorInterval, TableScanLogic<T> tableScanLogic, AtomicBoolean isRunning) {
        this.monitorInterval = monitorInterval;
        this.tableScanLogic = tableScanLogic;
        this.isRunning = isRunning;
    }

    @SuppressWarnings("BusyWait")
    @Override
    public void scan(SourceFunction.SourceContext<T> ctx) throws Exception {
        while (isRunning.get()) {
            Boolean isEmpty = tableScanLogic.collectFiles(ctx);
            if (isEmpty == null) return;
            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }
}
