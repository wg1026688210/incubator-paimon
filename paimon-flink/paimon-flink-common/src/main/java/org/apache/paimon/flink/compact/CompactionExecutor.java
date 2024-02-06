package org.apache.paimon.flink.compact;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface CompactionExecutor<T> {
    void execute(SourceFunction.SourceContext<T> ctx)  throws Exception;

//    public void incrementMonitor(SourceFunction.SourceContext<T> ctx) throws Exception {
//        this.ctx = ctx;
//        while (isRunning) {
//            Boolean isEmpty = execute();
//            if (isEmpty == null) return;
//            if (isEmpty) {
//                Thread.sleep(monitorInterval);
//            }
//        }
//    }
//
//    public void batchMonitor(SourceFunction.SourceContext<T> ctx) throws Exception {
//        this.ctx = ctx;
//        if (isRunning) {
//            Boolean isEmpty = execute();
//            if (isEmpty == null) return;
//            if (isEmpty) {
//                throw new Exception(
//                        "No file were collected. Please ensure there are tables detected after pattern matching");
//            }
//        }
}

