package org.apache.paimon.flink.sink;

import org.apache.paimon.options.Options;

/** test class for {@link TableWriteOperator} with primarykey writer. */
public class PrimaryKeyWriterOperatorTest extends WriterOperatorTestBase {
    @Override
    protected void setTableConfig(Options options) {
        options.set("primary-key", "a");
        options.set("bucket-key", "a");
        options.set("write-buffer-size", "256 b");
        options.set("page-size", "32 b");
    }
}
