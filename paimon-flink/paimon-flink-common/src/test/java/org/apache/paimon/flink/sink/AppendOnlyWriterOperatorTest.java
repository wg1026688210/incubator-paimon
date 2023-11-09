package org.apache.paimon.flink.sink;

import org.apache.paimon.options.Options;

/** test class for {@link TableWriteOperator} with append only writer. */
public class AppendOnlyWriterOperatorTest extends WriterOperatorTestBase {
    @Override
    protected void setTableConfig(Options options) {
        options.set("write-buffer-for-append", "true");
        options.set("write-buffer-size", "256 b");
        options.set("page-size", "32 b");
        options.set("write-buffer-spillable", "false");
    }
}
