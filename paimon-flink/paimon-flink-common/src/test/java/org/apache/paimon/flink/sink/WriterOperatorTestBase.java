package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalRowTypeSerializer;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.flink.utils.MetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** test class for {@link TableWriteOperator}. */
public abstract class WriterOperatorTestBase {
    private static final RowType ROW_TYPE =
            RowType.of(new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"a", "b"});
    @TempDir public java.nio.file.Path tempDir;
    protected Path tablePath;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
    }

    @Test
    public void testMetric() throws Exception {
        String tableName = tablePath.getName();
        FileStoreTable fileStoreTable = createFileStoreTable();
        RowDataStoreWriteOperator rowDataStoreWriteOperator =
                getRowDataStoreWriteOperator(fileStoreTable);

        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createWriteOperatorHarness(fileStoreTable, rowDataStoreWriteOperator);

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(serializer);
        harness.open();

        int size = 10;
        for (int i = 0; i < size; i++) {
            GenericRow row = GenericRow.of(1, 1);
            harness.processElement(row, 1);
        }
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 2);
        harness.notifyOfCompletedCheckpoint(1);

        MetricGroup metricGroup =
                rowDataStoreWriteOperator
                        .getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", tableName)
                        .addGroup("commit_user", "test")
                        .addGroup("writer");

        Counter writeRecordCount = MetricUtils.getCounter(metricGroup, "writeRecordCount");
        Assertions.assertThat(writeRecordCount.getCount()).isEqualTo(size);

        // test histogram has sample
        Histogram writeCostMS = MetricUtils.getHistogram(metricGroup, "writeCostMS");
        Assertions.assertThat(writeCostMS.getCount()).isEqualTo(size);

        Histogram flushCostMS = MetricUtils.getHistogram(metricGroup, "flushCostMS");
        Assertions.assertThat(flushCostMS.getCount()).isGreaterThan(0);

        Histogram prepareCommitCostMS =
                MetricUtils.getHistogram(metricGroup, "prepareCommitCostMS");
        Assertions.assertThat(prepareCommitCostMS.getCount()).isGreaterThan(0);

        Histogram syncLastestCompactionCostMS =
                MetricUtils.getHistogram(metricGroup, "syncLastestCompactionCostMS");
        Assertions.assertThat(syncLastestCompactionCostMS.getCount()).isGreaterThan(0);

        Gauge<Long> bufferPreemptCount = MetricUtils.getGauge(metricGroup, "bufferPreemptCount");
        Assertions.assertThat(bufferPreemptCount.getValue()).isEqualTo(0);

        Gauge<Long> totalWriteBufferSizeByte =
                MetricUtils.getGauge(metricGroup, "totalWriteBufferSizeByte");
        Assertions.assertThat(totalWriteBufferSizeByte.getValue()).isEqualTo(256);

        GenericRow row = GenericRow.of(1, 1);
        harness.processElement(row, 1);
        Gauge<Long> usedWriteBufferSizeByte =
                MetricUtils.getGauge(metricGroup, "usedWriteBufferSizeByte");
        Assertions.assertThat(usedWriteBufferSizeByte.getValue()).isGreaterThan(0);
    }

    @NotNull
    private static OneInputStreamOperatorTestHarness<InternalRow, Committable>
            createWriteOperatorHarness(
                    FileStoreTable fileStoreTable, RowDataStoreWriteOperator operator)
                    throws Exception {
        InternalTypeInfo<InternalRow> internalRowInternalTypeInfo =
                new InternalTypeInfo<>(new InternalRowTypeSerializer(ROW_TYPE));
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(
                        operator,
                        internalRowInternalTypeInfo.createSerializer(new ExecutionConfig()));
        return harness;
    }

    @NotNull
    private static RowDataStoreWriteOperator getRowDataStoreWriteOperator(
            FileStoreTable fileStoreTable) {
        StoreSinkWrite.Provider provider =
                (table, commitUser, state, ioManager, memoryPool, metricGroup) ->
                        new StoreSinkWriteImpl(
                                table,
                                commitUser,
                                state,
                                ioManager,
                                false,
                                false,
                                true,
                                memoryPool,
                                metricGroup);
        RowDataStoreWriteOperator operator =
                new RowDataStoreWriteOperator(fileStoreTable, null, provider, "test");
        return operator;
    }

    abstract void setTableConfig(Options options);

    protected FileStoreTable createFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        setTableConfig(conf);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        List<String> primaryKeys = setKeys(conf, CoreOptions.PRIMARY_KEY);
        List<String> paritionKeys = setKeys(conf, CoreOptions.PARTITION);

        schemaManager.createTable(
                new Schema(ROW_TYPE.getFields(), paritionKeys, primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }

    @NotNull
    private static List<String> setKeys(Options conf, ConfigOption<String> primaryKey) {
        List<String> primaryKeys =
                Optional.ofNullable(conf.get(CoreOptions.PRIMARY_KEY))
                        .map(key -> Arrays.asList(key.split(",")))
                        .orElse(Collections.emptyList());
        conf.remove(primaryKey.key());
        return primaryKeys;
    }
}
