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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * A dedicated operator for manual triggered compaction.
 *
 * <p>In-coming records are generated by sources built from {@link
 * org.apache.paimon.flink.source.CompactorSourceBuilder}. The records will contain partition keys
 * in the first few columns, and bucket number in the last column.
 */
public class StoreCompactOperator extends PrepareCommitOperator<RowData, Committable> {

    private final FileStoreTable table;
    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final boolean isStreaming;
    private final String initialCommitUser;

    private transient StoreSinkWriteState state;
    private transient StoreSinkWrite write;
    private transient DataFileMetaSerializer dataFileMetaSerializer;

    public StoreCompactOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            boolean isStreaming,
            String initialCommitUser) {
        Preconditions.checkArgument(
                !table.coreOptions().writeOnly(),
                CoreOptions.WRITE_ONLY.key() + " should not be true for StoreCompactOperator.");
        this.table = table;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.isStreaming = isStreaming;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        String commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        state =
                new StoreSinkWriteState(
                        context,
                        (tableName, partition, bucket) ->
                                ChannelComputer.select(
                                                partition,
                                                bucket,
                                                getRuntimeContext().getNumberOfParallelSubtasks())
                                        == getRuntimeContext().getIndexOfThisSubtask());

        write =
                storeSinkWriteProvider.provide(
                        table,
                        commitUser,
                        state,
                        getContainingTask().getEnvironment().getIOManager());
    }

    @Override
    public void open() throws Exception {
        super.open();
        dataFileMetaSerializer = new DataFileMetaSerializer();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData record = element.getValue();

        long snapshotId = record.getLong(0);
        BinaryRow partition = deserializeBinaryRow(record.getBinary(1));
        int bucket = record.getInt(2);
        byte[] serializedFiles = record.getBinary(3);
        List<DataFileMeta> files = dataFileMetaSerializer.deserializeList(serializedFiles);

        if (isStreaming) {
            write.notifyNewFiles(snapshotId, partition, bucket, files);
            write.compact(partition, bucket, false);
        } else {
            Preconditions.checkArgument(
                    files.isEmpty(),
                    "Batch compact job does not concern what files are compacted. "
                            + "They only need to know what buckets are compacted.");
            write.compact(partition, bucket, true);
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        return write.prepareCommit(doCompaction, checkpointId);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        write.snapshotState();
        state.snapshotState();
    }

    @Override
    public void close() throws Exception {
        super.close();
        write.close();
    }
}