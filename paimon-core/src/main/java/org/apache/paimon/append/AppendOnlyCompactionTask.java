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

package org.apache.paimon.append;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Compaction task generated by {@link AppendOnlyTableCompactionCoordinator}. */
public class AppendOnlyCompactionTask {

    private final BinaryRow partition;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;

    private final Identifier tableIdentifier;

    public AppendOnlyCompactionTask(BinaryRow partition, List<DataFileMeta> files) {
        this(partition, files, null);
    }

    public AppendOnlyCompactionTask(
            BinaryRow partition, List<DataFileMeta> files, Identifier identifier) {

        Preconditions.checkArgument(
                files != null && files.size() > 1,
                "AppendOnlyCompactionTask need more than one file input.");
        this.partition = partition;
        compactBefore = new ArrayList<>(files);
        compactAfter = new ArrayList<>();
        this.tableIdentifier = identifier;
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public CommitMessage doCompact(AppendOnlyFileStoreWrite write) throws Exception {
        compactAfter.addAll(write.compactRewriter(partition, 0).rewrite(compactBefore));
        CompactIncrement compactIncrement =
                new CompactIncrement(compactBefore, compactAfter, Collections.emptyList());
        return new CommitMessageImpl(
                partition,
                0, // bucket 0 is bucket for unaware-bucket table for compatibility with the old
                // design
                NewFilesIncrement.emptyIncrement(),
                compactIncrement);
    }

    public Identifier tableIdentifier() {
        return tableIdentifier;
    }

    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter, tableIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AppendOnlyCompactionTask that = (AppendOnlyCompactionTask) o;
        return Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter)
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactionTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s, "
                        + "tableIdentifier = %s}",
                partition, compactBefore, compactAfter, tableIdentifier);
    }
}
