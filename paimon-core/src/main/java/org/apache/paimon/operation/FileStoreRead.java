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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Projection;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Read operation which provides {@link RecordReader} creation.
 *
 * @param <T> type of record to read.
 */
public interface FileStoreRead<T> {

    FileStoreRead<T> withFilter(Predicate predicate);

    /** Create a {@link RecordReader} from split. */
    RecordReader<T> createReader(DataSplit split) throws IOException;

    /**
     * get default value for colomn which value is null.
     *
     * @param tableSchema
     * @param pushdownProjection
     * @param rowType
     * @return
     */
    default Optional<InternalRow> getDefaultColumnValues(
            TableSchema tableSchema, int[][] pushdownProjection, RowType rowType) {
        Options defaultValues =
                new Options(tableSchema.options())
                        .removePrefix(CoreOptions.COLUMN_DEFAULTVALUE_PREFIX.key() + ".");

        Optional<InternalRow> result = Optional.empty();
        if (!defaultValues.keySet().isEmpty()) {
            List<DataField> fields;
            if (pushdownProjection != null) {
                fields = Projection.of(pushdownProjection).project(rowType).getFields();
            } else {
                fields = rowType.getFields();
            }

            GenericRow defaultValueMapping = new GenericRow(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                DataField dataField = fields.get(i);
                String defaultValueStr = defaultValues.get(dataField.name());
                if (defaultValueStr == null) {
                    continue;
                }

                CastExecutor<Object, Object> resolve =
                        (CastExecutor<Object, Object>)
                                CastExecutors.resolve(VarCharType.STRING_TYPE, dataField.type());
                if (resolve != null) {
                    Object defaultValue = resolve.cast(BinaryString.fromString(defaultValueStr));
                    defaultValueMapping.setField(i, defaultValue);
                }
            }

            if (defaultValueMapping.getFieldCount() > 0) {
                result = Optional.of(defaultValueMapping);
            }
        }
        return result;
    }
}
