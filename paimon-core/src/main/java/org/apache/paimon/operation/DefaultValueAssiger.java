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
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Projection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * the field Default value assigner. note that the invoke of assigning should be after merge and
 * schema evolution
 */
public class DefaultValueAssiger {
    private int[][] project;

    private TableSchema tableSchema;

    private RowType valueType;

    public DefaultValueAssiger(int[][] project, TableSchema tableSchema, RowType valueType) {
        this.project = project;
        this.tableSchema = tableSchema;
        this.valueType = valueType;
    }

    /** assign default value for colomn which value is null. */
    public RecordReader<InternalRow> assignFieldsDefaultValue(RecordReader<InternalRow> reader) {
        RecordReader<InternalRow> result = reader;

        CoreOptions coreOptions = new CoreOptions(tableSchema.options());
        Options defaultValues = coreOptions.getFieldDefaultValues();
        List<DataField> fields = Collections.emptyList();
        if (!defaultValues.keySet().isEmpty()) {
            if (project != null) {
                fields = Projection.of(project).project(valueType).getFields();
            } else {
                fields = valueType.getFields();
            }
        }

        if (!fields.isEmpty()) {
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
                DefaultValueRow defaultValueRow = DefaultValueRow.from(defaultValueMapping);
                result = reader.transform(defaultValueRow::replaceRow);
            }
        }

        return result;
    }

    public static List<Predicate> filterPredicate(Set<String> fieldsWithDefaultValue, Predicate filters) {
        ArrayList<Predicate> filterWithouDefaultValueField = new ArrayList<>();
        if (filters == null) {
            return filterWithouDefaultValueField;
        }

        List<Predicate> predicates = PredicateBuilder.splitAnd(filters);
        for (Predicate filter : predicates) {
            // TODO improve predicate tree with replacing always true and always false
            PredicateReplaceVisitor deletePredicateWithFieldNameVisitor =
                    predicate -> {
                        if (fieldsWithDefaultValue.contains(predicate.fieldName())) {
                            return Optional.empty();
                        }
                        return Optional.of(predicate);
                    };
            filter.visit(deletePredicateWithFieldNameVisitor)
                    .ifPresent(filterWithouDefaultValueField::add);
        }


        return filterWithouDefaultValueField;
    }
}
