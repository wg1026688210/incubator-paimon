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

package org.apache.paimon.flink.action.cdc.format.autohome;

import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.FIELD_MAPPING;

/** Parser for field mapping. */
public class FieldMappingParser {
    public static Map<String, Map<String, String>> parseFieldMapping(
            MultipleParameterToolAdapter params) {
        Map<String, Map<String, String>> fieldMapping = new HashMap<>();

        // tableName=field1:mappingField1,field2:mappingField2
        Collection<String> multiParameter = params.getMultiParameter(FIELD_MAPPING);
        for (String fieldMappingStr : multiParameter) {
            String[] split = fieldMappingStr.split("=");
            String tableName = split[0];
            String[] fieldMappings = split[1].split(",");
            Map<String, String> fieldMap = new LinkedHashMap<>();
            for (String fieldMappingStr1 : fieldMappings) {
                String[] split1 = fieldMappingStr1.split(":");
                fieldMap.put(split1[0], split1[1]);
            }
            fieldMapping.put(tableName, fieldMap);
        }
        return fieldMapping;
    }
}
