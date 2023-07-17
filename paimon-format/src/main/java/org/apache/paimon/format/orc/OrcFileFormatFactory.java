/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.orc;

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.options.Options;

import java.util.Properties;

/** Factory to create {@link OrcFileFormat}. */
public class OrcFileFormatFactory implements FileFormatFactory {

    public static final String IDENTIFIER = "orc";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public OrcFileFormat create(FormatContext formatContext) {
        return new OrcFileFormat(
                new FormatContext(
                        supplyDefaultOptions(formatContext.formatOptions()),
                        formatContext.getUnifyOptions(),
                        formatContext.readBatchSize()));
    }

    private Options supplyDefaultOptions(Options options) {
        if (!options.containsKey("compress")) {
            Properties properties = new Properties();
            options.addAllToProperties(properties);
            properties.setProperty("compress", "lz4");
            Options newOptions = new Options();
            properties.forEach((k, v) -> newOptions.setString(k.toString(), v.toString()));
            return newOptions;
        }
        return options;
    }
}
