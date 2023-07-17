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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

/** A {@link ParquetBuilder} for {@link InternalRow}. */
public class RowDataParquetBuilder implements ParquetBuilder<InternalRow> {

    private final RowType rowType;
    private final Options customizedConf;

    private final Options unifiedConf;

    public RowDataParquetBuilder(RowType rowType, Options customizedConf) {
        this(rowType,customizedConf,new Options());
    }
    public RowDataParquetBuilder(RowType rowType, Options customizedConf, Options unifiedConf) {
        this.rowType = rowType;
        this.customizedConf = customizedConf;
        this.unifiedConf = unifiedConf;
    }

    @Override
    public ParquetWriter<InternalRow> createWriter(OutputFile out, String compression)
            throws IOException {
        ParquetRowDataBuilder parquetRowDataBuilder = new ParquetRowDataBuilder(out, rowType, unifiedConf)
                .withCompressionCodec(CompressionCodecName.fromConf(getCompression(compression)))
                .withRowGroupSize(
                        customizedConf.getLong(
                                ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE))
                .withPageSize(
                        customizedConf.getInteger(
                                ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE))
                .withDictionaryPageSize(
                        customizedConf.getInteger(
                                ParquetOutputFormat.DICTIONARY_PAGE_SIZE,
                                ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE))
                .withMaxPaddingSize(
                        customizedConf.getInteger(
                                ParquetOutputFormat.MAX_PADDING_BYTES,
                                ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
                .withDictionaryEncoding(
                        customizedConf.getBoolean(
                                ParquetOutputFormat.ENABLE_DICTIONARY,
                                ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED))
                .withValidation(customizedConf.getBoolean(ParquetOutputFormat.VALIDATION, false))
                .withWriterVersion(
                        ParquetProperties.WriterVersion.fromString(
                                customizedConf.getString(
                                        ParquetOutputFormat.WRITER_VERSION,
                                        ParquetProperties.DEFAULT_WRITER_VERSION.toString())));

        getFieldsDictionaryOpt(parquetRowDataBuilder);

        ParquetWriter<InternalRow> build = parquetRowDataBuilder
                .build();
        return build;
    }

    private void  getFieldsDictionaryOpt(ParquetRowDataBuilder parquetRowDataBuilder) {
        ArrayList<String> disableDictionaryFields = new ArrayList<>();
        ArrayList<String> enableDictionaryFields = new ArrayList<>();

        for (String fieldName : rowType.getFieldNames()) {
            String dictionary = String.format("%s.%s.%s", OptionConstant.FORMAT_PREFIX, fieldName, OptionConstant.DICTIONARY_SUFFIX);
            if (unifiedConf.containsKey(dictionary)) {
                String fieldOpt = unifiedConf.getString(dictionary, null);
                if ("enable".equalsIgnoreCase(fieldOpt)) {
                    enableDictionaryFields.add(dictionary);
                } else if ("disable".equalsIgnoreCase(fieldOpt)) {
                    disableDictionaryFields.add(dictionary);
                }
            }
        }

        for (String enableDictionaryField : enableDictionaryFields) {
            parquetRowDataBuilder.withDictionaryEncoding(enableDictionaryField,true);
        }
        for (String disableDictionaryField : disableDictionaryFields) {
            parquetRowDataBuilder.withDictionaryEncoding(disableDictionaryField,false);
        }
    }

    public String getCompression(@Nullable String compression) {
        String compressName;
        if (null != compression) {
            compressName = compression;
        } else {
            compressName =
                    customizedConf.getString(
                            ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name());
        }
        return compressName;
    }
}
