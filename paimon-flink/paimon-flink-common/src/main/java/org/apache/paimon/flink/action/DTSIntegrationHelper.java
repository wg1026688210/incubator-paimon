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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveMetaStoreClientFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

import static org.apache.paimon.flink.action.ActionFactory.DATABASE;

/** Helper class for DTS integration. */
public class DTSIntegrationHelper {
    private static final String URI =
            "thrift://10.28.234.10:48860,thrift://10.28.234.11:48860,thrift://10.28.234.12:48860,thrift://10.28.242.171:48860,thrift://10.28.242.172:48860,thrift://10.28.242.173:48860,thrift://10.28.242.174:48860,thrift://10.28.242.175:48860,thrift://10.28.242.176:48860";

    public static Options getDTSCatalogOptions(Options catalogOptions) {

        try {
            if (catalogOptions.get(CatalogOptions.DTSINTEGRATION_ENABLED)) {

                Options options = new Options();
                catalogOptions.toMap().forEach(options::set);
                options.set(CatalogOptions.URI, URI);
                CatalogContext catalogContext = CatalogContext.create(options);

                // query warehouse using hive client.
                HiveConf hiveConf = HiveCatalog.createHiveConf(catalogContext);

                IMetaStoreClient client =
                        HiveMetaStoreClientFactory.getClient(
                                hiveConf, catalogContext.options().get(CatalogOptions.PROXY_USER));
                Database database = client.getDatabase(options.get(DATABASE));

                // set warehouse
                String warehouse = database.getLocationUri();
                options.set(CatalogOptions.WAREHOUSE, warehouse);

                // mkdir db path
                Path dbPath = new Path(warehouse + "/" + options.get(DATABASE) + ".db");
                FileIO fileIO = FileIO.get(new Path(warehouse), catalogContext);
                if (!fileIO.exists(dbPath)) {
                    fileIO.mkdirs(dbPath);
                }

                return options;
            } else {
                return catalogOptions;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
