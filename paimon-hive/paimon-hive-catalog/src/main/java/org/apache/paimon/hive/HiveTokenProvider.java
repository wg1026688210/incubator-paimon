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

package org.apache.paimon.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/** the token of provider. */
public class HiveTokenProvider {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTokenProvider.class);

    public void obtainDelegationTokens(Configuration hadoopConf, Credentials creds)
            throws IOException, InterruptedException {
        HiveConf conf = new HiveConf(hadoopConf, HiveConf.class);
        conf.addResource(hadoopConf);

        String principalKey = "hive.metastore.kerberos.principal";
        String principal = conf.getTrimmed(principalKey, "");

        LOG.info("hive principal {} , {}", principal, hadoopConf.getTrimmed(principalKey, ""));
        String metastoreUri = conf.getTrimmed("hive.metastore.uris", "");
        LOG.info("hive metastoreUri {}", metastoreUri);

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        UserGroupInformation realUser1 = currentUser.getRealUser();
        if (realUser1 == null) {
            realUser1 = currentUser;
        }
        LOG.info("realuser {}", realUser1.getUserName());

        try {
            realUser1.doAs(
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            Hive hive = Hive.get(conf, HiveConf.class);
                            LOG.info("currentUser {}", currentUser.getUserName());
                            String tokenStr =
                                    hive.getDelegationToken(currentUser.getUserName(), principal);
                            Token hive2Token = new Token<DelegationTokenIdentifier>();
                            hive2Token.decodeFromUrlString(tokenStr);
                            creds.addToken(new Text("hive.server2.delegation.token"), hive2Token);
                            return null;
                        }
                    });
        } finally {
            Hive.closeCurrent();
        }
    }
}
