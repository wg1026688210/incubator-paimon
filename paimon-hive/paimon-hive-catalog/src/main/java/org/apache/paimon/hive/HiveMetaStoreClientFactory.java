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

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;

/** the factory of hive metastore. */
public class HiveMetaStoreClientFactory {
    protected static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientFactory.class);

    private HiveMetaStoreClientFactory() {}

    public static HiveMetaStoreClient getClient(HiveConf conf, String proxyUser) throws Exception {
        if (proxyUser == null) {
            throw new IllegalAccessException("proxyUser should not be null.");
        }
        HiveMetaStoreClient client;
        LOG.info("Create MyHiveMetaStoreClient use proxy user {}", proxyUser);
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(proxyUser, loginUser);

        client =
                ugi.doAs(
                        new PrivilegedExceptionAction<HiveMetaStoreClient>() {
                            @Override
                            public HiveMetaStoreClient run() throws Exception {
                                try {
                                    HiveTokenProvider hiveTokenProvider = new HiveTokenProvider();
                                    Credentials credentials1 = ugi.getCredentials();
                                    hiveTokenProvider.obtainDelegationTokens(conf, credentials1);
                                    ugi.addCredentials(credentials1);

                                    Enhancer enhancer = new Enhancer();
                                    enhancer.setCallback(new AutoMethodInterceptor(ugi, proxyUser));
                                    enhancer.setSuperclass(HiveMetaStoreClient.class);
                                    return (HiveMetaStoreClient) create(conf, enhancer);
                                } catch (Exception e) {
                                    LOG.error(
                                            "Create HiveMetaStoreClient error : {}",
                                            e.getMessage(),
                                            e);
                                    return null;
                                }
                            }
                        });
        if (client == null) {
            throw new IllegalArgumentException("Please chk log.");
        }
        return client;
    }

    private static Object create(HiveConf conf, Enhancer enhancer) {
        if (DetectHiveUtil.hive3PresentOnClasspath()) {
            return enhancer.create(new Class[] {Configuration.class}, new Object[] {conf});
        } else {
            return enhancer.create(new Class[] {HiveConf.class}, new Object[] {conf});
        }
    }

    private static class AutoMethodInterceptor implements MethodInterceptor {
        private UserGroupInformation ugi;
        private String proxyUser;

        private AutoMethodInterceptor(UserGroupInformation ugi, String proxyUser) {
            this.ugi = ugi;
            this.proxyUser = proxyUser;
        }

        @Override
        public Object intercept(
                Object proxy, Method method, Object[] objects, MethodProxy methodProxy)
                throws Throwable {
            if (this.proxyUser == null || this.ugi == null) {
                return methodProxy.invokeSuper(proxy, objects);
            }

            final Throwable[] throwable = {null};
            Object result =
                    this.ugi.doAs(
                            (PrivilegedExceptionAction<Object>)
                                    () -> {
                                        try {
                                            return methodProxy.invokeSuper(proxy, objects);
                                        } catch (Throwable e) {
                                            throwable[0] = e;
                                        }
                                        return null;
                                    });
            if (throwable[0] != null) {
                throw throwable[0];
            }
            return result;
        }
    }
}
