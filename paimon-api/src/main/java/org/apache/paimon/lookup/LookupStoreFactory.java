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

package org.apache.paimon.lookup;

import java.io.File;
import java.io.IOException;

/**
 * A key-value store for lookup, key-value store should be single binary file written once and ready
 * to be used. This factory provide two interfaces:
 *
 * <ul>
 *   <li>Writer: written once to prepare binary file.
 *   <li>Reader: lookup value by key bytes.
 * </ul>
 */
public interface LookupStoreFactory {

    LookupStoreWriter createWriter(File file) throws IOException;

    LookupStoreReader createReader(File file) throws IOException;
}
