/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.basekv.localengine.spi;

import com.google.protobuf.Struct;
import javax.annotation.Nullable;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;

/**
 * Service provider for IKVEngine runtime binding.
 */
public interface IKVEngineProvider {
    /**
     * Engine type identifier used in configuration, such as "memory" or "rocksdb".
     */
    String type();

    /**
     * The default config for CPable KVEngine.
     *
     * @return the default config in struct
     */
    Struct defaultsForCPable();

    /**
     * The default config for WALable KVEngine.
     *
     * @return the default config in struct
     */
    Struct defaultsForWALable();

    /**
     * Create CPable KVEngine.
     *
     * @param overrideIdentity the override identity could be null
     * @param conf the complete config
     * @return the engine instance
     */
    IKVEngine<? extends ICPableKVSpace> createCPable(@Nullable String overrideIdentity, Struct conf);

    /**
     * Create WALable KVEngine.
     *
     * @param overrideIdentity the override identity could be null
     * @param conf the complete config
     * @return the engine instance
     */
    IKVEngine<? extends IWALableKVSpace> createWALable(String overrideIdentity, Struct conf);
}
