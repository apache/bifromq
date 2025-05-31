/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basekv.server;

/**
 * The interface of the BaseKV store server.
 */
public interface IBaseKVStoreServer {
    static BaseKVStoreServerBuilder builder() {
        return new BaseKVStoreServerBuilder();
    }

    /**
     * Get the member store of the provided cluster hosted by current store server.
     *
     * @param clusterId the id of the cluster
     * @return the id of the store hosted by the store server
     * @throws NullPointerException if the store server is not hosting any store for the given cluster
     */
    String storeId(String clusterId);

    void start();

    void stop();
}
