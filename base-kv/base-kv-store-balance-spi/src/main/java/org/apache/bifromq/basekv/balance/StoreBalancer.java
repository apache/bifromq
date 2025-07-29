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

package org.apache.bifromq.basekv.balance;

import com.google.protobuf.Struct;
import java.util.Set;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

/**
 * The base class for implementing store balancer.
 */
public abstract class StoreBalancer {
    protected final Logger log;
    protected final String clusterId;
    protected final String localStoreId;

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public StoreBalancer(String clusterId, String localStoreId) {
        this.log = MDCLogger.getLogger("balancer.logger", "clusterId", clusterId, "storeId", localStoreId);
        this.clusterId = clusterId;
        this.localStoreId = localStoreId;
    }

    public Struct initialLoadRules() {
        return Struct.getDefaultInstance();
    }

    public boolean validate(Struct loadRules) {
        return Struct.getDefaultInstance().equals(loadRules);
    }

    /**
     * Update the store balancer with latest load rules.
     * <br>
     * It's up to the balancer implementation to decide how to interpret the rules to generate concrete balancing action
     * commands.
     *
     * @param loadRules the latest load rules
     */
    public void update(Struct loadRules) {
        // do nothing by default
    }

    /**
     * Update the store balancer with landscape.
     *
     * @param landscape the latest store descriptors
     */
    public abstract void update(Set<KVRangeStoreDescriptor> landscape);

    public abstract BalanceResult balance();

    public void close() {
        // do nothing by default
    }
}
