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

package org.apache.bifromq.basecrdt.core.internal;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDTInflater;
import org.apache.bifromq.basecrdt.proto.Replica;

/**
 * A factory for creating {@link CausalCRDTInflater} instances.
 */
public class CausalCRDTInflaterFactory {
    private final String storeId;
    private final ScheduledExecutorService executor;
    private final Duration inflationInterval;
    private final Duration orHistoryExpiryTime;
    private final Duration maxCompactionTime;
    private final String[] tags;

    public CausalCRDTInflaterFactory(String storeId,
                                     Duration inflationInterval,
                                     Duration orHistoryExpiryTime,
                                     Duration maxCompactionTime,
                                     ScheduledExecutorService executor,
                                     String... tags) {
        this.storeId = storeId;
        this.executor = executor;
        this.inflationInterval = inflationInterval;
        this.orHistoryExpiryTime = orHistoryExpiryTime;
        this.maxCompactionTime = maxCompactionTime;
        this.tags = tags;
    }

    /**
     * Create a {@link CausalCRDTInflater} instance.
     *
     * @param replicaId The typed ID of the replicaId.
     * @return The {@link CausalCRDTInflater} instance.
     */
    public ICausalCRDTInflater<?, ?> create(Replica replicaId) {
        CRDTURI.checkURI(replicaId.getUri());

        IReplicaStateLattice lattice =
            new InMemReplicaStateLattice(storeId, replicaId, orHistoryExpiryTime, maxCompactionTime);

        return switch (CRDTURI.parseType(replicaId.getUri())) {
            case aworset -> new AWORSetInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case rworset -> new RWORSetInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case ormap -> new ORMapInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case cctr -> new CCounterInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case dwflag -> new DWFlagInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case ewflag -> new EWFlagInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
            case mvreg -> new MVRegInflater(storeId, replicaId, lattice, executor, inflationInterval, tags);
        };
    }
}
