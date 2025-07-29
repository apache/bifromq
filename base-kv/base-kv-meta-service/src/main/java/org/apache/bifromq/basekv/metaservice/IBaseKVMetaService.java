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

package org.apache.bifromq.basekv.metaservice;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baseenv.EnvProvider;

/**
 * The meta service of base-kv.
 */
public interface IBaseKVMetaService extends AutoCloseable {
    Scheduler SHARED_SCHEDULER = Schedulers.from(ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), EnvProvider.INSTANCE.newThreadFactory("basekv-metadata-manager", true)),
        "basekv-metadata-manager"));

    static IBaseKVMetaService newInstance(ICRDTService crdtService) {
        return new BaseKVMetaService(crdtService);
    }

    /**
     * the id of the base-kv cluster currently discovered.
     *
     * @return the set of cluster id
     */
    Observable<Set<String>> clusterIds();

    /**
     * Get the balancer state proposer of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @return the balancer state proposer of the cluster
     */
    IBaseKVStoreBalancerStatesProposer balancerStatesProposer(String clusterId);

    /**
     * Get the balancer states proposal of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @return the balancer states proposal of the cluster
     */
    IBaseKVStoreBalancerStatesProposal balancerStatesProposal(String clusterId);

    /**
     * Get the observer of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @return the observer of the cluster
     */
    IBaseKVLandscapeObserver landscapeObserver(String clusterId);

    /**
     * Get the reporter of the store of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @param storeId the id of the store
     * @return the reporter of the store
     */
    IBaseKVLandscapeReporter landscapeReporter(String clusterId, String storeId);

    /**
     * Get the balancer states observer of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @return the balancer states observer of the cluster
     */
    IBaseKVStoreBalancerStatesObserver balancerStatesObserver(String clusterId);

    /**
     * Get the balancer states reporter of the base-kv cluster.
     *
     * @param clusterId the id of the cluster
     * @param storeId the id of the store
     * @return the balancer states reporter of the store
     */
    IBaseKVStoreBalancerStatesReporter balancerStatesReporter(String clusterId, String storeId);

    /**
     * Stop the meta service.
     */
    void close();
}
