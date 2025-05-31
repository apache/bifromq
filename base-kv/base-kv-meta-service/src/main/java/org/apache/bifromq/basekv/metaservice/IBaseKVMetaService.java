/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.metaservice;

import org.apache.bifromq.basecrdt.service.ICRDTService;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;

/**
 * The meta service of base-kv.
 */
public interface IBaseKVMetaService extends AutoCloseable {
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
     * Get the metadata manager of the cluster.
     *
     * @param clusterId the id of the cluster
     * @return the metadata manager of the cluster
     */
    IBaseKVClusterMetadataManager metadataManager(String clusterId);

    /**
     * Stop the meta service.
     */
    void close();
}
