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

package org.apache.bifromq.basekv.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.testng.annotations.Test;

/**
 * Regression: mutation pipelines dropped by a route refresh must be closed, not silently leaked.
 */
public class BaseKVStoreClientPipelineRefreshTest {

    private static KVRangeId rangeId(long id) {
        return KVRangeId.newBuilder().setEpoch(id).setId(id).build();
    }

    @Test
    public void closesPipelineWhenRangeLeadershipMovesAway() {
        IMutationPipeline kept = mock(IMutationPipeline.class);
        IMutationPipeline dropped = mock(IMutationPipeline.class);
        Map<String, Map<KVRangeId, IMutationPipeline>> current = Map.of(
            "storeA", Map.of(rangeId(1), kept, rangeId(2), dropped));
        Map<String, Map<KVRangeId, IMutationPipeline>> next = Map.of(
            "storeA", Map.of(rangeId(1), kept));

        BaseKVStoreClient.closeDroppedMutationPipelines(current, next);

        verify(dropped).close();
        verify(kept, never()).close();
    }

    @Test
    public void closesPipelinesWhenStoreDisappears() {
        IMutationPipeline ppln = mock(IMutationPipeline.class);
        Map<String, Map<KVRangeId, IMutationPipeline>> current = Map.of("storeB", Map.of(rangeId(3), ppln));

        BaseKVStoreClient.closeDroppedMutationPipelines(current, Map.of());

        verify(ppln).close();
    }

    @Test
    public void closesReplacedPipelineInstance() {
        IMutationPipeline oldPpln = mock(IMutationPipeline.class);
        IMutationPipeline newPpln = mock(IMutationPipeline.class);
        Map<String, Map<KVRangeId, IMutationPipeline>> current = Map.of("storeA", Map.of(rangeId(1), oldPpln));
        Map<String, Map<KVRangeId, IMutationPipeline>> next = Map.of("storeA", Map.of(rangeId(1), newPpln));

        BaseKVStoreClient.closeDroppedMutationPipelines(current, next);

        verify(oldPpln).close();
        verify(newPpln, never()).close();
    }
}
