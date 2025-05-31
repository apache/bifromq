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

package org.apache.bifromq.basekv.store;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.util.VerUtil;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterSplitTest extends KVRangeStoreClusterTestTemplate {

    @Test(groups = "integration")
    public void splitFromLeaderStore() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(10)).until(() -> cluster.allKVRangeIds().size() == 2);
        for (KVRangeId kvRangeId : cluster.allKVRangeIds()) {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                KVRangeConfig kvRangeSettings = cluster.kvRangeSetting(kvRangeId);
                return kvRangeSettings.clusterConfig.getVotersCount() == 3;
            });

            KVRangeConfig kvRangeSettings = cluster.kvRangeSetting(kvRangeId);
            assertEquals(kvRangeSettings.ver, VerUtil.bump(genesisKVRangeSettings.ver, true));
            if (kvRangeId.equals(genesisKVRangeId)) {
                assertEquals(kvRangeSettings.leader, genesisKVRangeSettings.leader);
                assertEquals(kvRangeSettings.boundary, Boundary.newBuilder()
                    .setEndKey(ByteString.copyFromUtf8("e"))
                    .build());
            } else {
                assertEquals(kvRangeSettings.boundary, Boundary.newBuilder()
                    .setStartKey(ByteString.copyFromUtf8("e"))
                    .build());
            }
        }
    }

    @Test(groups = "integration")
    public void splitFromNonLeaderStore() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        String nonLeaderStore = nonLeaderStore(genesisKVRangeSettings);
        cluster.awaitKVRangeReady(nonLeaderStore, genesisKVRangeId);
        cluster.split(nonLeaderStore, genesisKVRangeSettings.ver, genesisKVRangeId, copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(20)).until(() -> cluster.allKVRangeIds().size() == 2);
        for (KVRangeId kvRangeId : cluster.allKVRangeIds()) {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                KVRangeConfig kvRangeSettings = cluster.kvRangeSetting(kvRangeId);
                return kvRangeSettings.clusterConfig.getVotersCount() == 3;
            });
            KVRangeConfig kvRangeSettings = cluster.kvRangeSetting(kvRangeId);
            assertEquals(kvRangeSettings.ver, VerUtil.bump(genesisKVRangeSettings.ver, true));
            if (kvRangeId.equals(genesisKVRangeId)) {
                assertEquals(kvRangeSettings.leader, genesisKVRangeSettings.leader);
                assertEquals(kvRangeSettings.boundary, Boundary.newBuilder()
                    .setEndKey(ByteString.copyFromUtf8("e"))
                    .build());
            } else {
                assertEquals(kvRangeSettings.boundary, Boundary.newBuilder()
                    .setStartKey(ByteString.copyFromUtf8("e"))
                    .build());
            }
        }
    }
}
