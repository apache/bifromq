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

package org.apache.bifromq.retain.store.gc;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.retain.rpc.proto.GCRequest;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetainStoreGCProcessorTest {
    @Mock
    private IBaseKVStoreClient storeClient;
    private RetainStoreGCProcessor gcProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void testGCNonExistTenant() {
        String tenantId = "tenantId";
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        when(storeClient.latestEffectiveRouter()).thenReturn(Collections.emptyNavigableMap());

        IRetainStoreGCProcessor.Result result =
            gcProcessor.gc(System.nanoTime(), tenantId, null, HLC.INST.getPhysical()).join();
        assertEquals(result, IRetainStoreGCProcessor.Result.OK);
    }

    @Test
    public void testGCTenantWithNullExpirySeconds() {
        String tenantId = "tenantId";
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .build();
        KVRangeSetting setting = new KVRangeSetting("clueter", "store", rangeDescriptor);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, setting);
        }});
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, tenantId, null, now);
        verify(storeClient).execute(eq(setting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return gcRequest.getTenantId().equals(tenantId)
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && !gcRequest.hasExpirySeconds();
        }));
    }

    @Test
    public void testGCTenantWithExpirySeconds() {
        String tenantId = "tenantId";
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        int expirySeconds = 10;
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .build();
        KVRangeSetting setting = new KVRangeSetting("cluster", "store", rangeDescriptor);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, setting);
        }});
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, tenantId, expirySeconds, now);
        verify(storeClient).execute(eq(setting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return gcRequest.getTenantId().equals(tenantId)
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && gcRequest.getExpirySeconds() == expirySeconds;
        }));
    }

    @Test
    public void testGCWithLocalStoreAndExpirySeconds() {
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        int expirySeconds = 10;
        String localStoreId = "localStore";
        gcProcessor = new RetainStoreGCProcessor(storeClient, localStoreId);
        KVRangeDescriptor localDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.next(1))
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build())
            .build();
        KVRangeDescriptor remoteDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.next(1))
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).build())
            .build();
        KVRangeSetting localSetting = new KVRangeSetting("cluster", localStoreId, localDescriptor);
        KVRangeSetting remoteSetting = new KVRangeSetting("cluster", "remoteStore", remoteDescriptor);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(localDescriptor.getBoundary(), localSetting);
            put(remoteDescriptor.getBoundary(), remoteSetting);
        }});
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, null, expirySeconds, now);
        verify(storeClient).execute(eq(localSetting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != localDescriptor.getVer()
                || !req.getKvRangeId().equals(localDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return !gcRequest.hasTenantId()
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && gcRequest.getExpirySeconds() == expirySeconds;
        }));
    }

    @Test
    public void testGCWithLocalStoreSpecified() {
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        String localStoreId = "localStore";
        gcProcessor = new RetainStoreGCProcessor(storeClient, localStoreId);
        KVRangeDescriptor localDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.next(1))
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build())
            .build();
        KVRangeDescriptor remoteDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.next(1))
            .setVer(1)
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("a")).build())
            .build();
        KVRangeSetting localSetting = new KVRangeSetting("cluster", localStoreId, localDescriptor);
        KVRangeSetting remoteSetting = new KVRangeSetting("cluster", "remoteStore", remoteDescriptor);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(localDescriptor.getBoundary(), localSetting);
            put(remoteDescriptor.getBoundary(), remoteSetting);
        }});
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, null, null, now);
        verify(storeClient).execute(eq(localSetting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != localDescriptor.getVer()
                || !req.getKvRangeId().equals(localDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return !gcRequest.hasTenantId()
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && !gcRequest.hasExpirySeconds();
        }));
    }
}
