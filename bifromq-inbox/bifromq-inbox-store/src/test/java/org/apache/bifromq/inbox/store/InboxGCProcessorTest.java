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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InboxGCProcessorTest {

    private final String localStoreId = "testLocalStoreId";
    private final KVRangeSetting localRangeSetting = new KVRangeSetting("cluster", localStoreId,
        KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).build());
    private final String remoteStoreId = "testRemoteStoreId";
    private final KVRangeSetting remoteRangeSetting = new KVRangeSetting("cluster", remoteStoreId,
        KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).setBoundary(FULL_BOUNDARY).build());
    @Mock
    private IBaseKVStoreClient storeClient;
    private InboxStoreGCProcessor inboxGCProc;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void testStoreQueryOk() {
        inboxGCProc = new InboxStoreGCProcessor(storeClient, localStoreId);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, remoteRangeSetting);
        }});
        when(storeClient.query(anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeROReply.newBuilder().setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder().build())
                        .build())
                    .build())
                .build()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.OK);
    }

    @Test
    public void testStoreQueryException() {
        inboxGCProc = new InboxStoreGCProcessor(storeClient, localStoreId);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, localRangeSetting);
        }});

        when(storeClient.query(anyString(), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }

    @Test
    public void testStoreQueryFailed() {
        inboxGCProc = new InboxStoreGCProcessor(storeClient, localStoreId);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, localRangeSetting);
        }});
        when(storeClient.query(anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeROReply.newBuilder().setCode(ReplyCode.InternalError).build()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }

    @Test
    public void testGCScanFailed() {
        inboxGCProc = new InboxStoreGCProcessor(storeClient, localStoreId);
        when(storeClient.latestEffectiveRouter()).thenReturn(new TreeMap<>(BoundaryUtil::compare) {{
            put(FULL_BOUNDARY, localRangeSetting);
        }});

        when(storeClient.query(anyString(), any())).thenReturn(CompletableFuture.failedFuture(new TryLaterException()));
        IInboxStoreGCProcessor.Result result = inboxGCProc.gc(System.nanoTime(), HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }
}

