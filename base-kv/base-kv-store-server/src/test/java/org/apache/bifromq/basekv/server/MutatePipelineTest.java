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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.Put;
import org.apache.bifromq.basekv.store.IKVRangeStore;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class MutatePipelineTest extends MockableTest {
    @Mock
    private IKVRangeStore rangeStore;
    @Mock
    private ServerCallStreamObserver streamObserver;

    @Test
    public void put() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString putKey = ByteString.copyFromUtf8("put");
        ByteString putVal = ByteString.copyFromUtf8("val");
        Put put = Put.newBuilder()
            .setKey(putKey)
            .setValue(putVal)
            .build();
        KVRangeRWRequest putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();

        when(rangeStore.put(1, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.completedFuture(ByteString.empty()));

        KVRangeRWReply putReply = pipeline.handleRequest("_", putRequest).join();

        assertEquals(putReply.getReqId(), 1);
        assertEquals(putReply.getCode(), ReplyCode.Ok);
        assertTrue(putReply.getPutResult().isEmpty());
    }

    @Test
    public void delete() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString delKey = ByteString.copyFromUtf8("del");
        KVRangeRWRequest delRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setDelete(delKey)
            .build();

        when(rangeStore.delete(1, rangeId, delKey)).thenReturn(CompletableFuture.completedFuture(ByteString.empty()));

        KVRangeRWReply delReply = pipeline.handleRequest("_", delRequest).join();

        assertEquals(delReply.getReqId(), 1);
        assertEquals(delReply.getCode(), ReplyCode.Ok);
        assertTrue(delReply.getDeleteResult().isEmpty());
    }

    @Test
    public void mutateCoProc() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        RWCoProcInput mutateCoProcInput = RWCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("mutate")).build();
        KVRangeRWRequest mutateRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setRwCoProc(mutateCoProcInput)
            .build();

        when(rangeStore.mutateCoProc(1, rangeId, mutateCoProcInput)).thenReturn(
            CompletableFuture.completedFuture(RWCoProcOutput.newBuilder().setRaw(ByteString.empty()).build()));

        KVRangeRWReply mutateReply = pipeline.handleRequest("_", mutateRequest).join();

        assertEquals(mutateReply.getReqId(), 1);
        assertEquals(mutateReply.getCode(), ReplyCode.Ok);
        assertTrue(mutateReply.getRwCoProcResult().getRaw().isEmpty());
    }

    @Test
    public void errorCodeConversion() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString putKey = ByteString.copyFromUtf8("put");
        ByteString putVal = ByteString.copyFromUtf8("val");
        Put put = Put.newBuilder()
            .setKey(putKey)
            .setValue(putVal)
            .build();
        // bad version
        KVRangeRWRequest putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(1, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.BadVersion("bad version")));
        KVRangeRWReply putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(putReply.getCode(), ReplyCode.BadVersion);

        // bad request
        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(2)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(2, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.BadRequest("bad request")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(putReply.getCode(), ReplyCode.BadRequest);

        // try later
        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(3)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(3, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.TryLater("try later")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(putReply.getCode(), ReplyCode.TryLater);

        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(4)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(4, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.InternalException("internal error")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(putReply.getCode(), ReplyCode.InternalError);
    }
}
