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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.IKVRangeStore;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class QueryPipelineTest extends MockableTest {
    @Mock
    private IKVRangeStore rangeStore;

    @Mock
    private ServerCallStreamObserver streamObserver;

    @Test
    public void get() {
        get(false);
        get(true);
    }

    private void get(boolean linearized) {
        QueryPipeline pipeline = new QueryPipeline(rangeStore, linearized, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString getKey = ByteString.copyFromUtf8("get");
        KVRangeRORequest getRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setGetKey(getKey)
            .build();

        when(rangeStore.get(1, rangeId, getKey, linearized))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        KVRangeROReply getReply = pipeline.handleRequest("_", getRequest).join();

        assertEquals(getReply.getReqId(), 1);
        assertEquals(getReply.getCode(), ReplyCode.Ok);
        assertFalse(getReply.getGetResult().hasValue());
    }

    @Test
    public void exist() {
        exist(false);
        exist(true);
    }

    private void exist(boolean linearized) {
        QueryPipeline pipeline = new QueryPipeline(rangeStore, linearized, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString existKey = ByteString.copyFromUtf8("exist");
        KVRangeRORequest existRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setExistKey(existKey)
            .build();

        when(rangeStore.exist(1, rangeId, existKey, linearized))
            .thenReturn(CompletableFuture.completedFuture(true));

        KVRangeROReply existReply = pipeline.handleRequest("_", existRequest).join();

        assertEquals(existReply.getReqId(), 1);
        assertEquals(existReply.getCode(), ReplyCode.Ok);
        assertTrue(existReply.getExistResult());
    }

    @Test
    public void queryCoProc() {
        queryCoProc(false);
        queryCoProc(true);
    }

    private void queryCoProc(boolean linearized) {
        QueryPipeline pipeline = new QueryPipeline(rangeStore, linearized, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ROCoProcInput coProcInput = ROCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("coProc")).build();
        KVRangeRORequest coProcRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setRoCoProc(coProcInput)
            .build();

        when(rangeStore.queryCoProc(1, rangeId, coProcInput, linearized))
            .thenReturn(
                CompletableFuture.completedFuture(ROCoProcOutput.newBuilder().setRaw(ByteString.empty()).build()));

        KVRangeROReply coProcReply = pipeline.handleRequest("_", coProcRequest).join();

        assertEquals(coProcReply.getReqId(), 1);
        assertEquals(coProcReply.getCode(), ReplyCode.Ok);
        assertEquals(coProcReply.getRoCoProcResult().getRaw(), ByteString.empty());
    }


    @Test
    public void multiQueries() {
        multiQueries(false);
        multiQueries(true);
    }

    private void multiQueries(boolean linearized) {
        QueryPipeline pipeline = new QueryPipeline(rangeStore, linearized, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        int reqCount = 10;
        List<KVRangeRORequest> requests = new ArrayList<>();
        List<KVRangeROReply> replies = new ArrayList<>();
        List<CompletableFuture<KVRangeROReply>> replyFutures = new ArrayList<>();
        for (int i = 0; i < reqCount; i++) {
            ByteString getKey = ByteString.copyFromUtf8("get-" + i);
            KVRangeRORequest getRequest = KVRangeRORequest.newBuilder()
                .setReqId(i)
                .setVer(1)
                .setKvRangeId(rangeId)
                .setGetKey(getKey)
                .build();

            when(rangeStore.get(1, rangeId, getKey, linearized))
                .thenReturn(new CompletableFuture<Optional<ByteString>>()
                    .completeOnTimeout(Optional.empty(), ThreadLocalRandom.current().nextInt(0, 100),
                        TimeUnit.MILLISECONDS));
            requests.add(getRequest);
            replyFutures.add(pipeline.handleRequest("_", getRequest)
                .whenComplete((v, e) -> replies.add(v)));
        }
        CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[] {})).join();
        assertEquals(replies.size(), requests.size());
        for (int i = 0; i < reqCount; i++) {
            assertEquals(replies.get(i).getReqId(), requests.get(i).getReqId());
        }
    }

    @Test
    public void errorCodeConversion() {
        QueryPipeline pipeline = new QueryPipeline(rangeStore, false, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString getKey = ByteString.copyFromUtf8("get");

        // bad version
        KVRangeRORequest getRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setGetKey(getKey)
            .build();
        when(rangeStore.get(1, rangeId, getKey, false))
            .thenReturn(CompletableFuture.failedFuture(new KVRangeException.BadVersion("bad version")));
        KVRangeROReply getReply = pipeline.handleRequest("_", getRequest).join();
        assertEquals(getReply.getCode(), ReplyCode.BadVersion);

        // bad request
        getRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(2)
            .setKvRangeId(rangeId)
            .setGetKey(getKey)
            .build();
        when(rangeStore.get(2, rangeId, getKey, false))
            .thenReturn(CompletableFuture.failedFuture(new KVRangeException.BadRequest("bad request")));
        getReply = pipeline.handleRequest("_", getRequest).join();
        assertEquals(getReply.getCode(), ReplyCode.BadRequest);

        // try later
        getRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(3)
            .setKvRangeId(rangeId)
            .setGetKey(getKey)
            .build();
        when(rangeStore.get(3, rangeId, getKey, false))
            .thenReturn(CompletableFuture.failedFuture(new KVRangeException.TryLater("try later")));
        getReply = pipeline.handleRequest("_", getRequest).join();
        assertEquals(getReply.getCode(), ReplyCode.TryLater);

        // internal error
        getRequest = KVRangeRORequest.newBuilder()
            .setReqId(1)
            .setVer(4)
            .setKvRangeId(rangeId)
            .setGetKey(getKey)
            .build();
        when(rangeStore.get(4, rangeId, getKey, false))
            .thenReturn(CompletableFuture.failedFuture(new KVRangeException.InternalException("internal error")));
        getReply = pipeline.handleRequest("_", getRequest).join();
        assertEquals(getReply.getCode(), ReplyCode.InternalError);
    }
}
