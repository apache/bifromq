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

package org.apache.bifromq.baserpc.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertTrue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class BiDiStreamTest {

    @Test
    public void lateErrorAfterCancelDoesNotReachGlobalHandler() {
        assertLateErrorIgnored(true);
    }

    @Test
    public void lateErrorAfterCloseDoesNotReachGlobalHandler() {
        assertLateErrorIgnored(false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void assertLateErrorIgnored(boolean cancel) {
        List<Throwable> undeliverable = new ArrayList<>();
        RxJavaPlugins.setErrorHandler(undeliverable::add);
        AtomicReference<ClientResponseObserver<Object, Object>> responseObserver = new AtomicReference<>();
        ClientCallStreamObserver<Object> requestObserver = mock(ClientCallStreamObserver.class);
        try (MockedStatic<ClientCalls> mocked = mockStatic(ClientCalls.class, invocation -> {
            if ("asyncBidiStreamingCall".equals(invocation.getMethod().getName())) {
                responseObserver.set(invocation.getArgument(1));
                return requestObserver;
            }
            return invocation.callRealMethod();
        })) {
            BiDiStream<Object, Object> stream = new BiDiStream<>("tenant", "server",
                mock(Channel.class), mock(MethodDescriptor.class), Map.of(), CallOptions.DEFAULT);
            if (cancel) {
                stream.cancel("test");
                // The first error terminates the subject; a repeated error must not go global.
                responseObserver.get().onError(Status.UNAVAILABLE.asRuntimeException());
            } else {
                stream.close();
                // The server completes the stream; a late error must not go global.
                responseObserver.get().onCompleted();
            }
            responseObserver.get().onError(Status.UNAVAILABLE.asRuntimeException());

            assertTrue(undeliverable.isEmpty(),
                "late onError must not become an RxJava UndeliverableException: " + undeliverable);
        } finally {
            RxJavaPlugins.setErrorHandler(null);
        }
    }
}
