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

package org.apache.bifromq.mqtt.session;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.PubAction;
import org.apache.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.EventExecutor;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTSessionAuthProviderTest extends MockableTest {
    @Mock
    private IAuthProvider delegate;
    @Mock
    private ChannelHandlerContext context;
    private EventExecutor contextExecutor;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        contextExecutor = new DefaultEventLoop(Executors.newSingleThreadExecutor());
        when(context.executor()).thenReturn(contextExecutor);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        contextExecutor.shutdownGracefully();
    }


    @Test
    public void checkPermissionFIFOSemantic() {
        MQTTSessionAuthProvider authProvider = new MQTTSessionAuthProvider(delegate, context);
        when(delegate.checkPermission(any(), any())).thenAnswer(new Answer<CompletableFuture<CheckResult>>() {
            @Override
            public CompletableFuture<CheckResult> answer(InvocationOnMock invocation) {
                MQTTAction action = (MQTTAction) invocation.getArguments()[1];
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        long sleep = ThreadLocalRandom.current().nextInt(1, 100);
                        log.debug("Sleep {} for action {}", sleep, action.getPub().getTopic());
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    log.debug("Check {}", action.getPub().getTopic());
                    return CheckResult.newBuilder().setGranted(Granted.getDefaultInstance()).build();
                });
            }
        });
        LinkedList<Integer> expected = new LinkedList<>();
        Set<CompletableFuture<CheckResult>> checkFutures = ConcurrentHashMap.newKeySet();

        CompletableFuture<Void> onDone = new CompletableFuture<>();
        context.executor().execute(() -> {
            for (int i = 0; i < 100; i++) {
                final int n = i;
                checkFutures.add(
                    authProvider.checkPermission(ClientInfo.newBuilder().build(), MQTTAction.newBuilder()
                            .setPub(PubAction.newBuilder().setTopic("" + n).build())
                            .build())
                        .whenComplete((result, throwable) -> {
                            assert context.executor().inEventLoop();
                            expected.add(n);
                        }));
            }
            onDone.complete(null);
        });
        onDone.join();

        CompletableFuture.allOf(checkFutures.toArray(CompletableFuture[]::new)).join();
        // assert expected contains 0 - 99
        for (int i = 0; i < 100; i++) {
            assertEquals(expected.get(i), i);
        }
    }
}
