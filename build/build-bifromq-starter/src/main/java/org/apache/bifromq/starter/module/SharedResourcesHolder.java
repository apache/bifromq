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

package org.apache.bifromq.starter.module;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class SharedResourcesHolder {
    private final Stack<Object> closeableResources = new Stack<>();

    public <T> T add(T resource) {
        assert closeableResources instanceof AutoCloseable || closeableResources instanceof ExecutorService;
        closeableResources.add(resource);
        return resource;
    }

    public void close() {
        while (!closeableResources.isEmpty()) {
            Object resource = closeableResources.pop();
            if (resource instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) resource).close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (resource instanceof EventExecutorGroup) {
                ((EventExecutorGroup) resource).shutdownGracefully();
            }
            if (resource instanceof ExecutorService) {
                MoreExecutors.shutdownAndAwaitTermination((ExecutorService) resource, 5, TimeUnit.SECONDS);
            }
        }
    }
}
