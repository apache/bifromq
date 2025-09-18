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

package org.apache.bifromq.dist.worker.schema.cache;

import static org.apache.bifromq.util.TopicConst.NUL;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Interner;
import org.apache.bifromq.dist.worker.schema.Receiver;

public class ReceiverCache {
    private static final Interner<String> RECEIVER_URL_INTERNER = Interner.newWeakInterner();
    private static final Interner<Receiver> RECEIVER_INTERNER = Interner.newWeakInterner();
    private static final Cache<String, Receiver> RECEIVER_CACHE = Caffeine.newBuilder().weakKeys().build();

    public static Receiver get(String receiverUrl) {
        receiverUrl = RECEIVER_URL_INTERNER.intern(receiverUrl);
        return RECEIVER_CACHE.get(receiverUrl, k -> {
            String[] parts = k.split(NUL);
            return RECEIVER_INTERNER.intern(new Receiver(Integer.parseInt(parts[0]), parts[1], parts[2]));
        });
    }
}
