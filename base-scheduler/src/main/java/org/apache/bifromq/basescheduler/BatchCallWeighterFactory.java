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

package org.apache.bifromq.basescheduler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basescheduler.spi.IBatchCallWeighter;
import org.apache.bifromq.basescheduler.spi.IBatchCallWeighterFactory;

@Slf4j
class BatchCallWeighterFactory implements IBatchCallWeighterFactory {
    public static final IBatchCallWeighterFactory INSTANCE = new BatchCallWeighterFactory();

    private final IBatchCallWeighterFactory delegate;

    private BatchCallWeighterFactory() {
        Map<String, IBatchCallWeighterFactory> factoryMap = BaseHookLoader.load(IBatchCallWeighterFactory.class);
        if (factoryMap.isEmpty()) {
            delegate = FallbackFactory.INSTANCE;
        } else {
            delegate = factoryMap.values().iterator().next();
            if (factoryMap.size() > 1) {
                log.warn("Multiple BatchCallWeigher implementations found, the first loaded will be used:{}",
                    delegate.getClass().getName());
            }
        }
    }

    @Override
    public <ReqT> IBatchCallWeighter<ReqT> create(String name, Class<ReqT> reqType) {
        try {
            IBatchCallWeighter<ReqT> weighter = delegate.create(name, reqType);
            if (weighter == null) {
                return FallbackFactory.INSTANCE.create(name, reqType);
            }
            return weighter;
        } catch (Throwable e) {
            log.error("Failed to create BatchCallWeighter: scheduler={}", name, e);
            return FallbackFactory.INSTANCE.create(name, reqType);
        }
    }

    private static class FallbackWeighter<ReqT> implements IBatchCallWeighter<ReqT> {
        private int count = 0;

        @Override
        public void add(ReqT req) {
            count++;
        }

        @Override
        public long weight() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }

    private static class FallbackFactory implements IBatchCallWeighterFactory {
        private static final IBatchCallWeighterFactory INSTANCE = new FallbackFactory();

        @Override
        public <ReqT> IBatchCallWeighter<ReqT> create(String name, Class<ReqT> reqType) {
            return new FallbackWeighter<>();
        }
    }
}
