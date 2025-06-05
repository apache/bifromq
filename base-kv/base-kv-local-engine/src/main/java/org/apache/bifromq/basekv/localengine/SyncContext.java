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

package org.apache.bifromq.basekv.localengine;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class SyncContext implements ISyncContext {
    private final AtomicLong stateModVer = new AtomicLong();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private class Refresher implements IRefresher {
        private final Lock rLock;
        private long readVer = -1;

        Refresher() {
            this.rLock = rwLock.readLock();
        }

        @Override
        public void runIfNeeded(Runnable refresh) {
            rLock.lock();
            try {
                if (readVer == stateModVer.get()) {
                    return;
                }
                readVer = stateModVer.get();
                refresh.run();
            } finally {
                rLock.unlock();
            }
        }

        @Override
        public <T> T call(Supplier<T> supplier) {
            rLock.lock();
            try {
                return supplier.get();
            } finally {
                rLock.unlock();
            }
        }
    }

    private class Mutator implements IMutator {
        private final Lock wLock;

        Mutator() {
            this.wLock = rwLock.writeLock();
        }

        @Override
        public void run(Runnable mutation) {
            wLock.lock();
            try {
                mutation.run();
                stateModVer.incrementAndGet();
            } finally {
                wLock.unlock();
            }
        }
    }

    @Override
    public IRefresher refresher() {
        return new Refresher();
    }

    @Override
    public IMutator mutator() {
        return new Mutator();
    }
}
