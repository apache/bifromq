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

package org.apache.bifromq.dist.worker.cache;

import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.function.Supplier;

class ThreadLocalKVReader implements Supplier<IKVReader> {
    private final Set<IKVCloseableReader> threadReaders = Sets.newConcurrentHashSet();
    private final ThreadLocal<IKVReader> threadLocalReader;

    public ThreadLocalKVReader(Supplier<IKVCloseableReader> readerProvider) {
        this.threadLocalReader = ThreadLocal.withInitial(() -> {
            IKVCloseableReader reader = readerProvider.get();
            threadReaders.add(reader);
            return reader;
        });
    }

    @Override
    public IKVReader get() {
        return threadLocalReader.get();
    }

    public void close() {
        threadReaders.forEach(IKVCloseableReader::close);
    }
}
