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


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ThreadLocalKVReaderTest {

    private Supplier<IKVCloseableReader> readerSupplierMock;
    private IKVCloseableReader readerMock1;
    private IKVCloseableReader readerMock2;
    private ThreadLocalKVReader threadLocalKVReader;

    @BeforeMethod
    void setUp() {
        readerSupplierMock = mock(Supplier.class);
        readerMock1 = mock(IKVCloseableReader.class);
        readerMock2 = mock(IKVCloseableReader.class);

        // 模拟 readerSupplier 返回不同的 reader 实例
        when(readerSupplierMock.get())
            .thenReturn(readerMock1)
            .thenReturn(readerMock2);

        threadLocalKVReader = new ThreadLocalKVReader(readerSupplierMock);
    }

    @Test
    void singleThreadGet() {
        IKVReader reader = threadLocalKVReader.get();
        assertSame(readerMock1, reader);

        IKVReader readerAgain = threadLocalKVReader.get();
        assertSame(readerMock1, readerAgain);
    }

    @Test
    void multiThreadGet() throws InterruptedException {
        Set<IKVReader> readers = new HashSet<>();
        CountDownLatch latch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.execute(() -> {
            readers.add(threadLocalKVReader.get());
            latch.countDown();
        });

        executorService.execute(() -> {
            readers.add(threadLocalKVReader.get());
            latch.countDown();
        });

        latch.await();

        assertEquals(2, readers.size());
        assertTrue(readers.contains(readerMock1));
        assertTrue(readers.contains(readerMock2));

        executorService.shutdown();
    }

    @Test
    void close() {
        threadLocalKVReader.get();
        threadLocalKVReader.get();

        threadLocalKVReader.close();
        verify(readerMock1, times(1)).close();
        verify(readerMock2, times(0)).close();
    }

    @Test
    void closeMultipleReaders() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.execute(threadLocalKVReader::get);
        executorService.execute(threadLocalKVReader::get);

        executorService.shutdown();
        executorService.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);

        threadLocalKVReader.close();
        verify(readerMock1, times(1)).close();
        verify(readerMock2, times(1)).close();
    }
}