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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.lang.reflect.Method;
import org.apache.bifromq.basekv.proto.Boundary;
import org.testng.annotations.Test;

public abstract class AbstractKVEngineTest<T extends IKVSpace> extends MockableTest {
    // Use covariant engine type to accept concrete space subtypes
    protected IKVEngine<? extends T> engine;

    @Override
    protected void doSetup(Method method) {
        beforeStart();
        engine = newEngine();
        engine.start();
    }

    protected void beforeStart() {
        // no-op
    }

    @Override
    protected void doTeardown(Method method) {
        engine.stop();
        afterStop();
    }

    protected void afterStop() {
        // no-op
    }

    protected abstract IKVEngine<? extends T> newEngine();

    // Subclass must provide writer for specific space subtype
    protected abstract IKVSpaceWriter writerOf(T space);

    // Common behavior tests below work for any IKVSpace implementation

    @Test
    public void createIfMissing() {
        String rangeId = "test_range1";
        IKVSpace space = engine.createIfMissing(rangeId);
        IKVSpace space1 = engine.createIfMissing(rangeId);
        assertEquals(space1, space);
    }

    @Test
    public void size() {
        String rangeId = "test_range1";
        String rangeId1 = "test_range2";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        assertEquals(space.size(), 0);
        writerOf((T) space).put(key, value).done();
        assertTrue(space.size() > 0);

        IKVSpace space1 = engine.createIfMissing(rangeId1);
        assertEquals(space1.size(), 0);
    }

    @Test
    public void metadata() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        writerOf((T) space).metadata(key, value).done();
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertTrue(reader.metadata(key).isPresent());
            assertEquals(reader.metadata(key).get(), value);
        }
    }

    @Test
    public void describe() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        KVSpaceDescriptor descriptor = space.describe();
        assertEquals(descriptor.id(), rangeId);
        assertEquals(descriptor.metrics().get("size"), 0);

        writerOf((T) space).put(key, value).metadata(key, value).done();
        descriptor = space.describe();
        assertTrue(descriptor.metrics().get("size") > 0);
    }

    @Test
    public void kvSpaceDestroy() {
        String rangeId = "test_range1";
        IKVSpace space = engine.createIfMissing(rangeId);
        assertTrue(engine.spaces().containsKey(rangeId));
        var disposable = space.metadata().subscribe();
        space.destroy();
        assertTrue(disposable.isDisposed());
        assertTrue(engine.spaces().isEmpty());
        assertFalse(engine.spaces().containsKey(rangeId));
    }

    @Test
    public void kvSpaceDestroyAndCreate() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        writerOf((T) space).put(key, value).done();
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertTrue(reader.exist(key));
        }
        space.destroy();
        space = engine.createIfMissing(rangeId);
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertFalse(reader.exist(key));
            writerOf((T) space).put(key, value).done();
            reader.refresh();
            assertTrue(reader.exist(key));
        }
    }

    @Test
    public void exist() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertFalse(reader.exist(key));

            IKVSpaceWriter writer = writerOf((T) space).put(key, value);
            reader.refresh();
            assertFalse(reader.exist(key));

            writer.done();
            reader.refresh();
            assertTrue(reader.exist(key));
        }
    }

    @Test
    public void get() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertFalse(reader.get(key).isPresent());

            IKVSpaceWriter writer = writerOf((T) space).put(key, value);
            reader.refresh();
            assertFalse(reader.get(key).isPresent());

            writer.done();
            reader.refresh();
            assertTrue(reader.get(key).isPresent());
            assertEquals(reader.get(key).get(), value);
        }
    }

    @Test
    public void iterator() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);

        try (IKVSpaceRefreshableReader reader = space.reader()) {
            IKVSpaceIterator itr = reader.newIterator();
            itr.seekToFirst();
            assertFalse(itr.isValid());
            writerOf((T) space).put(key, value).done();

            itr.seekToFirst();
            assertFalse(itr.isValid());
            reader.refresh();

            itr.seekToFirst();
            assertTrue(itr.isValid());
            assertEquals(itr.key(), key);
            assertEquals(itr.value(), value);
            itr.next();
            assertFalse(itr.isValid());

            itr.seekToLast();
            assertTrue(itr.isValid());
            assertEquals(itr.key(), key);
            assertEquals(itr.value(), value);
            itr.next();
            assertFalse(itr.isValid());

            itr.seekForPrev(key);
            assertTrue(itr.isValid());
            assertEquals(itr.key(), key);
            assertEquals(itr.value(), value);
            itr.next();
            assertFalse(itr.isValid());
        }
    }

    @Test
    public void iterateSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);

        try (IKVSpaceRefreshableReader reader = space.reader()) {
            IKVSpaceIterator itr = reader.newIterator(Boundary.newBuilder().setStartKey(key).build());
            itr.seekToFirst();
            assertFalse(itr.isValid());
            writerOf((T) space).put(key, value).done();

            itr.seekToFirst();
            assertFalse(itr.isValid());
            reader.refresh();

            itr.seekToFirst();
            assertTrue(itr.isValid());
            assertEquals(itr.key(), key);
            assertEquals(itr.value(), value);
            itr.next();
            assertFalse(itr.isValid());
        }
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            IKVSpaceIterator itr = reader.newIterator(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("0"))
                .setEndKey(ByteString.copyFromUtf8("9")).build());
            itr.seekToFirst();
            assertFalse(itr.isValid());
            writerOf((T) space).put(key, value).done();
            reader.refresh();
            itr.seekToFirst();
            assertFalse(itr.isValid());
        }
    }

    @Test
    public void writer() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        writerOf((T) space).put(key, value).delete(key).metadata(key, value).done();
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertFalse(reader.exist(key));

            IKVSpaceWriter writer = writerOf((T) space);
            assertEquals(reader.metadata(key).get(), value);
            writer.insert(key, value).done();
            reader.refresh();
            assertTrue(reader.exist(key));

            writerOf((T) space).clear().done();
            reader.refresh();
            assertFalse(reader.exist(key));
        }
    }

    @Test
    public void clearSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace space = engine.createIfMissing(rangeId);
        writerOf((T) space).put(key, value).done();

        writerOf((T) space).clear(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("0"))
                .setEndKey(ByteString.copyFromUtf8("9")).build())
            .done();
        try (IKVSpaceRefreshableReader reader = space.reader()) {
            assertTrue(reader.exist(key));
        }
    }
}
