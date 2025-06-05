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

import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.disposables.Disposable;
import java.lang.reflect.Method;
import org.testng.annotations.Test;

public abstract class AbstractKVEngineTest extends MockableTest {
    protected IKVEngine<? extends IKVSpace> engine;

    @Override
    protected void doSetup(Method method) {
        beforeStart();
        engine = newEngine();
        engine.start();
    }

    protected void beforeStart() {

    }

    @Override
    protected void doTeardown(Method method) {
        engine.stop();
        afterStop();
    }

    protected void afterStop() {

    }

    protected abstract IKVEngine<? extends IKVSpace> newEngine();

    @Test
    public void createIfMissing() {
        String rangeId = "test_range1";
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        IKVSpace keyRange1 = engine.createIfMissing(rangeId);
        assertEquals(keyRange1, keyRange);
    }

    @Test
    public void size() {
        String rangeId = "test_range1";
        String rangeId1 = "test_range2";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertEquals(keyRange.size(), 0);
        keyRange.toWriter().put(key, value).done();
        assertTrue(keyRange.size() > 0);

        IKVSpace keyRange1 = engine.createIfMissing(rangeId1);
        assertEquals(keyRange1.size(), 0);
    }

    @Test
    public void metadata() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().metadata(key, value).done();
        assertTrue(keyRange.metadata(key).isPresent());
        assertEquals(keyRange.metadata(key).get(), value);
    }

    @Test
    public void describe() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        KVSpaceDescriptor descriptor = keyRange.describe();
        assertEquals(descriptor.id(), rangeId);
        assertEquals(descriptor.metrics().get("size"), 0);

        keyRange.toWriter().put(key, value).metadata(key, value).done();
        descriptor = keyRange.describe();
        assertTrue(descriptor.metrics().get("size") > 0);
    }

    @Test
    public void kvSpaceDestroy() {
        String rangeId = "test_range1";
        IKVSpace range = engine.createIfMissing(rangeId);
        assertTrue(engine.spaces().containsKey(rangeId));
        Disposable disposable = range.metadata().subscribe();
        range.destroy();
        assertTrue(disposable.isDisposed());
        assertTrue(engine.spaces().isEmpty());
        assertFalse(engine.spaces().containsKey(rangeId));
    }

    @Test
    public void kvSpaceDestroyAndCreate() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace range = engine.createIfMissing(rangeId);
        range.toWriter().put(key, value).done();
        assertTrue(range.exist(key));
        range.destroy();

        range = engine.createIfMissing(rangeId);
        assertFalse(range.exist(key));
        range.toWriter().put(key, value).done();
        assertTrue(range.exist(key));
    }


    @Test
    public void exist() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertFalse(keyRange.exist(key));

        IKVSpaceWriter rangeWriter = keyRange.toWriter().put(key, value);
        assertFalse(keyRange.exist(key));

        rangeWriter.done();
        assertTrue(keyRange.exist(key));
    }

    @Test
    public void get() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        assertFalse(keyRange.get(key).isPresent());

        IKVSpaceWriter rangeWriter = keyRange.toWriter().put(key, value);
        assertFalse(keyRange.get(key).isPresent());

        rangeWriter.done();
        assertTrue(keyRange.get(key).isPresent());
        assertEquals(keyRange.get(key).get(), value);
    }

    @Test
    public void iterator() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);

        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator()) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekToLast();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekForPrev(key);
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());
        }
    }

    @Test
    public void iterateSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);

        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator(Boundary.newBuilder()
            .setStartKey(key)
            .build())) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekToLast();
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());

            keyRangeIterator.seekForPrev(key);
            assertTrue(keyRangeIterator.isValid());
            assertEquals(keyRangeIterator.key(), key);
            assertEquals(keyRangeIterator.value(), value);
            keyRangeIterator.next();
            assertFalse(keyRangeIterator.isValid());
        }
        try (IKVSpaceIterator keyRangeIterator = keyRange.newIterator(Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("0"))
            .setEndKey(ByteString.copyFromUtf8("9"))
            .build())) {
            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());

            keyRange.toWriter().put(key, value).done();

            keyRangeIterator.refresh();

            keyRangeIterator.seekToFirst();
            assertFalse(keyRangeIterator.isValid());
        }
    }

    @Test
    public void writer() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter()
            .put(key, value)
            .delete(key)
            .metadata(key, value)
            .done();
        assertFalse(keyRange.exist(key));

        IKVSpaceWriter rangeWriter = keyRange.toWriter();
        assertEquals(rangeWriter.metadata(key).get(), value);
        rangeWriter.insert(key, value).done();
        assertTrue(keyRange.exist(key));

        keyRange.toWriter().clear().done();
        assertFalse(keyRange.exist(key));
    }

    @Test
    public void clearSubBoundary() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().put(key, value).done();

        keyRange.toWriter()
            .clear(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("0"))
                .setEndKey(ByteString.copyFromUtf8("9"))
                .build())
            .done();
        assertTrue(keyRange.exist(key));
    }

    @Test
    public void migrateTo() {
        String leftRangeId = "test_range1";
        String rightRangeId = "test_range2";
        ByteString key1 = ByteString.copyFromUtf8("1");
        ByteString value1 = ByteString.copyFromUtf8("1");
        ByteString key2 = ByteString.copyFromUtf8("6");
        ByteString value2 = ByteString.copyFromUtf8("6");
        ByteString splitKey = ByteString.copyFromUtf8("5");

        ByteString metaKey = ByteString.copyFromUtf8("metaKey");
        ByteString metaVal = ByteString.copyFromUtf8("metaVal");


        IKVSpace leftRange = engine.createIfMissing(leftRangeId);
        leftRange.toWriter()
            .put(key1, value1)
            .put(key2, value2)
            .done();
        IKVSpaceWriter leftSpaceWriter = leftRange.toWriter();
        IKVSpaceMetadataWriter rightSpaceMetadataWriter = leftSpaceWriter
            .migrateTo(rightRangeId, Boundary.newBuilder().setStartKey(splitKey).build()).metadata(metaKey, metaVal);
        leftSpaceWriter.done();
        rightSpaceMetadataWriter.done();

        IKVSpace rightRange = engine.createIfMissing(rightRangeId);

        assertFalse(leftRange.metadata(metaKey).isPresent());
        assertTrue(rightRange.metadata(metaKey).isPresent());
        assertEquals(rightRange.metadata(metaKey).get(), metaVal);
        assertFalse(leftRange.exist(key2));
        assertTrue(rightRange.exist(key2));
    }

    @Test
    public void migrateFrom() {
        String leftRangeId = "test_range1";
        String rightRangeId = "test_range2";
        ByteString key1 = ByteString.copyFromUtf8("1");
        ByteString value1 = ByteString.copyFromUtf8("1");
        ByteString key2 = ByteString.copyFromUtf8("6");
        ByteString value2 = ByteString.copyFromUtf8("6");
        ByteString splitKey = ByteString.copyFromUtf8("5");

        ByteString metaKey = ByteString.copyFromUtf8("metaKey");
        ByteString metaVal = ByteString.copyFromUtf8("metaVal");


        IKVSpace leftRange = engine.createIfMissing(leftRangeId);
        leftRange.toWriter()
            .put(key1, value1)
            .done();
        assertFalse(leftRange.exist(key2));
        IKVSpace rightRange = engine.createIfMissing(rightRangeId);
        rightRange.toWriter()
            .put(key2, value2)
            .done();
        assertTrue(rightRange.exist(key2));

        IKVSpaceWriter leftSpaceWriter = leftRange.toWriter();
        IKVSpaceMetadataWriter rightSpaceMetadataWriter = leftSpaceWriter
            .migrateFrom(rightRangeId, Boundary.newBuilder().setStartKey(splitKey).build())
            .metadata(metaKey, metaVal);
        leftSpaceWriter
            .metadata(metaKey, metaVal)
            .done();
        rightSpaceMetadataWriter.done();

        assertTrue(leftRange.metadata(metaKey).isPresent());
        assertTrue(rightRange.metadata(metaKey).isPresent());

        assertEquals(rightRange.metadata(metaKey).get(), metaVal);
        assertEquals(rightRange.metadata(metaKey).get(), metaVal);

        assertTrue(leftRange.exist(key2));
        assertFalse(rightRange.exist(key2));
    }
}
