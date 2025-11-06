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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.AbstractWALableEngineTest;
import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.TestUtil;
import org.testng.annotations.Test;

public abstract class AbstractRocksDBWALableEngineTest extends AbstractWALableEngineTest {
    protected Path dbRootDir;

    @SneakyThrows
    @Override
    protected void beforeStart() {
        dbRootDir = Files.createTempDirectory("");
    }


    @Override
    protected void afterStop() {
        TestUtil.deleteDir(dbRootDir.toString());
    }

    @Test
    public void identityKeptSame() {
        String identity = engine.id();
        engine.stop();
        engine = newEngine();
        engine.start();
        assertEquals(identity, engine.id());
    }

    @Test
    public void loadExistingKeyRange() {
        String rangeId = "test_range1";
        ByteString metaKey = ByteString.copyFromUtf8("metaKey");
        ByteString metaValue = ByteString.copyFromUtf8("metaValue");
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IWALableKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().put(key, value).metadata(metaKey, metaValue).done();
        try (IKVSpaceReader reader = keyRange.reader()) { // use reader for read APIs
            assertTrue(reader.metadata(metaKey).isPresent());
            assertTrue(keyRange.metadata().blockingFirst().containsKey(metaKey));
            assertTrue(reader.exist(key));
            assertEquals(reader.get(key).get(), value);
        }
        engine.stop();

        engine = newEngine();
        engine.start();
        assertEquals(engine.spaces().size(), 1);
        IKVSpace keyRangeLoaded = engine.spaces().values().stream().findFirst().get();
        assertEquals(keyRangeLoaded.id(), rangeId);
        try (IKVSpaceReader reader = keyRangeLoaded.reader()) {
            assertTrue(reader.metadata(metaKey).isPresent());
            assertTrue(keyRangeLoaded.metadata().blockingFirst().containsKey(metaKey));
            assertTrue(reader.exist(key));
            assertEquals(reader.get(key).get(), value);
        }
        // stop again and start
        engine.stop();

        engine = newEngine();
        engine.start();
        assertEquals(engine.spaces().size(), 1);
        keyRangeLoaded = engine.spaces().values().stream().findFirst().get();
        assertEquals(keyRangeLoaded.id(), rangeId);
        try (IKVSpaceReader reader = keyRangeLoaded.reader()) {
            assertTrue(reader.metadata(metaKey).isPresent());
            assertTrue(keyRangeLoaded.metadata().blockingFirst().containsKey(metaKey));
            assertTrue(reader.exist(key));
            assertEquals(reader.get(key).get(), value);
        }
    }

    @Test
    public void flushOnClose() {
        String rangeId = "test_range1";
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString value = ByteString.copyFromUtf8("value");
        IWALableKVSpace keyRange = engine.createIfMissing(rangeId);
        keyRange.toWriter().put(key, value).done();
        engine.stop();
        engine = newEngine();
        engine.start();
        keyRange = engine.createIfMissing(rangeId);
        try (IKVSpaceReader reader = keyRange.reader()) {
            assertTrue(reader.exist(key));
        }
    }
}
