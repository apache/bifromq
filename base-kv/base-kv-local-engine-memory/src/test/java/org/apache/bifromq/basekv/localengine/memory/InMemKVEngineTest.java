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

package org.apache.bifromq.basekv.localengine.memory;

import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.localengine.AbstractKVEngineTest;
import org.apache.bifromq.basekv.localengine.ICPableKVSpace;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IRestoreSession;
import org.testng.annotations.Test;

public class InMemKVEngineTest extends AbstractKVEngineTest<ICPableKVSpace> {
    @Override
    protected IKVEngine<? extends ICPableKVSpace> newEngine() {
        return new InMemCPableKVEngine(null, InMemDefaultConfigs.CP);
    }

    @Override
    protected IKVSpaceWriter writerOf(ICPableKVSpace space) {
        // InMem CPable space returns a restorable writer which is also an IKVSpaceWriter
        return space.toWriter();
    }

    @Test
    public void receivingPublishesDataAndMetadataOnlyAfterCommit() {
        ICPableKVSpace space = engine.createIfMissing("receiving");
        ByteString key = ByteString.copyFromUtf8("key");
        ByteString oldValue = ByteString.copyFromUtf8("old");
        ByteString newValue = ByteString.copyFromUtf8("new");
        space.toWriter().put(key, oldValue).metadata(key, oldValue).done();

        try (IKVSpaceRefreshableReader reader = space.reader()) {
            IRestoreSession receiving = space.startReceiving(null);
            receiving.put(key, newValue).metadata(key, newValue);
            assertEquals(reader.get(key).orElseThrow(), oldValue);
            assertEquals(reader.metadata(key).orElseThrow(), oldValue);
            receiving.abort();
            reader.refresh();
            assertEquals(reader.get(key).orElseThrow(), oldValue);
            assertEquals(reader.metadata(key).orElseThrow(), oldValue);

            receiving = space.startReceiving(null);
            receiving.put(key, newValue).metadata(key, newValue);
            receiving.done();
            reader.refresh();
            assertEquals(reader.get(key).orElseThrow(), newValue);
            assertEquals(reader.metadata(key).orElseThrow(), newValue);
        }
    }
}
