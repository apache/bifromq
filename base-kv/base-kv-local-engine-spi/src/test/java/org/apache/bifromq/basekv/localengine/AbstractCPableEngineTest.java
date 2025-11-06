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
import org.apache.bifromq.basekv.proto.Boundary;
import org.testng.annotations.Test;

// CPable-specific tests; common behaviors are in AbstractKVEngineTest
public abstract class AbstractCPableEngineTest extends AbstractKVEngineTest<ICPableKVSpace> {
    @Override
    protected IKVSpaceWriter writerOf(ICPableKVSpace space) {
        // Restorable writer is-a IKVSpaceWriter for common operations
        return space.toWriter();
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

        ICPableKVSpace leftRange = engine.createIfMissing(leftRangeId);
        leftRange.toWriter()
            .put(key1, value1)
            .put(key2, value2)
            .done();
        IKVSpaceMigratableWriter leftSpaceWriter = leftRange.toWriter();
        IRestoreSession rightSpaceRestoreSession = leftSpaceWriter
            .migrateTo(rightRangeId, Boundary.newBuilder().setStartKey(splitKey).build())
            .metadata(metaKey, metaVal);
        leftSpaceWriter.done();
        rightSpaceRestoreSession.done();

        IKVSpace rightRange = engine.createIfMissing(rightRangeId);

        try (IKVSpaceRefreshableReader leftReader = leftRange.reader();
             IKVSpaceRefreshableReader rightReader = rightRange.reader()) {
            assertFalse(leftReader.metadata(metaKey).isPresent());
            assertTrue(rightReader.metadata(metaKey).isPresent());
            assertEquals(rightReader.metadata(metaKey).get(), metaVal);
            assertFalse(leftReader.exist(key2));
            assertTrue(rightReader.exist(key2));
        }
    }
}
