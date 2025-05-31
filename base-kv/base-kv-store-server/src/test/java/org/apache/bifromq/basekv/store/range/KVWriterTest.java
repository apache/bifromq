/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.store.range;

import static org.mockito.Mockito.verify;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KVWriterTest extends MockableTest {
    @Mock
    private IKVSpaceWriter keyRangeWriter;

    @Test
    public void write() {
        IKVWriter writer = new KVWriter(keyRangeWriter);

        // delete
        ByteString delKey = ByteString.copyFromUtf8("delKey");
        writer.delete(delKey);
        verify(keyRangeWriter).delete(delKey);

        // insert
        ByteString insKey = ByteString.copyFromUtf8("insertKey");
        ByteString insValue = ByteString.copyFromUtf8("insertValue");
        writer.insert(insKey, insValue);
        verify(keyRangeWriter).insert(insKey, insValue);

        // put
        ByteString putKey = ByteString.copyFromUtf8("putKey");
        ByteString putValue = ByteString.copyFromUtf8("putValue");
        writer.put(putKey, putValue);
        verify(keyRangeWriter).put(putKey, putValue);

        // delete range
        Boundary delRange = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setStartKey(ByteString.copyFromUtf8("z"))
            .build();
        writer.clear(delRange);
        verify(keyRangeWriter).clear(delRange);
    }
}
