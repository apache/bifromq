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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import com.google.protobuf.ByteString;
import java.lang.reflect.Method;
import java.util.Optional;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KVReaderTest extends MockableTest {
    @Mock
    private IKVRangeReader kvRangeReader;
    @Mock
    private IKVSpaceReader keyRangeReader;
    @Mock
    private IKVSpaceIterator keyRangeIterator;
    @Mock
    private IKVReader dataReader;

    @Override
    protected void doSetup(Method method) {
        when(kvRangeReader.newDataReader()).thenReturn(dataReader);
        when(keyRangeReader.newIterator()).thenReturn(keyRangeIterator);
    }

    @Test
    public void read() {
        IKVCloseableReader reader = new KVReader(keyRangeReader, kvRangeReader);
        // range
        when(kvRangeReader.boundary()).thenReturn(FULL_BOUNDARY);
        reader.boundary();

        // size
        Boundary range = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        reader.size(range);
        verify(kvRangeReader).size(range);

        // exist
        when(dataReader.exist(any())).thenReturn(false);
        ByteString existKey1 = ByteString.copyFromUtf8("existKey1");
        assertFalse(reader.exist(existKey1));

        when(keyRangeReader.exist(any())).thenReturn(true);
        ByteString existKey2 = ByteString.copyFromUtf8("existKey2");
        assertTrue(reader.exist(existKey2));

        // get
        when(keyRangeReader.get(any())).thenReturn(Optional.empty());
        ByteString getKey1 = ByteString.copyFromUtf8("getKey1");
        assertFalse(reader.get(getKey1).isPresent());

        when(keyRangeReader.get(any())).thenReturn(Optional.of(ByteString.copyFromUtf8("value")));
        ByteString getKey2 = ByteString.copyFromUtf8("getKey2");
        assertTrue(reader.get(getKey2).isPresent());
    }

    @Test
    public void close() {
        IKVCloseableReader reader = new KVReader(keyRangeReader, kvRangeReader);
        reader.close();
        verify(keyRangeIterator, never()).close();
    }

    @Test
    public void closeItr() {
        IKVCloseableReader reader = new KVReader(keyRangeReader, kvRangeReader);
        IKVIterator iterator = reader.iterator();
        reader.close();
        verify(keyRangeIterator).close();
    }
}
