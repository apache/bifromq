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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KVRangeIteratorTest extends MockableTest {
    @Mock
    private IKVSpaceIterator rangeIterator;

    @Test
    public void key() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(rangeIterator.isValid()).thenReturn(true);
        when(rangeIterator.key()).thenReturn(userKey);
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seekToFirst();
        assertEquals(itr.key(), userKey);
    }

    @Test
    public void value() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(rangeIterator.isValid()).thenReturn(true);
        when(rangeIterator.key()).thenReturn(userKey);
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seekToFirst();
        itr.value();
        verify(rangeIterator).value();
    }

    @Test
    public void isValid() {
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.isValid();
        verify(rangeIterator).isValid();
    }

    @Test
    public void next() {
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.next();
        verify(rangeIterator).next();
    }

    @Test
    public void prev() {
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.prev();
        verify(rangeIterator).prev();
    }

    @Test
    public void seekToFirst() {
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seekToFirst();
        verify(rangeIterator).seekToFirst();
    }

    @Test
    public void seekToLast() {
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seekToLast();
        verify(rangeIterator).seekToLast();
    }

    @Test
    public void seek() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seek(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(rangeIterator).seek(captor.capture());
        assertEquals(captor.getValue(), userKey);
    }

    @Test
    public void seekForPrev() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVIterator(rangeIterator);
        itr.seekForPrev(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(rangeIterator).seekForPrev(captor.capture());
        assertEquals(captor.getValue(), userKey);
    }
}
