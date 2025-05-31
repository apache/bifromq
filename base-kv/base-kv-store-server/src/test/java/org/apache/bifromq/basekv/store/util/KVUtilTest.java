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

package org.apache.bifromq.basekv.store.util;

import static org.apache.bifromq.basekv.store.util.KVUtil.cap;
import static org.apache.bifromq.basekv.store.util.KVUtil.toByteString;
import static org.apache.bifromq.basekv.store.util.KVUtil.toInt;
import static org.apache.bifromq.basekv.store.util.KVUtil.toKVRangeId;
import static org.apache.bifromq.basekv.store.util.KVUtil.toLong;
import static org.apache.bifromq.basekv.store.util.KVUtil.toLongNativeOrder;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVUtilTest extends MockableTest {
    @Test
    public void testToBytes() {
        assertEquals(toLong(KVUtil.toByteString(100L)), 100L);
        assertEquals(toInt(KVUtil.toByteString(100)), 100);
        assertEquals(toLongNativeOrder(KVUtil.toByteStringNativeOrder(100L)), 100L);
    }

    @Test
    public void testConcat() {
        assertEquals(KVUtil.concat(ByteString.copyFrom(new byte[] {1, 2}),
                ByteString.copyFrom(new byte[] {3, 4, 5}),
                ByteString.copyFrom(new byte[0]),
                ByteString.copyFrom(new byte[] {6, 7})),
            ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5, 6, 7}));
        assertEquals(KVUtil.concat(ByteString.EMPTY, ByteString.EMPTY), ByteString.EMPTY);
        assertEquals(KVUtil.concat(ByteString.copyFrom(new byte[] {1, 2, 3})),
            ByteString.copyFrom(new byte[] {1, 2, 3}));
    }

    @Test
    public void testUpperBound() {
        KVRangeId bucketId = cap(KVRangeId.newBuilder().build());
        assertEquals(0L, bucketId.getEpoch());
        assertEquals(1L, bucketId.getId());
    }

    @Test
    public void testKVRangeIdCodec() {
        KVRangeId id = KVRangeIdUtil.generate();
        assertEquals(toKVRangeId(toByteString(id)), id);
    }
}
