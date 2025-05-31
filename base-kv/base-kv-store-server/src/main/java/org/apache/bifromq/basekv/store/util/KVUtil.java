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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;

import org.apache.bifromq.basekv.proto.KVRangeId;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class KVUtil {
    public static ByteString toByteString(int i) {
        return UnsafeByteOperations.unsafeWrap(toBytes(i));
    }

    public static ByteString toByteString(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytes(l));
    }

    public static ByteString toByteString(KVRangeId kvRangeId) {
        return UnsafeByteOperations.unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES)
            .putLong(kvRangeId.getEpoch())
            .putLong(kvRangeId.getId())
            .flip());
    }

    public static ByteString toByteStringNativeOrder(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytesNativeOrder(l));
    }

    private static byte[] toBytes(long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    private static byte[] toBytes(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    private static byte[] toBytesNativeOrder(long l) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder()).putLong(l).array();
    }

    public static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }

    public static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    public static long toLong(byte[] b) {
        return toLong(UnsafeByteOperations.unsafeWrap(b));
    }

    public static long toLongNativeOrder(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer().order(ByteOrder.nativeOrder());
        return buffer.getLong();
    }

    public static KVRangeId toKVRangeId(ByteString b) {
        assert b.size() == 2 * Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return KVRangeId.newBuilder()
            .setEpoch(buffer.getLong())
            .setId(buffer.getLong())
            .build();
    }

    public static KVRangeId cap(KVRangeId kvRangeId) {
        ByteBuffer upper = ByteBuffer.wrap(upperBound(
            ByteBuffer.allocate(2 * Long.BYTES)
                .putLong(kvRangeId.getEpoch())
                .putLong(kvRangeId.getId())
                .array()
        ));
        return KVRangeId.newBuilder().setEpoch(upper.getLong()).setId(upper.getLong()).build();
    }

    public static ByteString concat(ByteString... keys) {
        ByteString finalBS = ByteString.EMPTY;
        for (ByteString key : keys) {
            finalBS = finalBS.concat(key);
        }
        return finalBS;
    }
}
