/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.util;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;

public class BSUtil {
    public static long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    public static byte[] toBytes(long l) {
        return ByteBuffer.allocate(Long.BYTES).putLong(l).array();
    }

    public static ByteString toByteString(long l) {
        return UnsafeByteOperations.unsafeWrap(toBytes(l));
    }

    public static int toInt(ByteString b) {
        assert b.size() == Integer.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getInt();
    }

    public static ByteString toByteString(int i) {
        return unsafeWrap(toBytes(i));
    }

    public static byte[] toBytes(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    public static short toShort(ByteString s) {
        assert s.size() == Short.BYTES;
        ByteBuffer buffer = s.asReadOnlyByteBuffer();
        return buffer.getShort();
    }

    public static byte[] toBytes(short s) {
        return ByteBuffer.allocate(Short.BYTES).putShort(s).array();
    }

    public static ByteString toByteString(short s) {
        return unsafeWrap(toBytes(s));
    }
}
