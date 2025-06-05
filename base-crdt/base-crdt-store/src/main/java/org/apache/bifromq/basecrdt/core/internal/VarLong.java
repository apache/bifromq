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

package org.apache.bifromq.basecrdt.core.internal;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;

/**
 * Variable-length Long.
 */
public class VarLong {
    /**
     * Decode bytes to long.
     *
     * @param bytes the var long bytes
     * @return long value
     */
    public static long decode(ByteString bytes) {
        return decode(bytes.asReadOnlyByteBuffer());
    }

    private static long decode(ByteBuffer buffer) {
        long temp = buffer.get();
        if (temp >= 0) {
            return temp;
        }
        long result = temp & 0x7F;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 7;
            return result;
        }
        result |= (temp & 0x7F) << 7;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 14;
            return result;
        }
        result |= (temp & 0x7F) << 14;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 21;
            return result;
        }
        result |= (temp & 0x7F) << 21;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 28;
            return result;
        }
        result |= (temp & 0x7F) << 28;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 35;
            return result;
        }
        result |= (temp & 0x7F) << 35;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 42;
            return result;
        }
        result |= (temp & 0x7F) << 42;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 49;
            return result;
        }
        result |= (temp & 0x7F) << 49;
        temp = buffer.get();
        if (temp >= 0) {
            result |= temp << 56;
            return result;
        }
        result |= (temp & 0x7F) << 56;
        result |= ((long) buffer.get()) << 63;
        return result;
    }

    /**
     * Encode long to var bytes.
     *
     * @param value the long value
     * @return encoded bytes
     */
    public static ByteString encode(long value) {
        int size = sizing(value);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        encode(value, buffer);
        return unsafeWrap(buffer.array());
    }

    private static int sizing(long value) {
        int size = 0;
        do {
            size++;
            value >>>= 7;
        } while (value != 0);
        return size;
    }

    private static void encode(long value, ByteBuffer buffer) {
        while (true) {
            int currentBits = (int) value & 0x7F;
            value >>>= 7;
            if (value == 0) {
                buffer.put((byte) currentBits);
                return;
            } else {
                buffer.put((byte) (currentBits | 0x80));
            }
        }
    }
}