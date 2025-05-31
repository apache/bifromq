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

package org.apache.bifromq.retain.store.schema;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.util.List;

/**
 * Hashing the levels of a topic to a byte array using FNV-1a hash.
 */
public class LevelHash {
    private static final int FNV_32_INIT = 0x811c9dc5;
    private static final int FNV_32_PRIME = 0x01000193;

    public static ByteString hash(List<String> topicLevels) {
        byte[] hash = new byte[topicLevels.size()];
        for (int i = 0; i < topicLevels.size(); i++) {
            hash[i] = hashToByte(topicLevels.get(i));
        }
        return unsafeWrap(hash);
    }

    private static byte hashToByte(String data) {
        int hash = FNV_32_INIT;
        for (int i = 0; i < data.length(); i++) {
            hash ^= data.charAt(i);
            hash *= FNV_32_PRIME;
        }
        return (byte) (hash & 0xff);
    }
}
