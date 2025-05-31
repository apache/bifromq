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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;

class Keys {
    public static final byte[] LATEST_CP_KEY = new byte[] {0x01};
    public static final byte[] META_SECTION_START = new byte[] {0x02};
    public static final byte[] META_SECTION_END = new byte[] {0x03};
    private static final ByteString METADATA_PREFIX = unsafeWrap(new byte[] {0x02});
    private static final ByteString DATA_PREFIX = unsafeWrap(new byte[] {(byte) 0xFE});
    public static final byte[] DATA_SECTION_START = new byte[] {(byte) 0xFE};
    public static final byte[] DATA_SECTION_END = new byte[] {(byte) 0xFF};

    public static byte[] toDataKey(ByteString key) {
        return DATA_PREFIX.concat(key).toByteArray();
    }

    public static ByteString fromDataKey(byte[] rawKey) {
        return unsafeWrap(rawKey).substring(DATA_PREFIX.size());
    }

    public static byte[] toDataKey(byte[] key) {
        return DATA_PREFIX.concat(unsafeWrap(key)).toByteArray();
    }

    public static byte[] toMetaKey(ByteString key) {
        return METADATA_PREFIX.concat(key).toByteArray();
    }

    public static ByteString fromMetaKey(byte[] rawKey) {
        return unsafeWrap(rawKey).substring(METADATA_PREFIX.size());
    }
}
