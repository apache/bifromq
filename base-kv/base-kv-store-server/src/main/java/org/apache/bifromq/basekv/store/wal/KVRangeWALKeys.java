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

package org.apache.bifromq.basekv.store.wal;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basekv.store.util.KVUtil;
import com.google.protobuf.ByteString;

class KVRangeWALKeys {
    public static final ByteString KEY_CURRENT_VOTING_BYTES = unsafeWrap(new byte[] {(byte) 0});
    public static final ByteString KEY_LATEST_SNAPSHOT_BYTES = unsafeWrap(new byte[] {(byte) 1});
    public static final ByteString KEY_CONFIG_ENTRY_INDEXES_BYTES = unsafeWrap(new byte[] {(byte) 2});
    public static final ByteString KEY_CURRENT_TERM_BYTES = unsafeWrap(new byte[] {(byte) 3});
    public static final ByteString KEY_LOG_ENTRIES_INCAR = unsafeWrap(new byte[] {(byte) 4});
    public static final ByteString KEY_PREFIX_LOG_ENTRIES_BYTES = unsafeWrap(new byte[] {(byte) 5});

    public static ByteString configEntriesKeyPrefixInfix(int logEntriesKeyInfix) {
        return KEY_CONFIG_ENTRY_INDEXES_BYTES.concat(KVUtil.toByteString(logEntriesKeyInfix));
    }

    public static ByteString configEntriesKey(int logEntriesKeyInfix, long index) {
        return configEntriesKeyPrefixInfix(logEntriesKeyInfix).concat(KVUtil.toByteString(index));
    }

    public static ByteString logEntriesKeyPrefixInfix(int logEntriesKeyInfix) {
        return KEY_PREFIX_LOG_ENTRIES_BYTES.concat(KVUtil.toByteString(logEntriesKeyInfix));
    }

    public static ByteString logEntryKey(int logEntriesKeyInfix, long logIndex) {
        return logEntriesKeyPrefixInfix(logEntriesKeyInfix).concat(KVUtil.toByteString(logIndex));
    }

    public static long parseLogIndex(ByteString logEntryKey) {
        return logEntryKey.asReadOnlyByteBuffer().getLong(2 + Integer.BYTES);
    }
}
