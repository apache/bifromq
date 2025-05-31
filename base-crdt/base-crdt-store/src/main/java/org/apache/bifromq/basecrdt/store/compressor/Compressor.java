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

package org.apache.bifromq.basecrdt.store.compressor;

import org.apache.bifromq.basecrdt.store.CompressAlgorithm;
import com.google.protobuf.ByteString;

public interface Compressor {

    ByteString compress(ByteString src);

    ByteString decompress(ByteString src);

    static Compressor newInstance(CompressAlgorithm algorithm) {
        switch (algorithm) {
            case GZIP: {
                return new GzipCompressor();
            }
            case NONE:
            default: {
                return new NoopCompressor();
            }
        }
    }

}
