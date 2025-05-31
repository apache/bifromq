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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressor implements Compressor {
    @Override
    public ByteString compress(ByteString src) {
        ByteString.Output out = ByteString.newOutput();
        try (GZIPOutputStream defl = new GZIPOutputStream(out); InputStream is = src.newInput()) {
            is.transferTo(defl);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteString();
    }

    @Override
    public ByteString decompress(ByteString src) {
        ByteString.Output out = ByteString.newOutput();
        try (GZIPInputStream infl = new GZIPInputStream(src.newInput())) {
            infl.transferTo(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteString();
    }
}
