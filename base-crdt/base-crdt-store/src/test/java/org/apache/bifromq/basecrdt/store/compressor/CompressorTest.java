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

package org.apache.bifromq.basecrdt.store.compressor;

import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompressorTest {

    @Test
    public void testGzip() {
        Compressor gzip = new GzipCompressor();
        ByteString bytes = ByteString.copyFromUtf8("gzip test");
        ByteString compressed = gzip.compress(bytes);
        Assert.assertEquals(bytes, gzip.decompress(compressed));
    }

    @Test
    public void testNoop() {
        Compressor noop = new NoopCompressor();
        ByteString bytes = ByteString.copyFromUtf8("noop test");
        ByteString compressed = noop.compress(bytes);
        Assert.assertEquals(bytes, noop.decompress(compressed));
    }

}
