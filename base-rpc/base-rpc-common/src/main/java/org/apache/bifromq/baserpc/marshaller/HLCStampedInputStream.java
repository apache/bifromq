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

package org.apache.bifromq.baserpc.marshaller;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.Drainable;
import io.grpc.KnownLength;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.bifromq.basehlc.HLC;

class HLCStampedInputStream extends InputStream implements Drainable, KnownLength {
    public static final int HLC_FIELD_ID = Short.MAX_VALUE;
    // varint encoded key: unknown fieldset_number=32767, wire_type=1 (fixed64)
    public static final byte[] HLC_TAG = new byte[3];
    public static final int HLC_FIELD_LENGTH = HLC_TAG.length + Long.BYTES; // 3 bytes tag + 8 bytes fixed64

    static {
        CodedOutputStream cos = CodedOutputStream.newInstance(HLC_TAG);
        try {
            cos.writeTag(HLC_FIELD_ID, WireFormat.WIRETYPE_FIXED64);
        } catch (IOException e) {
            // never happens
        }
    }

    private final long hlc;
    private final InputStream protoStream;
    private int cursor = 0;

    HLCStampedInputStream(InputStream protoStream) {
        assert protoStream instanceof Drainable;
        assert protoStream instanceof KnownLength;
        this.hlc = HLC.INST.get();
        this.protoStream = protoStream;
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
        while (cursor < HLC_FIELD_LENGTH) {
            target.write(read());
        }
        return HLC_FIELD_LENGTH + ((Drainable) protoStream).drainTo(target);
    }

    @Override
    public int available() throws IOException {
        return (HLC_FIELD_LENGTH - cursor) + protoStream.available();
    }

    @Override
    public int read() throws IOException {
        if (cursor < HLC_TAG.length) {
            return HLC_TAG[cursor++] & 0xFF;
        }
        if (cursor < HLC_FIELD_LENGTH) {
            int shift = (cursor - HLC_TAG.length) * Long.BYTES;
            cursor++;
            return (int) ((hlc >>> shift) & 0xFF);
        }
        int read = protoStream.read();
        if (read == -1) {
            return -1; // End of stream
        }
        cursor++;
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int total = 0;
        while (len > 0) {
            int r = read();
            if (r == -1) {
                return total == 0 ? -1 : total;
            }
            b[off++] = (byte) r;
            total++;
            len--;
        }
        return total;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = 0;
        while (n > 0 && cursor < HLC_FIELD_LENGTH) {
            cursor++;
            n--;
            skipped++;
        }
        if (n > 0) {
            skipped += protoStream.skip(n);
        }
        return skipped;
    }

    @Override
    public void close() throws IOException {
        protoStream.close();
    }

    public InputStream protoStream() {
        return protoStream;
    }
}
