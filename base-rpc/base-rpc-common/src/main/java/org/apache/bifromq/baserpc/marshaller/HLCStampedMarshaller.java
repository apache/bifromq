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

import static org.apache.bifromq.baserpc.marshaller.HLCStampedInputStream.HLC_FIELD_ID;
import static org.apache.bifromq.baserpc.marshaller.HLCStampedInputStream.HLC_TAG;

import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;

/**
 * Marshaller that prepend HLC timestamp to encoded proto bytes.
 */
@Slf4j
public class HLCStampedMarshaller<T> implements MethodDescriptor.Marshaller<T> {
    private final MethodDescriptor.Marshaller<T> delegate;

    public HLCStampedMarshaller(MethodDescriptor.Marshaller<T> delegate) {
        this.delegate = delegate;
    }

    @SuppressWarnings("unchecked")
    public Class<T> getMessageClass() {
        return (Class<T>) delegate.getClass();
    }

    @Override
    public InputStream stream(T value) {
        return new HLCStampedInputStream(delegate.stream(value));
    }

    @SneakyThrows
    @Override
    public T parse(InputStream stream) {
        if (stream instanceof HLCStampedInputStream) {
            // optimized for in-proc
            return delegate.parse(((HLCStampedInputStream) stream).protoStream());
        }
        PushbackInputStream pis = new PushbackInputStream(stream, 3);
        byte b0 = (byte) pis.read();
        byte b1 = (byte) pis.read();
        byte b2 = (byte) pis.read();
        if (HLC_TAG[0] != b0 || HLC_TAG[1] != b1 || HLC_TAG[2] != b2) {
            // backward compatible with obsolete EnhancedMarshaller
            pis.unread(b2);
            pis.unread(b1);
            pis.unread(b0);
            T message = delegate.parse(pis);
            UnknownFieldSet.Field piggybackField = ((Message) message).getUnknownFields().getField(HLC_FIELD_ID);
            HLC.INST.update(piggybackField.getFixed64List().get(0));
            return message;
        }
        long hlc = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            int b = pis.read();
            if (b == -1) {
                throw new IOException("Unexpected end of stream while reading HLC");
            }
            hlc |= ((long) b & 0xFF) << (i * Long.BYTES);
        }
        HLC.INST.update(hlc);
        return delegate.parse(pis);
    }
}
