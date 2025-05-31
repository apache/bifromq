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

package io.grpc.protobuf.lite;

import org.apache.bifromq.basehlc.HLC;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.KnownLength;
import io.grpc.MethodDescriptor;
import jakarta.annotation.Nullable;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import lombok.SneakyThrows;

/**
 * Enhance marshaller with HLC piggybacking & aliasing enabled.
 * The reason why it's put in io.grpc.protobuf.lite package is because ProtoInputStream is package-private.
 *
 * @param <T> the type of the message
 */
public class EnhancedMarshaller<T> implements MethodDescriptor.PrototypeMarshaller<T> {
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024;
    private static final int PIGGYBACK_FIELD_ID = Short.MAX_VALUE;
    private static final ThreadLocal<Reference<byte[]>> bufs = new ThreadLocal<>();
    private final T defaultInstance;
    private final Parser<T> parser;
    private final ThreadLocal<UnknownFieldSet.Builder> localFieldSetBuilder =
        ThreadLocal.withInitial(UnknownFieldSet::newBuilder);

    private final ThreadLocal<UnknownFieldSet.Field.Builder> localFieldBuilder =
        ThreadLocal.withInitial(UnknownFieldSet.Field::newBuilder);

    @SuppressWarnings("unchecked")
    public EnhancedMarshaller(T defaultInstance) {
        this.defaultInstance = defaultInstance;
        parser = (Parser<T>) ((MessageLite) defaultInstance).getParserForType();
    }

    @Nullable
    @Override
    public T getMessagePrototype() {
        return defaultInstance;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getMessageClass() {
        return (Class<T>) defaultInstance.getClass();
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputStream stream(T value) {
        UnknownFieldSet.Field hlcField = localFieldBuilder.get().clear().addFixed64(HLC.INST.get()).build();
        UnknownFieldSet fieldSet = localFieldSetBuilder.get().addField(PIGGYBACK_FIELD_ID, hlcField).build();
        return new ProtoInputStream(((Message) value).toBuilder().setUnknownFields(fieldSet).build(), parser);
    }

    @SneakyThrows
    @Override
    public T parse(InputStream stream) {
        if (stream instanceof ProtoInputStream protoStream) {
            if (protoStream.parser() == parser) {
                @SuppressWarnings("unchecked")
                T message = (T) protoStream.message();
                return message;
            }
        }
        CodedInputStream cis = null;
        if (stream instanceof KnownLength) {
            int size = stream.available();
            if (size > 0 && size <= DEFAULT_MAX_MESSAGE_SIZE) {
                Reference<byte[]> ref;
                byte[] buf;
                if ((ref = bufs.get()) == null || (buf = ref.get()) == null || buf.length < size) {
                    buf = new byte[size];
                    bufs.set(new WeakReference<>(buf));
                }
                int remaining = size;
                while (remaining > 0) {
                    int position = size - remaining;
                    int count = stream.read(buf, position, remaining);
                    if (count == -1) {
                        break;
                    }
                    remaining -= count;
                }
                if (remaining != 0) {
                    int position = size - remaining;
                    throw new IllegalStateException("Wrong size: " + size + " != " + position);
                }
                cis = CodedInputStream.newInstance(buf, 0, size);
            } else if (size == 0) {
                return getMessagePrototype();
            }
        }
        if (cis == null) {
            cis = CodedInputStream.newInstance(stream);
        }
        cis.setSizeLimit(Integer.MAX_VALUE);

        // we need aliasing to be enabled
        cis.enableAliasing(true);

        T message = parser.parseFrom(cis, ExtensionRegistryLite.getEmptyRegistry());
        cis.checkLastTagWas(0);
        UnknownFieldSet.Field piggybackField = ((Message) message).getUnknownFields().getField(PIGGYBACK_FIELD_ID);
        HLC.INST.update(piggybackField.getFixed64List().get(0));
        return message;
    }
}
