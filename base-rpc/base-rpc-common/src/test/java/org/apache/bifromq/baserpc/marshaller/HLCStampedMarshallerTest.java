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

import static io.grpc.protobuf.lite.ProtoLiteUtils.marshaller;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Value;
import io.grpc.MethodDescriptor;
import java.io.InputStream;
import lombok.SneakyThrows;
import org.apache.bifromq.basehlc.HLC;
import org.testng.annotations.Test;

public class HLCStampedMarshallerTest {
    @Test
    public void testHLCStamping() {
        Struct orig = Struct.newBuilder().putFields("key", Value.newBuilder().setNumberValue(123).build()).build();

        MethodDescriptor.Marshaller<Struct> baseMarshaller = marshaller(Struct.getDefaultInstance());
        HLCStampedMarshaller<Struct> stampedMarshaller = new HLCStampedMarshaller<>(baseMarshaller);

        long before = HLC.INST.get();
        InputStream stream = stampedMarshaller.stream(orig);
        Struct decoded = stampedMarshaller.parse(stream);
        long after = HLC.INST.get();

        assertEquals(orig, decoded);

        assertTrue(after >= before);
    }

    @SneakyThrows
    @Test
    public void testForwardCompatibility() {
        Struct orig = Struct.newBuilder()
            .putFields("key", Value.newBuilder().setNumberValue(123).build())
            .build();
        MethodDescriptor.Marshaller<Struct> baseMarshaller = marshaller(Struct.getDefaultInstance());
        HLCStampedMarshaller<Struct> stampedMarshaller = new HLCStampedMarshaller<>(baseMarshaller);
        InputStream stream = stampedMarshaller.stream(orig);
        Struct decoded = Struct.parseFrom(stream);
        UnknownFieldSet.Field piggybackField = decoded.getUnknownFields().getField(Short.MAX_VALUE);
        assertTrue(piggybackField != null && piggybackField.getFixed64List().size() == 1);
        assertEquals(orig, decoded.toBuilder().setUnknownFields(UnknownFieldSet.getDefaultInstance()).build());
    }

    @Test
    public void testBackwardCompatibility() {
        long before = HLC.INST.get();
        Struct orig = Struct.newBuilder()
            .putFields("key", Value.newBuilder().setNumberValue(123).build())
            .setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(Short.MAX_VALUE, UnknownFieldSet.Field.newBuilder()
                    .addFixed64(before).build())
                .build())
            .build();
        MethodDescriptor.Marshaller<Struct> baseMarshaller = marshaller(Struct.getDefaultInstance());
        HLCStampedMarshaller<Struct> stampedMarshaller = new HLCStampedMarshaller<>(baseMarshaller);
        stampedMarshaller.parse(orig.toByteString().newInput());
        long after = HLC.INST.get();
        assertTrue(after >= before);
    }
}
