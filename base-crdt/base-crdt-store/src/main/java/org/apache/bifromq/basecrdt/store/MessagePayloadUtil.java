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

package org.apache.bifromq.basecrdt.store;

import org.apache.bifromq.basecrdt.store.compressor.Compressor;
import org.apache.bifromq.basecrdt.store.proto.AckMessage;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.basecrdt.store.proto.DeltaMessage;
import org.apache.bifromq.basecrdt.store.proto.MessagePayload;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MessagePayloadUtil {

    public static ByteString compressToPayload(Compressor compressor, AckMessage ackMessage) {
        return compressor.compress(MessagePayload.newBuilder().setAck(ackMessage).build().toByteString());
    }

    public static ByteString compressToPayload(Compressor compressor, DeltaMessage deltaMessage) {
        return compressor.compress(MessagePayload.newBuilder().setDelta(deltaMessage).build().toByteString());
    }

    public static MessagePayload decompress(Compressor compressor, CRDTStoreMessage crdtStoreMessage) {
        try {
            return MessagePayload.parseFrom(compressor.decompress(crdtStoreMessage.getPayload()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Can not decompress message payload from message {}", e);
        }
    }

}
