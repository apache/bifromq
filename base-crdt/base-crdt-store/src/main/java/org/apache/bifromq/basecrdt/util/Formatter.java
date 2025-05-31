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

package org.apache.bifromq.basecrdt.util;

import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.proto.AckMessage;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.basecrdt.store.proto.DeltaMessage;
import org.apache.bifromq.logger.FormatableLogger;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

public class Formatter {
    public static FormatableLogger.Stringifiable toStringifiable(ByteString address) {
        return () -> BaseEncoding.base64().encode(address.toByteArray());
    }

    public static String toString(Replica replica) {
        return replica.getUri() + "-" + BaseEncoding.base32().encode(replica.getId().toByteArray());
    }

    public static String toString(DeltaMessage delta) {
        try {
            return JsonFormat.printer().print(delta);
        } catch (Exception e) {
            // ignore
            return delta.toString();
        }
    }

    public static String toString(AckMessage ack) {
        try {
            return JsonFormat.printer().print(ack);
        } catch (Exception e) {
            // ignore
            return ack.toString();
        }
    }

    public static String toString(CRDTStoreMessage ack) {
        try {
            return JsonFormat.printer().print(ack);
        } catch (Exception e) {
            // ignore
            return ack.toString();
        }
    }
}
