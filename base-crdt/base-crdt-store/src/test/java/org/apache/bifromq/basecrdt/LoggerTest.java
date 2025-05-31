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

package org.apache.bifromq.basecrdt;

import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.logger.FormatableLogger;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.testng.annotations.Test;

public class LoggerTest {
    @Test
    public void log() {
        Logger logger = FormatableLogger.getLogger(LoggerTest.class);
        logger.info("abc");
    }

    @Test
    public void log1() {
        Replica replica = Replica.newBuilder()
            .setUri(CRDTURI.toURI(CausalCRDTType.ormap, "test"))
            .setId(ByteString.copyFromUtf8("123"))
            .build();
        ReplicaLogger logger = new ReplicaLogger(replica, LoggerTest.class);
        logger.info("abc");
    }
}
