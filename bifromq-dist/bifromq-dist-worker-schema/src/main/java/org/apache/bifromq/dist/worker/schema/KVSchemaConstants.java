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

package org.apache.bifromq.dist.worker.schema;

import com.google.protobuf.ByteString;

public class KVSchemaConstants {
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});
    public static final int MAX_RECEIVER_BUCKETS = 0xFF; // one byte
    public static final byte FLAG_NORMAL = 0x01;
    public static final byte FLAG_UNORDERED = 0x02;
    public static final byte FLAG_ORDERED = 0x03;
    public static final ByteString SEPARATOR_BYTE = ByteString.copyFrom(new byte[] {0x00});
    public static final ByteString FLAG_NORMAL_VAL = ByteString.copyFrom(new byte[] {FLAG_NORMAL});
    public static final ByteString FLAG_UNORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_UNORDERED});
    public static final ByteString FLAG_ORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_ORDERED});
}
