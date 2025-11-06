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

package org.apache.bifromq.baseenv;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.google.protobuf.UnsafeByteOperations;

/**
 * A utility class for parsing protocol buffer messages from a ByteString using zero-copy parsing.
 * This allows for efficient parsing without the need to copy the underlying byte array.
 */
public class ZeroCopyParser {
    /**
     * Parses a protocol buffer message from a ByteString using zero-copy parsing.
     *
     * @param bytes The ByteString to parse
     * @param parser The parser for the protocol buffer message.
     * @return The parsed protocol buffer message.
     *
     * @throws InvalidProtocolBufferException If the parsing fails.
     */
    public static <T> T parse(ByteString bytes, Parser<T> parser) throws InvalidProtocolBufferException {
        CodedInputStream input = bytes.newCodedInput();
        input.enableAliasing(true);
        return parser.parseFrom(input);
    }

    /**
     * Parses a protocol buffer message from a byte array using zero-copy parsing.
     *
     * @param bytes The byte array to parse
     * @param parser The parser for the protocol buffer message.
     * @return The parsed protocol buffer message.
     *
     * @throws InvalidProtocolBufferException If the parsing fails.
     */
    public static <T> T parse(byte[] bytes, Parser<T> parser) throws InvalidProtocolBufferException {
        return parse(UnsafeByteOperations.unsafeWrap(bytes), parser);
    }
}
